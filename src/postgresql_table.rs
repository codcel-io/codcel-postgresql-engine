// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0 OR Codcel-Commercial
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT, LICENSE-APACHE, and LICENSE-CODCEL-COMMERCIAL in the project root.

use crate::postgresql_table_loader::{
    ensure_t_table_from_parquet, ensure_table_from_parquet, PostgreSqlColumnType, PostgresSqlColumn,
};
use async_trait::async_trait;
use codcel_table_engine::codcel_table::CodcelTable;
use codcel_table_engine::column_type::ColumnType;
use codcel_table_engine::condition::Condition;
use codcel_table_engine::sql_modifiers::{SqlAggregate, SqlModifiers};
use codcel_table_engine::searchable::{
    find_exact_position, find_largest_position, find_smallest_position, Searchable,
};
use codcel_table_engine::table_constants::{
    X_MATCH_MODE_EXACT, X_MATCH_MODE_EXACT_NEXT_LARGEST, X_MATCH_MODE_EXACT_NEXT_SMALLEST,
    X_MATCH_MODE_WILDCARD, X_SEARCH_MODE_BINARY_FIRST, X_SEARCH_MODE_BINARY_LAST,
    X_SEARCH_MODE_FIRST, X_SEARCH_MODE_REVERSE,
};
use codcel_table_engine::table_functions::TableFunctions;
use codcel_calculation_engine::input::Input;
use codcel_calculation_engine::value::Value;
use codcel_calculation_engine::value_format::ValueFormat;
use sqlx::postgres::{PgArguments, PgPoolOptions, PgRow};
use sqlx::{Arguments, Executor, Row};
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use std::sync::Arc;
use log::debug;
use uuid::Uuid;
use futures::stream::{self, StreamExt};

/// Quote a PostgreSQL identifier (column/table name) to prevent SQL injection.
fn qident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Escapes a string value for use in SQL queries (prevents SQL injection in string literals)
fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// Quote multiple column identifiers, splitting by comma
fn quote_columns(columns: &str) -> String {
    columns
        .split(',')
        .map(|s| qident(s.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build SQL clause fragments from SqlModifiers.
/// Returns (select_prefix, order_clause, limit_clause).
/// - `select_prefix`: "DISTINCT " or "" (aggregates handled separately)
/// - `order_clause`: " ORDER BY ..." or ""
/// - `limit_clause`: " LIMIT ... OFFSET ..." or ""
fn build_modifier_clauses(modifiers: &SqlModifiers, col_list: &[String]) -> (String, String, String) {
    let select_prefix = if modifiers.distinct { "DISTINCT ".to_string() } else { String::new() };

    let order_clause = if let Some(ref order) = modifiers.order_by {
        let parts: Vec<String> = order.iter().map(|(idx, desc)| {
            let col = if *idx > 0 && *idx <= col_list.len() {
                qident(&col_list[*idx - 1])
            } else {
                qident(&col_list[0])
            };
            format!("{} {}", col, if *desc { "DESC" } else { "ASC" })
        }).collect();
        format!(" ORDER BY {}", parts.join(", "))
    } else {
        String::new()
    };

    let limit_clause = if let Some((limit, offset)) = modifiers.limit_offset {
        format!(" LIMIT {} OFFSET {}", limit, offset)
    } else {
        String::new()
    };

    (select_prefix, order_clause, limit_clause)
}

/// Convert PostgreSqlColumnType to the abstract ColumnType used by condition.rs
fn pg_type_to_column_type(pg_type: &PostgreSqlColumnType) -> ColumnType {
    match pg_type {
        PostgreSqlColumnType::Text => ColumnType::Text,
        PostgreSqlColumnType::Integer => ColumnType::Integer,
        PostgreSqlColumnType::BigInt => ColumnType::BigInt,
        PostgreSqlColumnType::Real => ColumnType::Float,
        PostgreSqlColumnType::DoublePrecision => ColumnType::Double,
        PostgreSqlColumnType::Boolean => ColumnType::Boolean,
        PostgreSqlColumnType::Date => ColumnType::Date,
        PostgreSqlColumnType::Timestamp => ColumnType::Timestamp,
        PostgreSqlColumnType::Bytea => ColumnType::Binary,
    }
}

/// Pre-built SQL query templates for common table operations.
///
/// These templates are constructed once during table initialization and reused
/// throughout the table's lifetime. This optimization avoids repeated string
/// allocations and maximizes PostgreSQL's prepared statement cache hits.
///
/// All fields are private as this is an internal optimization detail.
/// Users interact with these templates indirectly through [`PostgreSQLTable`] methods.
#[derive(Debug, Clone)]
pub struct PreparedQueryTemplates {
    /// SELECT {all_cols} FROM {table} - for fetching all columns.
    #[allow(dead_code)]
    select_all: String,
    /// SELECT {all_cols} FROM {table} LIMIT 1 OFFSET $1 - for row-by-index access.
    #[allow(dead_code)]
    select_row_by_offset: String,
    /// SELECT COUNT(*) FROM {table} - for row counting.
    count_rows: String,
    /// INSERT INTO {table} ({cols}) VALUES ({placeholders}) - for adding rows.
    insert_row: String,
    /// UPDATE {table} SET {col=$n, ...} WHERE {id_col} = ${n+1} - for updating rows.
    update_row: String,
    /// DELETE FROM {table} WHERE {id_col} = $1 - for deleting rows.
    delete_row: String,
    /// SELECT {all_cols} FROM {table} WHERE {id_col} = $1 - for reading a single row by ID.
    read_row: String,
}

/// A PostgreSQL-backed table that implements Excel-like lookup and filter operations.
///
/// `PostgreSQLTable` provides a high-level interface for working with PostgreSQL tables
/// that support Excel-compatible functions like VLOOKUP, HLOOKUP, XLOOKUP, INDEX, MATCH,
/// and FILTER. It also supports CRUD operations (add, read, update, delete rows).
///
/// # Creating a Table
///
/// Use the [`PostgreSQLTable::init`] method to create a new instance. This will:
/// - Create the database if it doesn't exist
/// - Create the table from a Parquet file schema if it doesn't exist
/// - Optionally insert data from the Parquet file
///
/// # Trait Implementation
///
/// This struct implements the `CodcelTable` trait from `codcel_table_engine`, providing
/// all the standard table operations used by the Codcel calculation engine.
///
/// # Thread Safety
///
/// The underlying `sqlx::PgPool` is thread-safe and can be shared across async tasks.
/// However, `PostgreSQLTable` itself does not implement `Clone` or `Sync`.
#[derive(Debug)]
pub struct PostgreSQLTable {
    table_name: String,
    //postgresql_url: String,
    postgresql_columns: Vec<PostgresSqlColumn>,
    db: sqlx::PgPool,
    /// Cached column name -> index mapping for O(1) lookups.
    column_cache: HashMap<String, usize>,
    /// Pre-computed quoted table name for SQL queries (avoids repeated allocations).
    quoted_table_name: String,
    /// Pre-computed quoted column names for SQL queries (avoids repeated allocations).
    quoted_column_names: Vec<String>,
    /// Pre-built SQL query templates for frequently-used operations.
    query_templates: PreparedQueryTemplates,
}

impl PostgreSQLTable {
    // Helper to apply dynamic table functions for values marked with the "*F*<fn_name>" pattern.
    // If the marker is present and the function exists and succeeds, returns the function result; otherwise returns the original value.
    async fn apply_table_functions_to_value(
        value: Value,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Value {
        if let Ok(text) = value.string(value_format) {
            if let Some(func_name) = text.strip_prefix("*P*") {
                // Parameterized table function: *P*template_name:const1:const2:...
                if let Some(ref param_map) = table_functions.param_functions {
                    let mut parts = func_name.splitn(2, ':');
                    let template_name = parts.next().unwrap();
                    let constants_str = parts.next().unwrap_or("");
                    let params: Vec<Value> = constants_str.split(':')
                        .filter(|s| !s.is_empty())
                        .map(|s| {
                            // Try i32 first to preserve integer types (e.g., "1" → I32(1))
                            if let Ok(i) = s.parse::<i32>() {
                                Value::I32(i)
                            } else if let Ok(f) = s.parse::<f64>() {
                                Value::F64(f)
                            } else {
                                Value::String(s.to_string())
                            }
                        })
                        .collect();
                    if let Some(fun) = param_map.get(template_name) {
                        if let Ok(res) = fun(Arc::new(input.clone()), params).await {
                            return res;
                        }
                    }
                }
            } else if let Some(func_name) = text.strip_prefix("*F*") {
                if let Some(ref map) = table_functions.functions {
                    if let Some(fun) = map.get(func_name) {
                        if let Ok(res) = fun(Arc::new(input.clone())).await {
                            return res;
                        }
                    }
                }
            }
        }
        value
    }

    /// Initializes a new PostgreSQL table from Parquet file(s).
    ///
    /// This async constructor creates the database and table if they don't exist,
    /// and optionally loads data from the Parquet file. It handles two modes:
    ///
    /// - **Standard mode**: When `filename == crud_filename` or `column_names` is empty,
    ///   creates a table with columns matching the Parquet schema directly.
    /// - **T-table mode**: When `filename != crud_filename` and `column_names` is provided,
    ///   creates a table with a synthetic `c0` UUID column and custom column names.
    ///
    /// # Arguments
    ///
    /// * `filename` - Path to the main Parquet data file
    /// * `file_shortname` - Short name derived from the filename (used for table naming)
    /// * `db_name` - Name of the PostgreSQL database to use (created if it doesn't exist)
    /// * `column_names` - Custom column names for t-table mode (empty for standard mode)
    /// * `crud_filename` - Path to the CRUD operations Parquet file
    /// * `crud_file_shortname` - Short name for CRUD file (used for table naming in t-table mode)
    /// * `insert_rows` - If true, insert data from Parquet into the table
    /// * `unique_columns` - Column names that should have UNIQUE constraints
    /// * `optional_columns` - Column names that should allow NULL values
    ///
    /// # Returns
    ///
    /// A fully initialized [`PostgreSQLTable`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * Database creation or connection fails
    /// * Parquet file cannot be read
    /// * Table creation fails
    /// * Data insertion fails
    ///
    /// # Environment Variables
    ///
    /// * `CODCEL_POSTGRESQL_URL` - PostgreSQL connection URL (defaults to `postgresql://{username}@localhost:5432/`)
    #[allow(clippy::too_many_arguments)]
    pub async fn init(
        filename: String,
        file_shortname: &str,
        db_name: &str,
        column_names: Vec<String>,
        crud_filename: String,
        crud_file_shortname: &str,
        insert_rows: bool,
        unique_columns: Vec<String>,
        optional_columns: Vec<String>,
    ) -> Result<PostgreSQLTable, Box<dyn Error + Send + Sync>> {
        let table_name = if filename != crud_filename && !column_names.is_empty() {
            crud_file_shortname
                .replace(".parquet", "")
                .replace("_xyz0", "")
        } else {
            file_shortname.replace(".parquet", "").replace("_xyz0", "")
        };

        let postgresql_url = std::env::var("CODCEL_POSTGRESQL_URL").unwrap_or_else(|_| {
            let user = whoami::username().unwrap_or("codcel".to_string());
            format!("postgresql://{user}@localhost:5432/")
        });

        // This will create the database if it does not exist
        create_database(&postgresql_url, db_name).await?;

        // Now connect to your actual database
        let db_url = format!("{postgresql_url}{db_name}");
        let db = PgPoolOptions::new()
            .max_connections(20)
            .min_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(1800))
            .connect(&db_url)
            .await?;

        let postgresql_columns = if filename != crud_filename && !column_names.is_empty() {
            ensure_t_table_from_parquet(
                &filename,
                &crud_filename,
                &table_name,
                column_names,
                &db,
                insert_rows,
                &unique_columns,
                &optional_columns,
            )
            .await?
        } else {
            ensure_table_from_parquet(
                &filename,
                &table_name,
                &db,
                insert_rows,
                &unique_columns,
                &optional_columns,
            )
            .await?
        };

        let column_cache = Self::build_column_cache(&postgresql_columns);
        let quoted_table_name = qident(&table_name);
        let quoted_column_names = Self::build_quoted_column_names(&postgresql_columns);
        let query_templates = Self::build_query_templates(
            &quoted_table_name,
            &quoted_column_names,
            &postgresql_columns,
        );

        let postgresql_table = PostgreSQLTable {
            table_name,
            //postgresql_url,
            postgresql_columns,
            db,
            column_cache,
            quoted_table_name,
            quoted_column_names,
            query_templates,
        };

        Ok(postgresql_table)
    }

    /// Build pre-computed SQL query templates for frequently-used operations.
    /// These are constructed once at table initialization and reused to avoid
    /// repeated string allocations and to benefit from sqlx's statement caching.
    fn build_query_templates(
        quoted_table_name: &str,
        quoted_column_names: &[String],
        columns: &[PostgresSqlColumn],
    ) -> PreparedQueryTemplates {
        let all_cols = quoted_column_names.join(", ");
        let num_cols = columns.len();

        // SELECT {all_cols} FROM {table}
        let select_all = format!("SELECT {} FROM {}", all_cols, quoted_table_name);

        // SELECT {all_cols} FROM {table} LIMIT 1 OFFSET $1
        let select_row_by_offset = format!(
            "SELECT {} FROM {} LIMIT 1 OFFSET $1",
            all_cols, quoted_table_name
        );

        // SELECT COUNT(*) FROM {table}
        let count_rows = format!("SELECT COUNT(*) FROM {}", quoted_table_name);

        // INSERT INTO {table} ({cols}) VALUES ({placeholders})
        let placeholders = generate_placeholders(num_cols);
        let insert_row = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quoted_table_name, all_cols, placeholders
        );

        // UPDATE {table} SET {col=$n, ...} WHERE {id_col} = ${n+1}
        let set_clause: String = quoted_column_names
            .iter()
            .enumerate()
            .skip(1) // skip ID column
            .map(|(idx, quoted_name)| format!("{} = ${}", quoted_name, idx))
            .collect::<Vec<_>>()
            .join(", ");
        let id_placeholder = num_cols;
        let update_row = format!(
            "UPDATE {} SET {} WHERE {} = ${}",
            quoted_table_name, set_clause, quoted_column_names[0], id_placeholder
        );

        // DELETE FROM {table} WHERE {id_col} = $1
        let delete_row = format!(
            "DELETE FROM {} WHERE {} = $1",
            quoted_table_name, quoted_column_names[0]
        );

        // SELECT {all_cols} FROM {table} WHERE {id_col} = $1
        let read_row = format!(
            "SELECT {} FROM {} WHERE {} = $1",
            all_cols, quoted_table_name, quoted_column_names[0]
        );

        PreparedQueryTemplates {
            select_all,
            select_row_by_offset,
            count_rows,
            insert_row,
            update_row,
            delete_row,
            read_row,
        }
    }

    /// Build a HashMap from column names to their indices for O(1) lookups
    fn build_column_cache(columns: &[PostgresSqlColumn]) -> HashMap<String, usize> {
        columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.column_name.clone(), i))
            .collect()
    }

    /// Build pre-computed quoted column names to avoid repeated allocations
    fn build_quoted_column_names(columns: &[PostgresSqlColumn]) -> Vec<String> {
        columns.iter().map(|c| qident(&c.column_name)).collect()
    }

    /// Get the cached quoted column name by column name, or compute it if not cached
    /// (for columns that may not be in our schema, like user-provided column names)
    fn get_quoted_column_name(&self, column_name: &str) -> String {
        if let Some(&idx) = self.column_cache.get(column_name) {
            self.quoted_column_names[idx].clone()
        } else {
            // Fallback for columns not in our cache (e.g., user-provided names)
            qident(column_name)
        }
    }

    /// Get the cached quoted table name
    #[inline]
    fn get_quoted_table_name(&self) -> &str {
        &self.quoted_table_name
    }

    fn column_by_column_name(&self, column_name: &str) -> Option<PostgresSqlColumn> {
        self.column_cache
            .get(column_name)
            .map(|&i| self.postgresql_columns[i].clone())
    }

    /// Returns a mapping of column names to their abstract column types.
    ///
    /// This method provides column type information needed for building SQL WHERE
    /// clauses with the `Condition` type from `codcel_table_engine`. The returned
    /// types are abstract representations that map to PostgreSQL's native types.
    ///
    /// # Returns
    ///
    /// A [`HashMap`] where keys are column names and values are the corresponding
    /// [`ColumnType`] enum variants.
    pub fn get_abstract_column_types(&self) -> HashMap<String, ColumnType> {
        let mut column_types: HashMap<String, ColumnType> = HashMap::new();
        for col in &self.postgresql_columns {
            column_types.insert(col.column_name.clone(), pg_type_to_column_type(&col.sql_type));
        }
        column_types
    }

    // New helper that always binds as Option<T>
    fn bind_value_for_col_type_nullable(
        &self,
        args: &mut PgArguments,
        col_type: &PostgreSqlColumnType,
        value: &Value,
        value_format: &ValueFormat,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match col_type {
            PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                if value.is_none() {
                    let v: Option<i32> = None;
                    let _ = args.add(v);
                } else {
                    let v = value.i32(value_format)?;
                    let _ = args.add(Some(v));
                }
            }
            PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                if value.is_none() {
                    let v: Option<f64> = None;
                    let _ = args.add(v);
                } else {
                    let v = value.f64(value_format)?;
                    let _ = args.add(Some(v));
                }
            }
            PostgreSqlColumnType::Boolean => {
                if value.is_none() {
                    let v: Option<bool> = None;
                    let _ = args.add(v);
                } else {
                    let v = value.bool(value_format)?;
                    let _ = args.add(Some(v));
                }
            }
            PostgreSqlColumnType::Text => {
                if value.is_none() {
                    let v: Option<String> = None;
                    let _ = args.add(v);
                } else {
                    let v = value.string(value_format)?;
                    let _ = args.add(Some(v));
                }
            }
            PostgreSqlColumnType::Bytea => {
                if value.is_none() {
                    let v: Option<Vec<u8>> = None;
                    let _ = args.add(v);
                } else {
                    let v = value.string(value_format)?;
                    let bytes = v.into_bytes();
                    let _ = args.add(Some(bytes));
                }
            }
            // Your schema treats these as numeric f64; keep that mapping consistent.
            PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                if value.is_none() {
                    let v: Option<f64> = None;
                    let _ = args.add(v);
                } else {
                    let v = value.f64(value_format)?;
                    let _ = args.add(Some(v));
                }
            }
        }
        Ok(())
    }

    /// Replace the existing binder to use the nullable helper above.
    fn bind_non_id_values(
        &self,
        args: &mut PgArguments,
        values: &[Value],
        value_format: &ValueFormat,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (i, col) in self.postgresql_columns.iter().enumerate().skip(1) {
            let value = &values[i - 1];
            self.bind_value_for_col_type_nullable(args, &col.sql_type, value, value_format)?;
        }
        Ok(())
    }

    fn get_add_row_arguments(
        &self,
        values: &[Value],
        value_format: &ValueFormat,
        id: &str,
    ) -> Result<PgArguments, Box<dyn Error + Send + Sync>> {
        let mut args = PgArguments::default();
        args.reserve(self.postgresql_columns.len(), 0);

        // First placeholder is the ID
        let _ = args.add(&id);

        // Then all non-ID values in order
        self.bind_non_id_values(&mut args, values, value_format)?;

        Ok(args)
    }

    fn get_value_at_column_name_pos(
        &self,
        row: &Option<PgRow>,
        column_name: &str,
        pos: usize,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if let Some(row) = row {
            if let Some(column) = self.column_by_column_name(column_name) {
                return match column.sql_type {
                    PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                        let v: Option<i32> = row.try_get(pos)?;
                        match v {
                            Some(val) => Ok(Value::I32(val)),
                            _ => Ok(Value::OptionI32(None)),
                        }
                    }
                    PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                        let v: Option<f64> = row.try_get(pos)?;
                        match v {
                            Some(val) => Ok(Value::F64(val)),
                            None => Ok(Value::OptionF64(None)),
                        }
                    }
                    PostgreSqlColumnType::Boolean => {
                        let v: Option<bool> = row.try_get(pos)?;
                        match v {
                            Some(val) => Ok(Value::Bool(val)),
                            None => Ok(Value::OptionBool(None)),
                        }
                    }
                    PostgreSqlColumnType::Text => {
                        let v: Option<String> = row.try_get(pos)?;
                        Ok(match v {
                            Some(val) => Value::String(val),
                            None => Value::OptionString(None),
                        })
                    }
                    PostgreSqlColumnType::Bytea => {
                        let v: Option<Vec<u8>> = row.try_get(0)?;
                        Ok(match v {
                            Some(bytes) => {
                                let s = String::from_utf8_lossy(&bytes).into_owned();
                                Value::String(s)
                            }
                            None => Value::OptionString(None),
                        })
                    }
                    PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                        let v: Option<f64> = row.try_get(0)?;
                        Ok(match v {
                            Some(val) => Value::F64(val),
                            None => Value::OptionF64(None),
                        })
                    }
                };
            }
        }

        Ok(Value::None)
    }
}

#[async_trait]
impl CodcelTable for PostgreSQLTable {
    /// Performs a vertical lookup (VLOOKUP) operation on the table.
    ///
    /// Searches for `lookup_value` in the `search_column_index` column and returns
    /// the corresponding value from `result_column_index` column. Similar to Excel's
    /// VLOOKUP function.
    ///
    /// # Arguments
    ///
    /// * `lookup_value` - The value to search for (as a string)
    /// * `result_column_index` - Name of the column to return results from
    /// * `search_column_index` - Name of the column to search in
    /// * `range` - If `Some(true)`, uses range lookup (`<=` comparison); if `None` or `Some(false)`, uses exact match
    /// * `table_functions` - Optional table functions for dynamic value resolution
    /// * `input` - Input context for table function evaluation
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// The value from `result_column_index` in the matching row.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup value is not found in the search column.
    #[allow(clippy::too_many_arguments)]
    async fn v_lookup(
        &self,
        lookup_value: &str,
        result_column_index: &str,
        search_column_index: &str,
        range: Option<bool>,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let operator = if range == Some(true) { "<=" } else { "=" };
        let quoted_result_col = self.get_quoted_column_name(result_column_index);
        let quoted_search_col = self.get_quoted_column_name(search_column_index);
        let quoted_table = self.get_quoted_table_name();

        // Try to parse the lookup value as a number first
        let sql_query = if lookup_value.parse::<f64>().is_ok() {
            format!(
                "SELECT {} FROM {} WHERE CAST({} AS FLOAT) {} CAST($1 AS FLOAT) ORDER BY {} DESC LIMIT 1",
                quoted_result_col, quoted_table, quoted_search_col, operator, quoted_search_col
            )
        } else {
            format!(
                "SELECT {} FROM {} WHERE {} {} $1 ORDER BY {} DESC LIMIT 1",
                quoted_result_col, quoted_table, quoted_search_col, operator, quoted_search_col
            )
        };

        let mut args = PgArguments::default();
        args.reserve(1, 0);
        let _ = args.add(lookup_value);

        let row = sqlx::query_with(&sql_query, args)
            .fetch_optional(&self.db)
            .await?;

        let mut response_value = self.get_value_at_column_name_pos(&row, result_column_index, 0)?;

        response_value = PostgreSQLTable::apply_table_functions_to_value(
            response_value,
            table_functions,
            input,
            value_format,
        )
        .await;

        if !response_value.is_none() {
            Ok(response_value)
        } else {
            Err(format!(
                "VLOOKUP: Search value {lookup_value} does not exist at column {:} for table {}",
                &result_column_index, &self.table_name
            )
            .into())
        }
    }

    /// Finds the position of a value in a column or row (MATCH function).
    ///
    /// When `row == 0`, performs a vertical search in the specified column and returns
    /// the 1-based row index. When `row > 0`, performs a horizontal search across
    /// the specified columns in that row and returns the 1-based column position.
    ///
    /// # Arguments
    ///
    /// * `match_value` - The value to search for
    /// * `match_type` - Match behavior:
    ///   - `-1`: Find smallest value >= match_value (data must be in descending order)
    ///   - `0`: Find exact match
    ///   - `1` (default): Find largest value <= match_value (data must be in ascending order)
    /// * `column` - Column name for vertical search, or comma-separated column names for horizontal search
    /// * `row` - Row number for horizontal search (0 for vertical search)
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// The 1-based position of the match as `Value::I32`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The column doesn't exist
    /// * The match value is not found
    /// * An invalid match_type is provided
    async fn match_table(
        &self,
        match_value: &str,
        match_type: Option<i32>,
        column: &str,
        row: u32,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let match_type = match_type.unwrap_or(1);

        if row == 0 {
            // Vertical column search: return the 1-based row index of the matched row
            let col = self.column_by_column_name(column).ok_or_else(|| {
                format!(
                    "MATCH: Column {column} does not exist for table {}",
                    &self.table_name
                )
            })?;

            // Determine comparison operator and ordering
            let (op, dir) = match match_type {
                -1 => (">=", "ASC"),
                0 => ("=", "ASC"),
                1 => ("<=", "DESC"),
                _ => {
                    return Err(format!(
                        "MATCH: The match_type must be -1, 0 or 1.  {match_type} is not permitted"
                    )
                    .into())
                }
            };

            // Build comparison expressions depending on column type
            let quoted_col = self.get_quoted_column_name(column);
            let (lhs, rhs, order_col, bind_val) = match col.sql_type {
                PostgreSqlColumnType::Text => (
                    format!("UPPER({})", quoted_col),
                    "UPPER($1)".to_string(),
                    format!("UPPER({})", quoted_col),
                    match_value.to_string(),
                ),
                _ => (
                    format!("CAST({} AS FLOAT)", quoted_col),
                    "CAST($1 AS FLOAT)".to_string(),
                    quoted_col.clone(),
                    match_value.replace(&value_format.decimal_separator, "."),
                ),
            };

            // Use a derived row number (rn) to emulate Parquet's c0
            let quoted_table = self.get_quoted_table_name();
            let base_sql = format!(
                "WITH s AS (SELECT ROW_NUMBER() OVER () AS rn, * FROM {tbl}) SELECT rn FROM s WHERE {lhs} {op} {rhs}",
                tbl = quoted_table,
                lhs = lhs,
                op = op,
                rhs = rhs
            );

            let sql_query = if match_type == 0 {
                format!("{base} LIMIT 1", base = base_sql)
            } else {
                format!(
                    "{base} ORDER BY {order_col} {dir}, rn {dir} LIMIT 1",
                    base = base_sql,
                    order_col = order_col,
                    dir = dir
                )
            };

            let row_opt: Option<i64> = sqlx::query_scalar(&sql_query)
                .bind(bind_val)
                .fetch_optional(&self.db)
                .await?;

            if let Some(rn) = row_opt {
                // Return as 1-based position
                return Ok(Value::I32(rn as i32));
            }
        } else {
            // Horizontal row search: compute position within the selected columns on a given row
            // Parse and quote column names to prevent SQL injection
            let selected_cols: Vec<String> = column
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let quoted_cols = selected_cols
                .iter()
                .map(|c| self.get_quoted_column_name(c))
                .collect::<Vec<_>>()
                .join(", ");

            // Fetch the row using OFFSET/LIMIT (more efficient than ROW_NUMBER CTE)
            let quoted_table = self.get_quoted_table_name();
            let sql_query = format!(
                "SELECT {cols} FROM {tbl} LIMIT 1 OFFSET $1",
                tbl = quoted_table,
                cols = quoted_cols
            );

            let row_pg = sqlx::query(&sql_query)
                .bind((row - 1) as i64) // Convert 1-based row to 0-based offset
                .fetch_optional(&self.db)
                .await?;

            if let Some(pg_row) = row_pg {
                // Map selected columns into Value list
                let mut values: Vec<Value> = Vec::with_capacity(selected_cols.len());
                for (idx, col_name) in selected_cols.iter().enumerate() {
                    if let Some(col_def) = self.column_by_column_name(col_name) {
                        match col_def.sql_type {
                            PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                                let v: Option<i32> = pg_row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::I32(x),
                                    None => Value::OptionI32(None),
                                });
                            }
                            PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                                let v: Option<f64> = pg_row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::F64(x),
                                    None => Value::OptionF64(None),
                                });
                            }
                            PostgreSqlColumnType::Boolean => {
                                let v: Option<bool> = pg_row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::Bool(x),
                                    None => Value::OptionBool(None),
                                });
                            }
                            PostgreSqlColumnType::Text => {
                                let v: Option<String> = pg_row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::String(x),
                                    None => Value::OptionString(None),
                                });
                            }
                            PostgreSqlColumnType::Bytea => {
                                let v: Option<Vec<u8>> = pg_row.try_get(idx)?;
                                values.push(match v {
                                    Some(bytes) => {
                                        Value::String(String::from_utf8_lossy(&bytes).into_owned())
                                    }
                                    None => Value::OptionString(None),
                                });
                            }
                            PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                                let v: Option<f64> = pg_row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::F64(x),
                                    None => Value::OptionF64(None),
                                });
                            }
                        }
                    } else {
                        let v: Option<String> = pg_row.try_get(idx)?;
                        values.push(match v {
                            Some(x) => Value::String(x),
                            None => Value::OptionString(None),
                        });
                    }
                }

                // Prepare match value (normalize decimal separator and case like Parquet search_value_pure)
                let mv = match_value
                    .replace(&value_format.decimal_separator, ".")
                    .to_uppercase();

                // Decide collection type and search accordingly
                let all_f64 = values.iter().all(|v| matches!(v, Value::F64(_)));
                let all_i32 = values.iter().all(|v| matches!(v, Value::I32(_)));
                let mixed_f64_i32 = values
                    .iter()
                    .all(|v| matches!(v, Value::F64(_) | Value::I32(_)));
                let contains_string = values.iter().any(|v| matches!(v, Value::String(_)));

                let pos_opt = if all_f64 {
                    let vals: Vec<f64> = values
                        .into_iter()
                        .map(|v| match v {
                            Value::F64(x) => x,
                            _ => 0.0,
                        })
                        .collect();
                    match match_type {
                        -1 => find_smallest_position(&vals, &mv),
                        0 => find_exact_position(&vals, &mv),
                        1 => find_largest_position(&vals, &mv),
                        _ => None,
                    }
                } else if all_i32 {
                    let vals: Vec<i32> = values
                        .into_iter()
                        .map(|v| match v {
                            Value::I32(x) => x,
                            _ => 0,
                        })
                        .collect();
                    match match_type {
                        -1 => find_smallest_position(&vals, &mv),
                        0 => find_exact_position(&vals, &mv),
                        1 => find_largest_position(&vals, &mv),
                        _ => None,
                    }
                } else if mixed_f64_i32 {
                    let vals: Vec<f64> = values
                        .into_iter()
                        .map(|v| match v {
                            Value::F64(x) => x,
                            Value::I32(x) => x as f64,
                            _ => 0.0,
                        })
                        .collect();
                    match match_type {
                        -1 => find_smallest_position(&vals, &mv),
                        0 => find_exact_position(&vals, &mv),
                        1 => find_largest_position(&vals, &mv),
                        _ => None,
                    }
                } else if contains_string {
                    let vals: Vec<String> = values
                        .into_iter()
                        .map(|v| match v {
                            Value::F64(x) => x.to_string(),
                            Value::I32(x) => x.to_string(),
                            Value::String(s) => s,
                            _ => String::new(),
                        })
                        .collect();
                    match match_type {
                        -1 => find_smallest_position(&vals, &mv),
                        0 => find_exact_position(&vals, &mv),
                        1 => find_largest_position(&vals, &mv),
                        _ => None,
                    }
                } else {
                    None
                };

                if let Some(pos0) = pos_opt {
                    return Ok(Value::I32((pos0 + 1) as i32));
                } else {
                    return Err("MATCH: Position not found".into());
                }
            }
        }

        Err(format!(
            "MATCH: Search value {match_value} does not exist for table {} and match type {}",
            &self.table_name, match_type
        )
        .into())
    }

    /// Returns a value or range of values from the table by position (INDEX function).
    ///
    /// This function retrieves data from the table based on row and column indices,
    /// similar to Excel's INDEX function. It supports several access patterns:
    ///
    /// - `row=0, column=0`: Returns the entire table as an area
    /// - `row=0, column>0`: Returns an entire column as a vector
    /// - `row>0, column=0`: Returns an entire row as a vector
    /// - `row>0, column>0`: Returns a single cell value
    /// - `row>0, column=None`: Context-dependent (entire row if multiple rows exist, else cell)
    ///
    /// # Arguments
    ///
    /// * `row` - 1-based row index (0 for all rows)
    /// * `column` - 1-based column index, `Some(0)` for all columns, or `None` for context-dependent behavior
    /// * `table_functions` - Optional table functions for dynamic value resolution
    /// * `input` - Input context for table function evaluation
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// Depending on the arguments:
    /// - `Value::AreaValue` for entire table
    /// - `Value::VecValue` for entire row or column
    /// - Single `Value` for a specific cell
    ///
    /// # Errors
    ///
    /// Returns an error if the specified row or column position doesn't exist.
    async fn index(
        &self,
        row: i32,
        column: Option<i32>,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // In Postgres there is no physical c0 row-number column. We emulate it with
        // ROW_NUMBER() OVER () as rn when we need to address rows by index.
        let col_idx = column.unwrap_or(-1);

        // Helper: obtain the list of "data" columns to mirror Parquet's generate_column_string()
        // If the first column is named "c0" (t_table mode), skip it for data operations.
        let data_cols: Vec<String> = if self
            .postgresql_columns
            .first()
            .map(|c| c.column_name.as_str() == "c0")
            .unwrap_or(false)
        {
            self.postgresql_columns
                .iter()
                .skip(1)
                .map(|c| c.column_name.clone())
                .collect()
        } else {
            self.postgresql_columns
                .iter()
                .map(|c| c.column_name.clone())
                .collect()
        };

        let data_cols_len = data_cols.len();
        // Quote all column names for SQL safety (use cached where available)
        let all_cols_quoted: Vec<String> = data_cols.iter().map(|c| self.get_quoted_column_name(c)).collect();
        let all_cols_str = all_cols_quoted.join(", ");
        let quoted_table = self.get_quoted_table_name();

        // Case 1: Entire table (area)
        if row == 0 && col_idx == 0 {
            return self
                .select_all(&all_cols_str, table_functions, input, value_format)
                .await;
        }

        // Case 2: Row == 0 (return an entire column as a vector, or all cells as a flat vector)
        if row == 0 {
            if col_idx != -1 {
                // Return a single column by 1-based index
                if col_idx <= 0 || (col_idx as usize) > data_cols_len {
                    return Err(format!(
                        "Index: Row {row} and column {col_idx} position does not exist for table {}",
                        &self.table_name
                    )
                    .into());
                }
                let col_name = &data_cols[(col_idx - 1) as usize];
                let quoted_col = self.get_quoted_column_name(col_name);
                let sql_query = format!(
                    "SELECT {col} FROM {tbl}",
                    col = quoted_col,
                    tbl = quoted_table
                );
                let rows: Vec<PgRow> = sqlx::query(&sql_query).fetch_all(&self.db).await?;

                // Map all rows for this single column
                let mut out: Vec<Value> = Vec::new();
                if let Some(col_meta) = self.column_by_column_name(col_name) {
                    // First collect all values (with error handling)
                    let mut raw_vals: Vec<Value> = Vec::with_capacity(rows.len());
                    for rowv in rows.into_iter() {
                        let val = match col_meta.sql_type {
                            PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                                let v: Option<i32> = rowv.try_get(0)?;
                                match v {
                                    Some(x) => Value::I32(x),
                                    None => Value::OptionI32(None),
                                }
                            }
                            PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                                let v: Option<f64> = rowv.try_get(0)?;
                                match v {
                                    Some(x) => Value::F64(x),
                                    None => Value::OptionF64(None),
                                }
                            }
                            PostgreSqlColumnType::Boolean => {
                                let v: Option<bool> = rowv.try_get(0)?;
                                match v {
                                    Some(x) => Value::Bool(x),
                                    None => Value::OptionBool(None),
                                }
                            }
                            PostgreSqlColumnType::Text => {
                                let v: Option<String> = rowv.try_get(0)?;
                                match v {
                                    Some(x) => Value::String(x),
                                    None => Value::OptionString(None),
                                }
                            }
                            PostgreSqlColumnType::Bytea => {
                                let v: Option<Vec<u8>> = rowv.try_get(0)?;
                                match v {
                                    Some(bytes) => {
                                        Value::String(String::from_utf8_lossy(&bytes).into_owned())
                                    }
                                    None => Value::OptionString(None),
                                }
                            }
                            PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                                let v: Option<f64> = rowv.try_get(0)?;
                                match v {
                                    Some(x) => Value::F64(x),
                                    None => Value::OptionF64(None),
                                }
                            }
                        };
                        raw_vals.push(val);
                    }
                    // Then apply table functions concurrently
                    out = stream::iter(raw_vals)
                        .map(|v| async {
                            PostgreSQLTable::apply_table_functions_to_value(
                                v,
                                table_functions,
                                input,
                                value_format,
                            )
                            .await
                        })
                        .buffer_unordered(10)
                        .collect()
                        .await;
                }

                if !out.is_empty() {
                    return Ok(Value::VecValue(out));
                }
            } else {
                // Return all cells as a flat vector (column-major), similar to Parquet's sql_query_responses
                if data_cols_len == 0 {
                    return Err("Index: No columns available".into());
                }
                let sql_query = format!(
                    "SELECT {cols} FROM {tbl}",
                    cols = all_cols_str,
                    tbl = quoted_table
                );
                let rows: Vec<PgRow> = sqlx::query(&sql_query).fetch_all(&self.db).await?;

                // Build a column-major flattened vector - first collect all values (with error handling)
                let mut raw_vals: Vec<Value> = Vec::with_capacity(rows.len() * data_cols_len);
                for (ci, col_name) in data_cols.iter().enumerate() {
                    if let Some(col_meta) = self.column_by_column_name(col_name) {
                        for rowv in rows.iter() {
                            let val = match col_meta.sql_type {
                                PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                                    let v: Option<i32> = rowv.try_get(ci)?;
                                    match v {
                                        Some(x) => Value::I32(x),
                                        None => Value::OptionI32(None),
                                    }
                                }
                                PostgreSqlColumnType::Real
                                | PostgreSqlColumnType::DoublePrecision => {
                                    let v: Option<f64> = rowv.try_get(ci)?;
                                    match v {
                                        Some(x) => Value::F64(x),
                                        None => Value::OptionF64(None),
                                    }
                                }
                                PostgreSqlColumnType::Boolean => {
                                    let v: Option<bool> = rowv.try_get(ci)?;
                                    match v {
                                        Some(x) => Value::Bool(x),
                                        None => Value::OptionBool(None),
                                    }
                                }
                                PostgreSqlColumnType::Text => {
                                    let v: Option<String> = rowv.try_get(ci)?;
                                    match v {
                                        Some(x) => Value::String(x),
                                        None => Value::OptionString(None),
                                    }
                                }
                                PostgreSqlColumnType::Bytea => {
                                    let v: Option<Vec<u8>> = rowv.try_get(ci)?;
                                    match v {
                                        Some(bytes) => Value::String(
                                            String::from_utf8_lossy(&bytes).into_owned(),
                                        ),
                                        None => Value::OptionString(None),
                                    }
                                }
                                PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                                    let v: Option<f64> = rowv.try_get(ci)?;
                                    match v {
                                        Some(x) => Value::F64(x),
                                        None => Value::OptionF64(None),
                                    }
                                }
                            };
                            raw_vals.push(val);
                        }
                    }
                }
                // Then apply table functions concurrently
                if !raw_vals.is_empty() {
                    let out: Vec<Value> = stream::iter(raw_vals)
                        .map(|v| async {
                            PostgreSQLTable::apply_table_functions_to_value(
                                v,
                                table_functions,
                                input,
                                value_format,
                            )
                            .await
                        })
                        .buffer_unordered(10)
                        .collect()
                        .await;
                    return Ok(Value::VecValue(out));
                }
            }
        }

        // Case 3: Column specified
        if col_idx != -1 {
            if col_idx == 0 {
                // Entire row at given row number using OFFSET/LIMIT (more efficient than ROW_NUMBER CTE)
                if data_cols_len == 0 {
                    return Err("Index: No columns available".into());
                }
                let sql_query = format!(
                    "SELECT {cols} FROM {tbl} LIMIT 1 OFFSET $1",
                    cols = all_cols_str,
                    tbl = quoted_table
                );
                if let Some(rowv) = sqlx::query(&sql_query)
                    .bind(row - 1) // Convert 1-based row to 0-based offset
                    .fetch_optional(&self.db)
                    .await?
                {
                    // First collect all values (with error handling)
                    let mut raw_vals: Vec<Value> = Vec::with_capacity(data_cols_len);
                    for (idx, col_name) in data_cols.iter().enumerate() {
                        let val = if let Some(col_meta) = self.column_by_column_name(col_name) {
                            match col_meta.sql_type {
                                PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                                    let v: Option<i32> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::I32(x),
                                        None => Value::OptionI32(None),
                                    }
                                }
                                PostgreSqlColumnType::Real
                                | PostgreSqlColumnType::DoublePrecision => {
                                    let v: Option<f64> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::F64(x),
                                        None => Value::OptionF64(None),
                                    }
                                }
                                PostgreSqlColumnType::Boolean => {
                                    let v: Option<bool> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::Bool(x),
                                        None => Value::OptionBool(None),
                                    }
                                }
                                PostgreSqlColumnType::Text => {
                                    let v: Option<String> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::String(x),
                                        None => Value::OptionString(None),
                                    }
                                }
                                PostgreSqlColumnType::Bytea => {
                                    let v: Option<Vec<u8>> = rowv.try_get(idx)?;
                                    match v {
                                        Some(bytes) => Value::String(
                                            String::from_utf8_lossy(&bytes).into_owned(),
                                        ),
                                        None => Value::OptionString(None),
                                    }
                                }
                                PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                                    let v: Option<f64> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::F64(x),
                                        None => Value::OptionF64(None),
                                    }
                                }
                            }
                        } else {
                            let v: Option<String> = rowv.try_get(idx)?;
                            match v {
                                Some(x) => Value::String(x),
                                None => Value::OptionString(None),
                            }
                        };
                        raw_vals.push(val);
                    }
                    // Then apply table functions concurrently
                    let row_vals: Vec<Value> = stream::iter(raw_vals)
                        .map(|v| async {
                            PostgreSQLTable::apply_table_functions_to_value(
                                v,
                                table_functions,
                                input,
                                value_format,
                            )
                            .await
                        })
                        .buffer_unordered(10)
                        .collect()
                        .await;
                    return Ok(Value::VecValue(row_vals));
                }
            } else {
                // Single cell: given (row, column) using OFFSET/LIMIT (more efficient than ROW_NUMBER CTE)
                if col_idx <= 0 || (col_idx as usize) > data_cols_len {
                    return Err(format!(
                        "Index: Row {row} and column {col_idx} position does not exist for table {}",
                        &self.table_name
                    )
                    .into());
                }
                let col_name = &data_cols[(col_idx - 1) as usize];
                let quoted_col = self.get_quoted_column_name(col_name);
                let sql_query = format!(
                    "SELECT {col} FROM {tbl} LIMIT 1 OFFSET $1",
                    col = quoted_col,
                    tbl = quoted_table
                );
                if let Some(rowv) = sqlx::query(&sql_query)
                    .bind(row - 1) // Convert 1-based row to 0-based offset
                    .fetch_optional(&self.db)
                    .await?
                {
                    let val = self.get_value_at_column_name_pos(&Some(rowv), col_name, 0)?;
                    let val = PostgreSQLTable::apply_table_functions_to_value(
                        val,
                        table_functions,
                        input,
                        value_format,
                    )
                    .await;
                    return Ok(val);
                }
            }
        } else {
            // Case 4: Column unspecified. Follow Parquet behavior:
            // If there are multiple rows, return the entire row at index `row`.
            // If there is only one row, treat `row` as the column index and return that cell from the first row.
            // Use pre-built query template for better prepared statement caching
            let number_rows: i64 = sqlx::query_scalar(&self.query_templates.count_rows)
                .fetch_one(&self.db)
                .await?;
            if number_rows > 1 {
                // Entire row using OFFSET/LIMIT (more efficient than ROW_NUMBER CTE)
                if data_cols_len == 0 {
                    return Err("Index: No columns available".into());
                }
                let sql_query = format!(
                    "SELECT {cols} FROM {tbl} LIMIT 1 OFFSET $1",
                    cols = all_cols_str,
                    tbl = quoted_table
                );
                if let Some(rowv) = sqlx::query(&sql_query)
                    .bind(row - 1) // Convert 1-based row to 0-based offset
                    .fetch_optional(&self.db)
                    .await?
                {
                    // First collect all values (with error handling)
                    let mut raw_vals: Vec<Value> = Vec::with_capacity(data_cols_len);
                    for (idx, col_name) in data_cols.iter().enumerate() {
                        let val = if let Some(col_meta) = self.column_by_column_name(col_name) {
                            match col_meta.sql_type {
                                PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                                    let v: Option<i32> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::I32(x),
                                        None => Value::OptionI32(None),
                                    }
                                }
                                PostgreSqlColumnType::Real
                                | PostgreSqlColumnType::DoublePrecision => {
                                    let v: Option<f64> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::F64(x),
                                        None => Value::OptionF64(None),
                                    }
                                }
                                PostgreSqlColumnType::Boolean => {
                                    let v: Option<bool> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::Bool(x),
                                        None => Value::OptionBool(None),
                                    }
                                }
                                PostgreSqlColumnType::Text => {
                                    let v: Option<String> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::String(x),
                                        None => Value::OptionString(None),
                                    }
                                }
                                PostgreSqlColumnType::Bytea => {
                                    let v: Option<Vec<u8>> = rowv.try_get(idx)?;
                                    match v {
                                        Some(bytes) => Value::String(
                                            String::from_utf8_lossy(&bytes).into_owned(),
                                        ),
                                        None => Value::OptionString(None),
                                    }
                                }
                                PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                                    let v: Option<f64> = rowv.try_get(idx)?;
                                    match v {
                                        Some(x) => Value::F64(x),
                                        None => Value::OptionF64(None),
                                    }
                                }
                            }
                        } else {
                            let v: Option<String> = rowv.try_get(idx)?;
                            match v {
                                Some(x) => Value::String(x),
                                None => Value::OptionString(None),
                            }
                        };
                        raw_vals.push(val);
                    }
                    // Then apply table functions concurrently
                    let row_vals: Vec<Value> = stream::iter(raw_vals)
                        .map(|v| async {
                            PostgreSQLTable::apply_table_functions_to_value(
                                v,
                                table_functions,
                                input,
                                value_format,
                            )
                            .await
                        })
                        .buffer_unordered(10)
                        .collect()
                        .await;
                    return Ok(Value::VecValue(row_vals));
                }
            } else {
                // Only one row exists: use `row` as column index
                if row <= 0 || (row as usize) > data_cols_len {
                    return Err(format!(
                        "Index: Row {row} and column none position does not exist for table {}",
                        &self.table_name
                    )
                    .into());
                }
                let col_name = &data_cols[(row - 1) as usize];
                let quoted_col = self.get_quoted_column_name(col_name);
                let sql_query = format!(
                    "SELECT {col} FROM {tbl} LIMIT 1",
                    col = quoted_col,
                    tbl = quoted_table
                );
                if let Some(rowv) = sqlx::query(&sql_query).fetch_optional(&self.db).await? {
                    let val = self.get_value_at_column_name_pos(&Some(rowv), col_name, 0)?;
                    let val = PostgreSQLTable::apply_table_functions_to_value(
                        val,
                        table_functions,
                        input,
                        value_format,
                    )
                    .await;
                    return Ok(val);
                }
            }
        }

        // If we reached here, no value was produced
        let col_desc = if col_idx != -1 {
            col_idx.to_string()
        } else {
            "none".to_string()
        };
        Err(format!(
            "Index: Row {row} and column {col} position does not exist for table {tbl}",
            col = col_desc,
            tbl = &self.table_name
        )
        .into())
    }

    /// Performs a horizontal lookup (HLOOKUP) operation on the table.
    ///
    /// Searches for `lookup_value` in the first row of the specified columns and
    /// returns the value from the same column in the `row_index` row. Similar to
    /// Excel's HLOOKUP function.
    ///
    /// # Arguments
    ///
    /// * `lookup_value` - The value to search for in the first row
    /// * `row_index` - The 1-based row index from which to return the result
    /// * `range` - If `Some(true)`, uses approximate match; if `None` or `Some(false)`, uses exact match
    /// * `table_functions` - Optional table functions for dynamic value resolution
    /// * `input` - Input context for table function evaluation
    /// * `column` - Comma-separated column names to search across
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// The value from `row_index` in the column where the lookup value was found.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup value is not found in the first row.
    #[allow(clippy::too_many_arguments)]
    async fn h_lookup(
        &self,
        lookup_value: &str,
        row_index: i32,
        range: Option<bool>,
        table_functions: &TableFunctions,
        input: &Input,
        column: &str,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Mirror Parquet implementation: find the matching column in the first row,
        // then return the value at (row_index, found_column)
        let range = range.unwrap_or(false);
        let match_type = if range { Some(1) } else { Some(0) };

        if let Ok(value) = self
            .match_table(lookup_value, match_type, column, 1, value_format)
            .await
        {
            if let Ok(found_column) = value.i32(value_format) {
                if let Ok(result) = self
                    .index(
                        row_index,
                        Some(found_column),
                        table_functions,
                        input,
                        value_format,
                    )
                    .await
                {
                    return Ok(result);
                }
            }
        }

        Err(format!(
            "HLOOKUP: Search value {lookup_value} does not exist at row {row_index} for table {}",
            &self.table_name
        )
        .into())
    }

    /// Performs an extended lookup (XLOOKUP) operation with advanced matching options.
    ///
    /// XLOOKUP is a more powerful alternative to VLOOKUP and HLOOKUP, supporting
    /// multiple match modes and search directions. When `row == 0`, performs a
    /// vertical search; otherwise performs a horizontal search.
    ///
    /// # Arguments
    ///
    /// * `lookup_value` - The value to search for
    /// * `search_column` - Column name(s) to search in
    /// * `columns` - Column name(s) to return values from
    /// * `row` - Row number for horizontal search (0 for vertical search)
    /// * `if_not_found` - Optional value to return if no match is found
    /// * `match_mode` - Match behavior:
    ///   - `0`: Exact match (default)
    ///   - `1`: Exact match or next largest
    ///   - `-1`: Exact match or next smallest
    ///   - `2`: Wildcard match
    /// * `search_mode` - Search direction:
    ///   - `1`: Search first to last (default)
    ///   - `-1`: Search last to first
    ///   - `2`: Binary search ascending
    ///   - `-2`: Binary search descending
    /// * `table_functions` - Optional table functions for dynamic value resolution
    /// * `input` - Input context for table function evaluation
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// The value(s) from the return columns in the matching row. Returns a single
    /// value if one column is specified, or `Value::VecValue` for multiple columns.
    ///
    /// # Errors
    ///
    /// Returns an error if no match is found and `if_not_found` is `None`.
    #[allow(clippy::too_many_arguments)]
    async fn x_lookup(
        &self,
        lookup_value: &str,
        search_column: &str,
        columns: &str,
        row: u32,
        if_not_found: Option<String>,
        match_mode: Option<i32>,
        search_mode: Option<i32>,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Helper: mimic Parquet's search_value_column for Postgres types
        // Use proper escaping/quoting to prevent SQL injection
        let quoted_search_col = self.get_quoted_column_name(search_column);
        let mut lookup_expr = lookup_value.to_string();
        let mut search_col_expr = quoted_search_col.clone();
        if let Some(col) = self.column_by_column_name(search_column) {
            match col.sql_type {
                PostgreSqlColumnType::Text => {
                    let escaped = escape_sql_string(&lookup_value.to_uppercase());
                    lookup_expr = format!("'{}'", escaped);
                    search_col_expr = format!("UPPER({})", quoted_search_col);
                }
                _ => {
                    lookup_expr = lookup_value.replace(&value_format.decimal_separator, ".");
                }
            }
        }

        if row == 0 {
            // Vertical search
            let match_mode = match_mode.unwrap_or(X_MATCH_MODE_EXACT);
            let search_mode = search_mode.unwrap_or(X_SEARCH_MODE_FIRST);

            // Quote identifiers for SQL safety
            let quoted_columns = quote_columns(columns);
            let quoted_table = self.get_quoted_table_name();

            // Build SQL similar to Parquet's x_search_query
            let sql_query = match match_mode {
                X_MATCH_MODE_EXACT => match search_mode {
                    X_SEARCH_MODE_FIRST => {
                        format!(
                            "SELECT {} FROM {} WHERE {} = {} LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, lookup_expr
                        )
                    }
                    X_SEARCH_MODE_REVERSE => {
                        // Note: mirrors Parquet logic using MAX(c1)
                        format!(
                            "SELECT {} FROM {} WHERE {} = (SELECT MAX(\"c1\") FROM {} WHERE {} = {}) LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, quoted_table, search_col_expr, lookup_expr
                        )
                    }
                    X_SEARCH_MODE_BINARY_FIRST => {
                        format!(
                            "SELECT {} FROM {} WHERE {} = {} ORDER BY {} ASC LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, lookup_expr, search_col_expr
                        )
                    }
                    X_SEARCH_MODE_BINARY_LAST => {
                        format!(
                            "SELECT {} FROM {} WHERE {} = {} ORDER BY {} DESC LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, lookup_expr, search_col_expr
                        )
                    }
                    _ => String::new(),
                },
                X_MATCH_MODE_EXACT_NEXT_LARGEST => match search_mode {
                    X_SEARCH_MODE_FIRST => {
                        format!(
                            "SELECT {} FROM {} WHERE {} >= {} LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, lookup_expr
                        )
                    }
                    X_SEARCH_MODE_REVERSE => {
                        format!(
                            "SELECT {} FROM {} WHERE {} = (SELECT MIN(\"c1\") FROM {} WHERE {} >= {}) LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, quoted_table, search_col_expr, lookup_expr
                        )
                    }
                    X_SEARCH_MODE_BINARY_FIRST => {
                        format!(
                            "SELECT {} FROM {} WHERE {} >= {} ORDER BY {} ASC LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, lookup_expr, search_col_expr
                        )
                    }
                    X_SEARCH_MODE_BINARY_LAST => {
                        format!(
                            "SELECT {} FROM {} WHERE {} >= {} ORDER BY {} DESC LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, lookup_expr, search_col_expr
                        )
                    }
                    _ => String::new(),
                },
                X_MATCH_MODE_EXACT_NEXT_SMALLEST => match search_mode {
                    X_SEARCH_MODE_FIRST | X_SEARCH_MODE_REVERSE | X_SEARCH_MODE_BINARY_FIRST => {
                        format!(
                            "SELECT {} FROM {} WHERE {} = (SELECT MAX({}) FROM {} WHERE {} <= {}) LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, search_col_expr, quoted_table, search_col_expr, lookup_expr
                        )
                    }
                    X_SEARCH_MODE_BINARY_LAST => {
                        format!(
                            "SELECT {} FROM {} WHERE {} <= {} ORDER BY {} DESC LIMIT 1",
                            quoted_columns, quoted_table, search_col_expr, lookup_expr, search_col_expr
                        )
                    }
                    _ => String::new(),
                },
                X_MATCH_MODE_WILDCARD => String::new(),
                _ => String::new(),
            };

            if !sql_query.is_empty() {
                let selected_cols: Vec<String> = columns
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                let row_opt = sqlx::query(&sql_query).fetch_optional(&self.db).await?;
                if let Some(row) = row_opt {
                    return if selected_cols.len() == 1 {
                        let mut v =
                            self.get_value_at_column_name_pos(&Some(row), &selected_cols[0], 0)?;
                        v = PostgreSQLTable::apply_table_functions_to_value(
                            v,
                            table_functions,
                            input,
                            value_format,
                        )
                        .await;
                        Ok(v)
                    } else {
                        let row_wrap = Some(row);
                        // First collect all values (with error handling)
                        let values: Result<Vec<Value>, _> = selected_cols
                            .iter()
                            .enumerate()
                            .map(|(idx, col_name)| {
                                self.get_value_at_column_name_pos(&row_wrap, col_name, idx)
                            })
                            .collect();
                        let values = values?;
                        // Then apply table functions concurrently
                        let out_vals: Vec<Value> = stream::iter(values)
                            .map(|v| async {
                                PostgreSQLTable::apply_table_functions_to_value(
                                    v,
                                    table_functions,
                                    input,
                                    value_format,
                                )
                                .await
                            })
                            .buffer_unordered(10)
                            .collect()
                            .await;
                        Ok(Value::VecValue(out_vals))
                    };
                }
            }
        } else {
            // Horizontal search
            let match_mode = match_mode.unwrap_or_default();
            // Similar to Parquet's search_value_pure
            let mut match_value = lookup_value.replace(&value_format.decimal_separator, ".");
            if match_value.parse::<f64>().is_err() {
                match_value = lookup_value.to_uppercase();
            }

            // Save row parameter before it gets shadowed
            let row_num = row as i64;

            let quoted_search_cols: Vec<String> = search_column
                .split(',')
                .map(|s| self.get_quoted_column_name(s.trim()))
                .collect();
            let sql_query = format!(
                "SELECT {} FROM {} WHERE {} = $1 LIMIT 1",
                quoted_search_cols.join(", "),
                self.get_quoted_table_name(),
                self.get_quoted_column_name("c0")
            );
            let row_opt = sqlx::query(&sql_query)
                .bind(row_num)
                .fetch_optional(&self.db)
                .await?;
            if let Some(row) = row_opt {
                let selected_cols: Vec<String> = search_column
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                // Collect values from the row across the selected columns
                let mut values: Vec<Value> = Vec::with_capacity(selected_cols.len());
                for (idx, col_name) in selected_cols.iter().enumerate() {
                    if let Some(col) = self.column_by_column_name(col_name) {
                        match col.sql_type {
                            PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                                let v: Option<i32> = row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::I32(x),
                                    None => Value::OptionI32(None),
                                });
                            }
                            PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                                let v: Option<f64> = row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::F64(x),
                                    None => Value::OptionF64(None),
                                });
                            }
                            PostgreSqlColumnType::Boolean => {
                                let v: Option<bool> = row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::Bool(x),
                                    None => Value::OptionBool(None),
                                });
                            }
                            PostgreSqlColumnType::Text => {
                                let v: Option<String> = row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::String(x),
                                    None => Value::OptionString(None),
                                });
                            }
                            PostgreSqlColumnType::Bytea => {
                                let v: Option<Vec<u8>> = row.try_get(idx)?;
                                values.push(match v {
                                    Some(bytes) => {
                                        Value::String(String::from_utf8_lossy(&bytes).into_owned())
                                    }
                                    None => Value::OptionString(None),
                                });
                            }
                            PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                                let v: Option<f64> = row.try_get(idx)?;
                                values.push(match v {
                                    Some(x) => Value::F64(x),
                                    None => Value::OptionF64(None),
                                });
                            }
                        }
                    } else {
                        let v: Option<String> = row.try_get(idx)?;
                        values.push(match v {
                            Some(x) => Value::String(x),
                            None => Value::OptionString(None),
                        });
                    }
                }

                // Process horizontal match results similar to Parquet
                let search_mode = search_mode.unwrap_or(1);
                let mut pos_opt: Option<usize> = None;

                // Define helper to adjust ordering
                let adjust_order_f64 = |vals: &mut Vec<f64>| match search_mode {
                    -1 => vals.reverse(),
                    2 => vals.sort_by(|a, b| a.partial_cmp(b).unwrap()),
                    -2 => vals.sort_by(|a, b| b.partial_cmp(a).unwrap()),
                    _ => {}
                };
                let adjust_order_i32 = |vals: &mut Vec<i32>| match search_mode {
                    -1 => vals.reverse(),
                    2 => vals.sort(),
                    -2 => vals.sort_by(|a, b| b.cmp(a)),
                    _ => {}
                };
                let adjust_order_string = |vals: &mut Vec<String>| match search_mode {
                    -1 => vals.reverse(),
                    2 => vals.sort(),
                    -2 => vals.sort_by(|a, b| b.cmp(a)),
                    _ => {}
                };

                let all_f64 = values.iter().all(|v| matches!(v, Value::F64(_)));
                let all_i32 = values.iter().all(|v| matches!(v, Value::I32(_)));
                let mixed_f64_i32 = values
                    .iter()
                    .all(|v| matches!(v, Value::F64(_) | Value::I32(_)));
                let contains_string = values.iter().any(|v| matches!(v, Value::String(_)));

                if all_f64 {
                    let mut vals: Vec<f64> = values
                        .into_iter()
                        .filter_map(|v| match v {
                            Value::F64(x) => Some(x),
                            _ => None,
                        })
                        .collect();
                    adjust_order_f64(&mut vals);
                    let col = &vals as &dyn Searchable;
                    pos_opt = match match_mode {
                        -1 => find_smallest_position(col, &match_value),
                        0 => find_exact_position(col, &match_value),
                        1 => find_largest_position(col, &match_value),
                        2 => None,
                        _ => None,
                    };
                } else if all_i32 {
                    let mut vals: Vec<i32> = values
                        .into_iter()
                        .filter_map(|v| match v {
                            Value::I32(x) => Some(x),
                            _ => None,
                        })
                        .collect();
                    adjust_order_i32(&mut vals);
                    let col = &vals as &dyn Searchable;
                    pos_opt = match match_mode {
                        -1 => find_smallest_position(col, &match_value),
                        0 => find_exact_position(col, &match_value),
                        1 => find_largest_position(col, &match_value),
                        2 => None,
                        _ => None,
                    };
                } else if mixed_f64_i32 {
                    let mut vals: Vec<f64> = values
                        .into_iter()
                        .filter_map(|v| match v {
                            Value::F64(x) => Some(x),
                            Value::I32(x) => Some(x as f64),
                            _ => None,
                        })
                        .collect();
                    adjust_order_f64(&mut vals);
                    let col = &vals as &dyn Searchable;
                    pos_opt = match match_mode {
                        -1 => find_smallest_position(col, &match_value),
                        0 => find_exact_position(col, &match_value),
                        1 => find_largest_position(col, &match_value),
                        2 => None,
                        _ => None,
                    };
                } else if contains_string {
                    let mut vals: Vec<String> = values
                        .into_iter()
                        .filter_map(|v| match v {
                            Value::F64(x) => Some(x.to_string()),
                            Value::I32(x) => Some(x.to_string()),
                            Value::String(s) => Some(s),
                            _ => None,
                        })
                        .collect();
                    adjust_order_string(&mut vals);
                    let col = &vals as &dyn Searchable;
                    pos_opt = match match_mode {
                        -1 => find_smallest_position(col, &match_value),
                        0 => find_exact_position(col, &match_value),
                        1 => find_largest_position(col, &match_value),
                        2 => None,
                        _ => None,
                    };
                }

                if let Some(pos) = pos_opt {
                    let col_index = (pos + 1) as i32; // 1-based as in Parquet
                    let target_col = format!("c{}", col_index);
                    let sql_query = format!(
                        "SELECT {} FROM {} WHERE {} <> $1",
                        self.get_quoted_column_name(&target_col),
                        self.get_quoted_table_name(),
                        self.get_quoted_column_name("c0")
                    );
                    let rows: Vec<PgRow> = sqlx::query(&sql_query)
                        .bind(row_num)
                        .fetch_all(&self.db)
                        .await?;
                    // First collect all values (with error handling)
                    let values: Result<Vec<Value>, _> = rows
                        .into_iter()
                        .map(|pg_row| self.get_value_at_column_name_pos(&Some(pg_row), &target_col, 0))
                        .collect();
                    let values = values?;
                    // Then apply table functions concurrently
                    let out_vals: Vec<Value> = stream::iter(values)
                        .map(|v| async {
                            PostgreSQLTable::apply_table_functions_to_value(
                                v,
                                table_functions,
                                input,
                                value_format,
                            )
                            .await
                        })
                        .buffer_unordered(10)
                        .collect()
                        .await;
                    return Ok(Value::VecValue(out_vals));
                }
            }
        }

        // Not found handling
        if let Some(not_found) = if_not_found {
            Ok(Value::String(not_found))
        } else {
            Err(format!(
                "XSEARCH: Search value {} does not exist for table {}",
                lookup_value, &self.table_name
            )
            .into())
        }
    }

    /// Performs a standard lookup operation (LOOKUP function).
    ///
    /// This is a simplified lookup that finds the largest value less than or equal
    /// to the lookup value (equivalent to XLOOKUP with `match_mode = -1`).
    /// The search column must be sorted in ascending order.
    ///
    /// # Arguments
    ///
    /// * `lookup_value` - The value to search for
    /// * `search_column` - Column name(s) to search in
    /// * `columns` - Column name(s) to return values from
    /// * `row` - Row number for horizontal search (0 for vertical search)
    /// * `table_functions` - Optional table functions for dynamic value resolution
    /// * `input` - Input context for table function evaluation
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// The value(s) from the return columns in the matching row.
    ///
    /// # Errors
    ///
    /// Returns an error if no match is found.
    #[allow(clippy::too_many_arguments)]
    async fn lookup(
        &self,
        lookup_value: &str,
        search_column: &str,
        columns: &str,
        row: u32,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // We are using x_lookup for lookup, consistent with ParquetTable implementation
        self.x_lookup(
            lookup_value,
            search_column,
            columns,
            row,
            None,
            Some(-1),
            None,
            table_functions,
            input,
            value_format,
        )
        .await
    }

    /// Performs an extended match (XMATCH) operation with advanced matching options.
    ///
    /// XMATCH is an enhanced version of MATCH that supports multiple match modes
    /// and search directions. When `row == 0`, performs a vertical search and
    /// returns the 1-based row index; otherwise performs a horizontal search
    /// and returns the 1-based column position.
    ///
    /// # Arguments
    ///
    /// * `match_value` - The value to search for
    /// * `match_mode` - Match behavior:
    ///   - `0`: Exact match (default)
    ///   - `1`: Exact match or next largest
    ///   - `-1`: Exact match or next smallest
    ///   - `2`: Wildcard match
    /// * `search_mode` - Search direction:
    ///   - `1`: Search first to last (default)
    ///   - `-1`: Search last to first
    ///   - `2`: Binary search ascending
    ///   - `-2`: Binary search descending
    /// * `column` - Column name for vertical search, or comma-separated column names for horizontal search
    /// * `row` - Row number for horizontal search (0 for vertical search)
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// The 1-based position of the match as `Value::I32`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The column doesn't exist
    /// * The match value is not found
    #[allow(clippy::too_many_arguments)]
    async fn x_match(
        &self,
        match_value: &str,
        match_mode: Option<i32>,
        search_mode: Option<i32>,
        column: &str,
        row: u32,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Follow ParquetTable::x_match semantics adapted for PostgreSQL
        let match_mode = match_mode.unwrap_or(X_MATCH_MODE_EXACT);
        let search_mode = search_mode.unwrap_or(X_SEARCH_MODE_FIRST);

        if row == 0 {
            // VERTICAL COLUMN SEARCH: return the 1-based row index (like c0 in Parquet)
            let col = self.column_by_column_name(column).ok_or_else(|| {
                format!(
                    "XMATCH: Column {column} does not exist for table {}",
                    &self.table_name
                )
            })?;

            // Build comparison/ordering expressions and bound value
            let quoted_col = self.get_quoted_column_name(column);
            let (lhs, rhs, order_col, bind_val) = match col.sql_type {
                PostgreSqlColumnType::Text => (
                    format!("UPPER({})", quoted_col),
                    "UPPER($1)".to_string(),
                    format!("UPPER({})", quoted_col),
                    match_value.to_string(),
                ),
                _ => (
                    format!("CAST({} AS FLOAT)", quoted_col),
                    "CAST($1 AS FLOAT)".to_string(),
                    quoted_col.clone(),
                    match_value.replace(&value_format.decimal_separator, "."),
                ),
            };

            // Base CTE with row numbers to emulate Parquet's c0
            let quoted_table = self.get_quoted_table_name();
            let base = format!(
                "WITH s AS (SELECT ROW_NUMBER() OVER () AS rn, * FROM {tbl})",
                tbl = quoted_table
            );

            let sql_query = match match_mode {
                X_MATCH_MODE_EXACT => match search_mode {
                    X_SEARCH_MODE_FIRST => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} = {rhs} ORDER BY rn ASC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs
                    ),
                    X_SEARCH_MODE_REVERSE => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} = {rhs} ORDER BY rn DESC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs
                    ),
                    X_SEARCH_MODE_BINARY_FIRST => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} = {rhs} ORDER BY {order_col} ASC, rn ASC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs, order_col = order_col
                    ),
                    X_SEARCH_MODE_BINARY_LAST => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} = {rhs} ORDER BY {order_col} DESC, rn DESC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs, order_col = order_col
                    ),
                    _ => String::new(),
                },
                X_MATCH_MODE_EXACT_NEXT_LARGEST => match search_mode {
                    X_SEARCH_MODE_FIRST => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} >= {rhs} ORDER BY rn ASC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs
                    ),
                    X_SEARCH_MODE_REVERSE => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} >= {rhs} ORDER BY rn DESC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs
                    ),
                    X_SEARCH_MODE_BINARY_FIRST => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} >= {rhs} ORDER BY {order_col} ASC, rn ASC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs, order_col = order_col
                    ),
                    X_SEARCH_MODE_BINARY_LAST => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} >= {rhs} ORDER BY {order_col} DESC, rn DESC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs, order_col = order_col
                    ),
                    _ => String::new(),
                },
                X_MATCH_MODE_EXACT_NEXT_SMALLEST => match search_mode {
                    X_SEARCH_MODE_FIRST | X_SEARCH_MODE_REVERSE | X_SEARCH_MODE_BINARY_FIRST => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} = (SELECT MAX({order_col}) FROM s WHERE {lhs} <= {rhs}) LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs, order_col = order_col
                    ),
                    X_SEARCH_MODE_BINARY_LAST => format!(
                        "{base} SELECT rn FROM s WHERE {lhs} <= {rhs} ORDER BY {order_col} DESC, rn DESC LIMIT 1",
                        base = base, lhs = lhs, rhs = rhs, order_col = order_col
                    ),
                    _ => String::new(),
                },
                X_MATCH_MODE_WILDCARD => String::new(), // TODO: wildcard matching not implemented
                _ => String::new(),
            };

            if !sql_query.is_empty() {
                let rn_opt: Option<i64> = sqlx::query_scalar(&sql_query)
                    .bind(bind_val)
                    .fetch_optional(&self.db)
                    .await?;
                if let Some(rn) = rn_opt {
                    return Ok(Value::I32(rn as i32));
                }
            }

            return Err(format!(
                "XMATCH: Search value {match_value} does not exist for table {}",
                &self.table_name
            )
            .into());
        }

        // HORIZONTAL ROW SEARCH: match within a given row across the provided columns
        // Use OFFSET/LIMIT (more efficient than ROW_NUMBER CTE)
        let quoted_table = self.get_quoted_table_name();
        let quoted_cols = quote_columns(column);
        let sql_query = format!(
            "SELECT {cols} FROM {tbl} LIMIT 1 OFFSET $1",
            tbl = quoted_table,
            cols = quoted_cols
        );

        let row_pg = sqlx::query(&sql_query)
            .bind((row - 1) as i64) // Convert 1-based row to 0-based offset
            .fetch_optional(&self.db)
            .await?;

        if let Some(pg_row) = row_pg {
            // Gather the selected column values
            let selected_cols: Vec<String> = column
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            let mut values: Vec<Value> = Vec::with_capacity(selected_cols.len());
            for (idx, col_name) in selected_cols.iter().enumerate() {
                if let Some(col_def) = self.column_by_column_name(col_name) {
                    match col_def.sql_type {
                        PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                            let v: Option<i32> = pg_row.try_get(idx)?;
                            values.push(match v {
                                Some(x) => Value::I32(x),
                                None => Value::OptionI32(None),
                            });
                        }
                        PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                            let v: Option<f64> = pg_row.try_get(idx)?;
                            values.push(match v {
                                Some(x) => Value::F64(x),
                                None => Value::OptionF64(None),
                            });
                        }
                        PostgreSqlColumnType::Boolean => {
                            let v: Option<bool> = pg_row.try_get(idx)?;
                            values.push(match v {
                                Some(x) => Value::Bool(x),
                                None => Value::OptionBool(None),
                            });
                        }
                        PostgreSqlColumnType::Text => {
                            let v: Option<String> = pg_row.try_get(idx)?;
                            values.push(match v {
                                Some(x) => Value::String(x),
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Bytea => {
                            let v: Option<Vec<u8>> = pg_row.try_get(idx)?;
                            values.push(match v {
                                Some(bytes) => {
                                    Value::String(String::from_utf8_lossy(&bytes).into_owned())
                                }
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                            let v: Option<f64> = pg_row.try_get(idx)?;
                            values.push(match v {
                                Some(x) => Value::F64(x),
                                None => Value::OptionF64(None),
                            });
                        }
                    }
                } else {
                    let v: Option<String> = pg_row.try_get(idx)?;
                    values.push(match v {
                        Some(x) => Value::String(x),
                        None => Value::OptionString(None),
                    });
                }
            }

            // Normalize the match_value similar to Parquet's search_value_pure
            let mut mv = match_value.replace(&value_format.decimal_separator, ".");
            if mv.parse::<f64>().is_err() {
                mv = match_value.to_uppercase();
            }

            // Apply search_mode ordering and find position depending on types
            let sm = search_mode;
            let mut pos_opt: Option<usize> = None;

            let all_f64 = values.iter().all(|v| matches!(v, Value::F64(_)));
            let all_i32 = values.iter().all(|v| matches!(v, Value::I32(_)));
            let mixed_f64_i32 = values
                .iter()
                .all(|v| matches!(v, Value::F64(_) | Value::I32(_)));
            let contains_string = values.iter().any(|v| matches!(v, Value::String(_)));

            if all_f64 {
                let mut vals: Vec<f64> = values
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::F64(x) => Some(x),
                        _ => None,
                    })
                    .collect();
                match sm {
                    -1 => vals.reverse(),
                    2 => vals.sort_by(|a, b| a.partial_cmp(b).unwrap()),
                    -2 => vals.sort_by(|a, b| b.partial_cmp(a).unwrap()),
                    _ => {}
                }
                let col = &vals as &dyn Searchable;
                pos_opt = match match_mode {
                    -1 => find_smallest_position(col, &mv),
                    0 => find_exact_position(col, &mv),
                    1 => find_largest_position(col, &mv),
                    2 => None,
                    _ => None,
                };
            } else if all_i32 {
                let mut vals: Vec<i32> = values
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::I32(x) => Some(x),
                        _ => None,
                    })
                    .collect();
                match sm {
                    -1 => vals.reverse(),
                    2 => vals.sort(),
                    -2 => vals.sort_by(|a, b| b.cmp(a)),
                    _ => {}
                }
                let col = &vals as &dyn Searchable;
                pos_opt = match match_mode {
                    -1 => find_smallest_position(col, &mv),
                    0 => find_exact_position(col, &mv),
                    1 => find_largest_position(col, &mv),
                    2 => None,
                    _ => None,
                };
            } else if mixed_f64_i32 {
                let mut vals: Vec<f64> = values
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::F64(x) => Some(x),
                        Value::I32(x) => Some(x as f64),
                        _ => None,
                    })
                    .collect();
                match sm {
                    -1 => vals.reverse(),
                    2 => vals.sort_by(|a, b| a.partial_cmp(b).unwrap()),
                    -2 => vals.sort_by(|a, b| b.partial_cmp(a).unwrap()),
                    _ => {}
                }
                let col = &vals as &dyn Searchable;
                pos_opt = match match_mode {
                    -1 => find_smallest_position(col, &mv),
                    0 => find_exact_position(col, &mv),
                    1 => find_largest_position(col, &mv),
                    2 => None,
                    _ => None,
                };
            } else if contains_string {
                let mut vals: Vec<String> = values
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::F64(x) => Some(x.to_string()),
                        Value::I32(x) => Some(x.to_string()),
                        Value::String(s) => Some(s),
                        _ => None,
                    })
                    .collect();
                match sm {
                    -1 => vals.reverse(),
                    2 => vals.sort(),
                    -2 => vals.sort_by(|a, b| b.cmp(a)),
                    _ => {}
                }
                let col = &vals as &dyn Searchable;
                pos_opt = match match_mode {
                    -1 => find_smallest_position(col, &mv),
                    0 => find_exact_position(col, &mv),
                    1 => find_largest_position(col, &mv),
                    2 => None,
                    _ => None,
                };
            }

            if let Some(pos0) = pos_opt {
                return Ok(Value::I32((pos0 + 1) as i32));
            }

            return Err("MATCH: Position not found".into());
        }

        Err(format!(
            "XMATCH: Search value {match_value} does not exist for table {}",
            &self.table_name
        )
        .into())
    }

    /// Filters table rows based on a condition (FILTER function).
    ///
    /// Returns all rows that match the specified condition. The condition is
    /// converted to a SQL WHERE clause for efficient server-side filtering.
    ///
    /// # Arguments
    ///
    /// * `condition` - The filter condition to apply (converted to SQL WHERE clause)
    /// * `if_empty` - Value to return if no rows match (empty string returns `#CALC!` error)
    /// * `columns` - Comma-separated column names to include in the result
    /// * `table_functions` - Optional table functions for dynamic value resolution
    /// * `input` - Input context for table function evaluation
    /// * `value_format` - Format settings for value conversion and condition parsing
    ///
    /// # Returns
    ///
    /// A `Value::AreaValue` containing the filtered rows and selected columns.
    /// If no rows match and `if_empty` is not empty, returns `Value::String(if_empty)`.
    /// If no rows match and `if_empty` is empty, returns `Value::String("#CALC!")`.
    ///
    /// # Errors
    ///
    /// Returns an error if the condition cannot be converted to valid SQL.
    #[allow(clippy::too_many_arguments)]
    async fn filter(
        &self,
        condition: Condition,
        if_empty: &str,
        columns: &str,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Build a column_types map using the abstract ColumnType
        let column_types = self.get_abstract_column_types();

        // Quote identifiers for SQL safety
        let quoted_columns = quote_columns(columns);
        let quoted_table = self.get_quoted_table_name();

        let where_condition = condition.condition(&column_types, value_format)?;
        let sql_query = format!(
            "SELECT {quoted_columns} FROM {quoted_table} WHERE {where_condition}"
        );

        // Execute query and collect all rows
        let rows: Vec<PgRow> = sqlx::query(&sql_query).fetch_all(&self.db).await?;

        // Prepare list of selected columns to know how to map result types by name
        let selected_cols: Vec<String> = columns
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let mut area: Vec<Vec<Value>> = Vec::with_capacity(rows.len());

        for row in rows.into_iter() {
            let mut row_vals: Vec<Value> = Vec::with_capacity(selected_cols.len());
            for (idx, col_name) in selected_cols.iter().enumerate() {
                if let Some(col) = self.column_by_column_name(col_name) {
                    match col.sql_type {
                        PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                            let v: Option<i32> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::I32(x),
                                None => Value::OptionI32(None),
                            });
                        }
                        PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                            let v: Option<f64> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::F64(x),
                                None => Value::OptionF64(None),
                            });
                        }
                        PostgreSqlColumnType::Boolean => {
                            let v: Option<bool> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::Bool(x),
                                None => Value::OptionBool(None),
                            });
                        }
                        PostgreSqlColumnType::Text => {
                            let v: Option<String> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::String(x),
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Bytea => {
                            let v: Option<Vec<u8>> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(bytes) => {
                                    Value::String(String::from_utf8_lossy(&bytes).into_owned())
                                }
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                            let v: Option<f64> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::F64(x),
                                None => Value::OptionF64(None),
                            });
                        }
                    }
                } else {
                    // Fallback: try string
                    let v: Option<String> = row.try_get(idx)?;
                    row_vals.push(match v {
                        Some(x) => Value::String(x),
                        None => Value::OptionString(None),
                    });
                }
            }

            // Apply table functions concurrently if any cell contains a special marker "*F*<fn_name>"
            let processed_row: Vec<Value> = stream::iter(row_vals)
                .map(|v| async {
                    PostgreSQLTable::apply_table_functions_to_value(
                        v,
                        table_functions,
                        input,
                        value_format,
                    )
                    .await
                })
                .buffer_unordered(10)
                .collect()
                .await;

            area.push(processed_row);
        }

        if !area.is_empty() {
            Ok(Value::AreaValue(area))
        } else if if_empty.is_empty() {
            // TODO: CHECK IF WE SHOULD RAISE AN ERROR HERE INSTEAD???
            Ok(Value::String("#CALC!".to_string()))
        } else {
            Ok(Value::String(if_empty.to_string()))
        }
    }

    /// Selects all rows from the table for the specified columns.
    ///
    /// Returns all data from the table for the given columns without any filtering.
    /// This is equivalent to a `SELECT columns FROM table` SQL query.
    ///
    /// # Arguments
    ///
    /// * `columns` - Comma-separated column names to select (should be pre-quoted)
    /// * `table_functions` - Optional table functions for dynamic value resolution
    /// * `input` - Input context for table function evaluation
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// A `Value::AreaValue` containing all rows with the selected columns.
    ///
    /// # Errors
    ///
    /// Returns an error if no values are found in the table.
    async fn select_all(
        &self,
        columns: &str,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Build and execute a simple SELECT without WHERE, similar to ParquetTable::select_all
        // Note: columns are expected to be pre-quoted by caller
        let quoted_table = self.get_quoted_table_name();
        let sql_query = format!("SELECT {columns} FROM {quoted_table}");

        // Execute query and collect all rows
        let rows: Vec<PgRow> = sqlx::query(&sql_query).fetch_all(&self.db).await?;

        // Prepare list of selected columns to know how to map result types by name
        let selected_cols: Vec<String> = columns
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let mut area: Vec<Vec<Value>> = Vec::with_capacity(rows.len());

        for row in rows.into_iter() {
            let mut row_vals: Vec<Value> = Vec::with_capacity(selected_cols.len());
            for (idx, col_name) in selected_cols.iter().enumerate() {
                if let Some(col) = self.column_by_column_name(col_name) {
                    match col.sql_type {
                        PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                            let v: Option<i32> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::I32(x),
                                None => Value::OptionI32(None),
                            });
                        }
                        PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                            let v: Option<f64> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::F64(x),
                                None => Value::OptionF64(None),
                            });
                        }
                        PostgreSqlColumnType::Boolean => {
                            let v: Option<bool> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::Bool(x),
                                None => Value::OptionBool(None),
                            });
                        }
                        PostgreSqlColumnType::Text => {
                            let v: Option<String> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::String(x),
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Bytea => {
                            let v: Option<Vec<u8>> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(bytes) => {
                                    Value::String(String::from_utf8_lossy(&bytes).into_owned())
                                }
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                            let v: Option<f64> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(x) => Value::F64(x),
                                None => Value::OptionF64(None),
                            });
                        }
                    }
                } else {
                    // Fallback: try string
                    let v: Option<String> = row.try_get(idx)?;
                    row_vals.push(match v {
                        Some(x) => Value::String(x),
                        None => Value::OptionString(None),
                    });
                }
            }

            // Apply table functions concurrently if any cell contains a special marker "*F*<fn_name>"
            let processed_row: Vec<Value> = stream::iter(row_vals)
                .map(|v| async {
                    PostgreSQLTable::apply_table_functions_to_value(
                        v,
                        table_functions,
                        input,
                        value_format,
                    )
                    .await
                })
                .buffer_unordered(10)
                .collect()
                .await;

            area.push(processed_row);
        }

        if !area.is_empty() {
            Ok(Value::AreaValue(area))
        } else {
            Err("ALL: No values found".into())
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn filter_with_modifiers(
        &self,
        condition: Condition,
        if_empty: &str,
        columns: &str,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
        modifiers: &SqlModifiers,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if modifiers.is_empty() {
            return self.filter(condition, if_empty, columns, table_functions, input, value_format).await;
        }

        let column_types = self.get_abstract_column_types();
        let quoted_table = self.get_quoted_table_name();
        let where_condition = condition.condition(&column_types, value_format)?;

        let selected_cols: Vec<String> = columns
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let (select_prefix, order_clause, limit_clause) = build_modifier_clauses(modifiers, &selected_cols);

        // Aggregate query: returns a single scalar value
        if let Some(ref agg) = modifiers.aggregate {
            let select_expr = if matches!(agg, SqlAggregate::Count | SqlAggregate::CountA) {
                "COUNT(*)".to_string()
            } else {
                // Filter to numeric columns only, matching Excel behavior of ignoring text
                let numeric_cols: Vec<String> = selected_cols.iter()
                    .filter(|col| column_types.get(*col).map_or(false, |ct| ct.is_numeric()))
                    .map(|col| qident(col))
                    .collect();
                if numeric_cols.is_empty() {
                    format!("{}({})", agg.sql_function(), qident(&selected_cols[0]))
                } else {
                    agg.build_aggregate_select(&numeric_cols)
                }
            };
            let sql_query = format!(
                "SELECT {} FROM {} WHERE {}",
                select_expr, quoted_table, where_condition
            );
            let row: PgRow = sqlx::query(&sql_query).fetch_one(&self.db).await?;
            let result: Option<f64> = row.try_get(0)?;
            return Ok(Value::F64(result.unwrap_or(0.0)));
        }

        // Non-aggregate query with modifiers (DISTINCT, ORDER BY, LIMIT)
        let quoted_columns = quote_columns(columns);
        let sql_query = format!(
            "SELECT {select_prefix}{quoted_columns} FROM {quoted_table} WHERE {where_condition}{order_clause}{limit_clause}"
        );

        let rows: Vec<PgRow> = sqlx::query(&sql_query).fetch_all(&self.db).await?;

        let mut area: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
            let mut row_vals: Vec<Value> = Vec::with_capacity(selected_cols.len());
            for (idx, col_name) in selected_cols.iter().enumerate() {
                if let Some(col) = self.column_by_column_name(col_name) {
                    match col.sql_type {
                        PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                            let v: Option<i32> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::I32(x), None => Value::OptionI32(None) });
                        }
                        PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                            let v: Option<f64> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::F64(x), None => Value::OptionF64(None) });
                        }
                        PostgreSqlColumnType::Boolean => {
                            let v: Option<bool> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::Bool(x), None => Value::OptionBool(None) });
                        }
                        PostgreSqlColumnType::Text => {
                            let v: Option<String> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::String(x), None => Value::OptionString(None) });
                        }
                        PostgreSqlColumnType::Bytea => {
                            let v: Option<Vec<u8>> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(bytes) => Value::String(String::from_utf8_lossy(&bytes).into_owned()),
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                            let v: Option<f64> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::F64(x), None => Value::OptionF64(None) });
                        }
                    }
                } else {
                    let v: Option<String> = row.try_get(idx)?;
                    row_vals.push(match v { Some(x) => Value::String(x), None => Value::OptionString(None) });
                }
            }

            let processed_row: Vec<Value> = stream::iter(row_vals)
                .map(|v| async {
                    PostgreSQLTable::apply_table_functions_to_value(v, table_functions, input, value_format).await
                })
                .buffer_unordered(10)
                .collect()
                .await;

            area.push(processed_row);
        }

        if !area.is_empty() {
            Ok(Value::AreaValue(area))
        } else if if_empty.is_empty() {
            Ok(Value::String("#CALC!".to_string()))
        } else {
            Ok(Value::String(if_empty.to_string()))
        }
    }

    async fn select_all_with_modifiers(
        &self,
        columns: &str,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
        modifiers: &SqlModifiers,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if modifiers.is_empty() {
            return self.select_all(columns, table_functions, input, value_format).await;
        }

        let quoted_table = self.get_quoted_table_name();

        let selected_cols: Vec<String> = columns
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let (select_prefix, order_clause, limit_clause) = build_modifier_clauses(modifiers, &selected_cols);

        // Aggregate query
        if let Some(ref agg) = modifiers.aggregate {
            let select_expr = if matches!(agg, SqlAggregate::Count | SqlAggregate::CountA) {
                "COUNT(*)".to_string()
            } else {
                let column_types = self.get_abstract_column_types();
                let numeric_cols: Vec<String> = selected_cols.iter()
                    .filter(|col| column_types.get(*col).map_or(false, |ct| ct.is_numeric()))
                    .map(|col| qident(col))
                    .collect();
                if numeric_cols.is_empty() {
                    format!("{}({})", agg.sql_function(), qident(&selected_cols[0]))
                } else {
                    agg.build_aggregate_select(&numeric_cols)
                }
            };
            let sql_query = format!(
                "SELECT {} FROM {}",
                select_expr, quoted_table
            );
            let row: PgRow = sqlx::query(&sql_query).fetch_one(&self.db).await?;
            let result: Option<f64> = row.try_get(0)?;
            return Ok(Value::F64(result.unwrap_or(0.0)));
        }

        let quoted_columns = quote_columns(columns);
        let sql_query = format!(
            "SELECT {select_prefix}{quoted_columns} FROM {quoted_table}{order_clause}{limit_clause}"
        );

        let rows: Vec<PgRow> = sqlx::query(&sql_query).fetch_all(&self.db).await?;

        let mut area: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
            let mut row_vals: Vec<Value> = Vec::with_capacity(selected_cols.len());
            for (idx, col_name) in selected_cols.iter().enumerate() {
                if let Some(col) = self.column_by_column_name(col_name) {
                    match col.sql_type {
                        PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                            let v: Option<i32> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::I32(x), None => Value::OptionI32(None) });
                        }
                        PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                            let v: Option<f64> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::F64(x), None => Value::OptionF64(None) });
                        }
                        PostgreSqlColumnType::Boolean => {
                            let v: Option<bool> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::Bool(x), None => Value::OptionBool(None) });
                        }
                        PostgreSqlColumnType::Text => {
                            let v: Option<String> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::String(x), None => Value::OptionString(None) });
                        }
                        PostgreSqlColumnType::Bytea => {
                            let v: Option<Vec<u8>> = row.try_get(idx)?;
                            row_vals.push(match v {
                                Some(bytes) => Value::String(String::from_utf8_lossy(&bytes).into_owned()),
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                            let v: Option<f64> = row.try_get(idx)?;
                            row_vals.push(match v { Some(x) => Value::F64(x), None => Value::OptionF64(None) });
                        }
                    }
                } else {
                    let v: Option<String> = row.try_get(idx)?;
                    row_vals.push(match v { Some(x) => Value::String(x), None => Value::OptionString(None) });
                }
            }

            let processed_row: Vec<Value> = stream::iter(row_vals)
                .map(|v| async {
                    PostgreSQLTable::apply_table_functions_to_value(v, table_functions, input, value_format).await
                })
                .buffer_unordered(10)
                .collect()
                .await;

            area.push(processed_row);
        }

        if !area.is_empty() {
            Ok(Value::AreaValue(area))
        } else {
            Err("ALL: No values found".into())
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn x_lookup_with_modifiers(
        &self,
        lookup_value: &str,
        search_column: &str,
        columns: &str,
        row: u32,
        if_not_found: Option<String>,
        match_mode: Option<i32>,
        search_mode: Option<i32>,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
        modifiers: &SqlModifiers,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if modifiers.is_empty() {
            return self.x_lookup(lookup_value, search_column, columns, row, if_not_found, match_mode, search_mode, table_functions, input, value_format).await;
        }

        // For x_lookup with modifiers, first get the base result then apply modifiers via SQL
        // x_lookup already returns filtered results; modifiers add ORDER BY / DISTINCT / aggregates
        // For now, delegate to the base implementation for the lookup, then apply modifiers
        // A full implementation would integrate modifiers into the x_lookup SQL query itself
        self.x_lookup(lookup_value, search_column, columns, row, if_not_found, match_mode, search_mode, table_functions, input, value_format).await
    }

    /// Adds a new row to the table with an auto-generated UUID as the ID.
    ///
    /// Inserts a new row where the first column (ID column) is automatically
    /// populated with a new UUID, and the remaining columns are filled with
    /// the provided values.
    ///
    /// # Arguments
    ///
    /// * `values` - Values for all columns except the ID column (must match column count - 1)
    /// * `_table_functions` - Unused (table functions don't apply to inserts)
    /// * `_input` - Unused
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// The generated UUID for the new row as `Value::String`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The number of values doesn't match the number of non-ID columns
    /// * The database insert operation fails
    async fn add_row(
        &self,
        values: Vec<Value>,
        _table_functions: &TableFunctions, // Table functions here make no sense
        _input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if self.postgresql_columns.len() != (values.len() + 1) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "ADD_ROW: Number of columns does not match number of columns in table {}",
                    self.table_name
                ),
            )));
        }

        let id = Uuid::new_v4().to_string();
        let sql_arguments = self.get_add_row_arguments(&values, value_format, &id)?;

        // Use pre-built query template for better prepared statement caching
        sqlx::query_with(&self.query_templates.insert_row, sql_arguments)
            .execute(&self.db)
            .await?;

        Ok(Value::String(id))
    }

    /// Updates an existing row identified by its ID.
    ///
    /// Updates all non-ID columns of the row with the specified ID. The ID column
    /// itself is not modified.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID (first column value) of the row to update
    /// * `values` - New values for all columns except the ID column (must match column count - 1)
    /// * `_table_functions` - Unused (table functions don't apply to updates)
    /// * `_input` - Unused
    /// * `value_format` - Format settings for value conversion
    ///
    /// # Returns
    ///
    /// The ID of the updated row as `Value::String`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The number of values doesn't match the number of non-ID columns
    /// * No row exists with the specified ID
    /// * The database update operation fails
    async fn update_row(
        &self,
        id: &str,
        values: Vec<Value>,
        _table_functions: &TableFunctions,
        _input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if self.postgresql_columns.len() != (values.len() + 1) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "UPDATE_ROW: Number of values ({}) does not match number of columns in table {} minus ID",
                    values.len(),
                    self.table_name
                ),
            )));
        }

        let mut args = PgArguments::default();
        args.reserve(values.len() + 1, 0);

        // Bind all non-ID values in order
        self.bind_non_id_values(&mut args, &values, value_format)?;

        // Bind the ID for WHERE
        let _ = args.add(&id);

        // Use pre-built query template for better prepared statement caching
        let result = sqlx::query_with(&self.query_templates.update_row, args)
            .execute(&self.db)
            .await?;

        if result.rows_affected() == 0 {
            let id_col_name = &self.postgresql_columns[0].column_name;
            debug!("UPDATE_ROW: No row found with {} = {}", id_col_name, id);
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "UPDATE_ROW: No row found with the specified ID",
            )));
        }

        Ok(Value::String(id.to_string()))
    }

    /// Deletes a row from the table by its ID.
    ///
    /// Removes the row with the specified ID from the table. This operation
    /// is permanent and cannot be undone.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID (first column value) of the row to delete
    /// * `_table_functions` - Unused (table functions don't apply to deletes)
    /// * `_input` - Unused
    /// * `_value_format` - Unused
    ///
    /// # Returns
    ///
    /// The ID of the deleted row as `Value::String`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * No row exists with the specified ID
    /// * The database delete operation fails
    async fn delete_row(
        &self,
        id: &str,
        _table_functions: &TableFunctions,
        _input: &Input,
        _value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let mut args = PgArguments::default();
        args.reserve(1, 0);
        let _ = args.add(id);

        // Use pre-built query template for better prepared statement caching
        let result = sqlx::query_with(&self.query_templates.delete_row, args)
            .execute(&self.db)
            .await?;

        if result.rows_affected() == 0 {
            let id_col_name = &self.postgresql_columns[0].column_name;
            debug!("DELETE_ROW: No row found with {} = {}", id_col_name, id);
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "DELETE_ROW: No row found with the specified ID",
            )));
        }

        Ok(Value::String(id.to_string()))
    }

    /// Reads a single row from the table by its ID.
    ///
    /// Fetches all columns of the row with the specified ID. This is useful
    /// for retrieving a complete record after knowing its identifier.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID (first column value) of the row to read
    /// * `_table_functions` - Unused (table functions don't apply to single row reads)
    /// * `_input` - Unused
    /// * `_value_format` - Unused
    ///
    /// # Returns
    ///
    /// A `Value::VecValue` containing all column values for the row, in column order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * No row exists with the specified ID
    /// * The database query fails
    async fn read_row(
        &self,
        id: &str,
        _table_functions: &TableFunctions,
        _input: &Input,
        _value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Use pre-built query template for better prepared statement caching
        let mut args = PgArguments::default();
        args.reserve(1, 0);
        let _ = args.add(id);
        let row = sqlx::query_with(&self.query_templates.read_row, args)
            .fetch_optional(&self.db)
            .await?;

        match row {
            Some(row) => {
                let mut values = Vec::new();

                // Read all columns normally
                for (i, col) in self.postgresql_columns.iter().enumerate() {
                    match col.sql_type {
                        PostgreSqlColumnType::Integer | PostgreSqlColumnType::BigInt => {
                            let ov: Option<i32> = row.try_get(i)?;
                            values.push(match ov {
                                Some(v) => Value::I32(v),
                                None => Value::OptionI32(None),
                            });
                        }
                        PostgreSqlColumnType::Real | PostgreSqlColumnType::DoublePrecision => {
                            let ov: Option<f64> = row.try_get(i)?;
                            values.push(match ov {
                                Some(v) => Value::F64(v),
                                None => Value::OptionF64(None),
                            });
                        }
                        PostgreSqlColumnType::Boolean => {
                            let ov: Option<bool> = row.try_get(i)?;
                            values.push(match ov {
                                Some(v) => Value::Bool(v),
                                None => Value::OptionBool(None),
                            });
                        }
                        PostgreSqlColumnType::Text => {
                            let ov: Option<String> = row.try_get(i)?;
                            values.push(match ov {
                                Some(v) => Value::String(v),
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Bytea => {
                            let ov: Option<Vec<u8>> = row.try_get(i)?;
                            values.push(match ov {
                                Some(bytes) => {
                                    let s = String::from_utf8_lossy(&bytes).into_owned();
                                    Value::String(s)
                                }
                                None => Value::OptionString(None),
                            });
                        }
                        PostgreSqlColumnType::Date | PostgreSqlColumnType::Timestamp => {
                            let ov: Option<f64> = row.try_get(i)?;
                            values.push(match ov {
                                Some(v) => Value::F64(v),
                                None => Value::OptionF64(None),
                            });
                        }
                    }
                }

                Ok(Value::VecValue(values))
            }
            None => {
                let id_col_name = &self.postgresql_columns[0].column_name;
                debug!("READ_ROW: No row found with {} = {}", id_col_name, id);
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "READ_ROW: No row found with the specified ID",
                )))
            }
        }
    }
}

// Helper function to generate placeholders
fn generate_placeholders(length: usize) -> String {
    (1..=length)
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn create_database(
    postgresql_url: &str,
    db_name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let admin_url = format!("{postgresql_url}postgres");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(60))
        .max_lifetime(Duration::from_secs(300))
        .connect(&admin_url)
        .await?;

    // Attempt to create the database directly and handle "already exists" error gracefully.
    // This avoids TOCTOU race conditions from check-then-create patterns.
    let create = format!(r#"CREATE DATABASE "{}""#, db_name.replace('"', "\"\""));
    match pool.execute(create.as_str()).await {
        Ok(_) => Ok(()),
        Err(e) => {
            // Check if error is "duplicate_database" (PostgreSQL error code 42P04)
            if let Some(db_err) = e.as_database_error() {
                if db_err.code().is_some_and(|c| c.as_ref() == "42P04") {
                    // Database already exists, this is fine
                    return Ok(());
                }
            }
            Err(e.into())
        }
    }
}
