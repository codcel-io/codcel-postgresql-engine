// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0 OR Codcel-Commercial
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT, LICENSE-APACHE, and LICENSE-CODCEL-COMMERCIAL in the project root.

use anyhow::{bail, Context, Result};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;
use sqlx::PgPool;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use uuid::Uuid;

// ──────────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────────

/// Represents the supported PostgreSQL column data types.
///
/// This enum maps Arrow/Parquet data types to their corresponding PostgreSQL types.
/// It is used when creating tables from Parquet files and for type-aware query operations.
///
/// # Variants
///
/// * `Integer` - 32-bit signed integer (`INTEGER` in PostgreSQL)
/// * `BigInt` - 64-bit signed integer (`BIGINT` in PostgreSQL)
/// * `Real` - 32-bit floating point (`REAL` in PostgreSQL)
/// * `DoublePrecision` - 64-bit floating point (`DOUBLE PRECISION` in PostgreSQL)
/// * `Boolean` - Boolean true/false (`BOOLEAN` in PostgreSQL)
/// * `Text` - Variable-length character string (`TEXT` in PostgreSQL)
/// * `Bytea` - Binary data (`BYTEA` in PostgreSQL)
/// * `Date` - Calendar date (`DATE` in PostgreSQL)
/// * `Timestamp` - Date and time (`TIMESTAMP` in PostgreSQL)
#[derive(Debug, Clone, Copy)]
pub enum PostgreSqlColumnType {
    /// 32-bit signed integer (`INTEGER` in PostgreSQL).
    Integer,
    /// 64-bit signed integer (`BIGINT` in PostgreSQL).
    BigInt,
    /// 32-bit floating point (`REAL` in PostgreSQL).
    Real,
    /// 64-bit floating point (`DOUBLE PRECISION` in PostgreSQL).
    DoublePrecision,
    /// Boolean true/false (`BOOLEAN` in PostgreSQL).
    Boolean,
    /// Variable-length character string (`TEXT` in PostgreSQL).
    Text,
    /// Binary data (`BYTEA` in PostgreSQL).
    Bytea,
    /// Calendar date (`DATE` in PostgreSQL).
    Date,
    /// Date and time (`TIMESTAMP` in PostgreSQL).
    Timestamp,
}

impl PostgreSqlColumnType {
    #[inline]
    fn as_str(self) -> &'static str {
        match self {
            Self::Integer => "INTEGER",
            Self::BigInt => "BIGINT",
            Self::Real => "REAL",
            Self::DoublePrecision => "DOUBLE PRECISION",
            Self::Boolean => "BOOLEAN",
            Self::Text => "TEXT",
            Self::Bytea => "BYTEA",
            Self::Date => "DATE",
            Self::Timestamp => "TIMESTAMP",
        }
    }
}

impl Display for PostgreSqlColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PostgreSqlColumnType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_uppercase().as_str() {
            "INTEGER" => Ok(Self::Integer),
            "BIGINT" => Ok(Self::BigInt),
            "REAL" => Ok(Self::Real),
            "DOUBLE PRECISION" => Ok(Self::DoublePrecision),
            "BOOLEAN" => Ok(Self::Boolean),
            "TEXT" => Ok(Self::Text),
            "BYTEA" => Ok(Self::Bytea),
            "DATE" => Ok(Self::Date),
            "TIMESTAMP" => Ok(Self::Timestamp),
            other => Err(format!("Unknown column type: {other}")),
        }
    }
}

/// Metadata for a PostgreSQL table column.
///
/// This struct holds the essential information about a column in a PostgreSQL table,
/// including its name, data type, and nullability constraint. It is used when creating
/// tables from Parquet files and for type-aware query operations.
///
/// # Fields
///
/// * `column_name` - The name of the column as it appears in the PostgreSQL table
/// * `sql_type` - The PostgreSQL data type for this column
/// * `nullable` - Whether the column allows NULL values
#[derive(Debug, Clone)]
pub struct PostgresSqlColumn {
    /// The name of the column as it appears in the PostgreSQL table.
    pub column_name: String,
    /// The PostgreSQL data type for this column.
    pub sql_type: PostgreSqlColumnType,
    /// Whether the column allows NULL values.
    pub nullable: bool,
}

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Ensures a PostgreSQL "t_table" exists based on a Parquet file schema.
///
/// This function creates a PostgreSQL table with a special structure where the first
/// column is renamed to `"c0"` (TEXT type) and values are auto-generated UUIDs when
/// inserting rows. This is useful for tables that need a synthetic primary key.
///
/// The table is created with `IF NOT EXISTS` semantics, so it's safe to call
/// multiple times. If `insert_rows` is true, data from the Parquet file will be
/// inserted into the table (only if the table was newly created).
///
/// # Arguments
///
/// * `parquet_file_path` - Path to the Parquet file (expects `_xyz0.parquet` naming convention)
/// * `_parquet_crud_file_path` - Reserved for signature compatibility (currently unused)
/// * `table_name` - Name for the PostgreSQL table to create
/// * `column_names` - Names for columns (excluding the first `c0` column which is auto-named)
/// * `db` - PostgreSQL connection pool
/// * `insert_rows` - If true, insert data from Parquet file into the table
/// * `unique_columns` - Column names that should have UNIQUE constraints
/// * `optional_columns` - Column names that should allow NULL values
///
/// # Returns
///
/// A vector of [`PostgresSqlColumn`] describing the created table schema.
///
/// # Errors
///
/// Returns an error if:
/// * The Parquet file cannot be read or doesn't exist
/// * The number of column names doesn't match the Parquet schema (minus one for c0)
/// * Database connection or query execution fails
/// * Table creation fails due to SQL errors
#[allow(clippy::too_many_arguments)]
pub async fn ensure_t_table_from_parquet(
    parquet_file_path: &str,
    _parquet_crud_file_path: &str, // kept for signature compatibility
    table_name: &str,
    column_names: Vec<String>,
    db: &PgPool,
    insert_rows: bool,
    unique_columns: &[String],
    optional_columns: &[String],
) -> Result<Vec<PostgresSqlColumn>> {
    ensure_table_from_parquet_core(
        parquet_file_path,
        table_name,
        db,
        insert_rows,
        Some(column_names),
        /*t_mode=*/ true,
        unique_columns,
        optional_columns
    )
    .await
}

/// Ensures a PostgreSQL table exists based on a Parquet file schema.
///
/// This function creates a PostgreSQL table that mirrors the schema of the provided
/// Parquet file. Column names and types are derived directly from the Parquet schema.
///
/// The table is created with `IF NOT EXISTS` semantics, so it's safe to call
/// multiple times. If `insert` is true, data from the Parquet file will be
/// inserted into the table (only if the table was newly created).
///
/// # Arguments
///
/// * `parquet_file_path` - Path to the Parquet file (expects `_xyz0.parquet` naming convention)
/// * `table_name` - Name for the PostgreSQL table to create
/// * `db` - PostgreSQL connection pool
/// * `insert` - If true, insert data from Parquet file into the table
/// * `unique_columns` - Column names that should have UNIQUE constraints
/// * `optional_columns` - Column names that should allow NULL values
///
/// # Returns
///
/// A vector of [`PostgresSqlColumn`] describing the created table schema.
///
/// # Errors
///
/// Returns an error if:
/// * The Parquet file cannot be read or doesn't exist
/// * Database connection or query execution fails
/// * Table creation fails due to SQL errors
pub async fn ensure_table_from_parquet(
    parquet_file_path: &str,
    table_name: &str,
    db: &PgPool,
    insert: bool,
    unique_columns: &[String],
    optional_columns: &[String],
) -> Result<Vec<PostgresSqlColumn>> {
    ensure_table_from_parquet_core(
        parquet_file_path,
        table_name,
        db,
        insert,
        None,
        /*t_mode=*/ false,
        unique_columns,
        optional_columns,
    )
    .await
}

// ──────────────────────────────────────────────────────────────────────────────
// Core
// ──────────────────────────────────────────────────────────────────────────────
#[allow(clippy::too_many_arguments)]
async fn ensure_table_from_parquet_core(
    parquet_file_path: &str,
    table_name: &str,
    db: &PgPool,
    insert_rows: bool,
    // When Some, we expect one extra parquet column (first ignored) and rename using provided names.
    column_names: Option<Vec<String>>,
    t_mode: bool,
    unique_columns: &[String],
    optional_columns: &[String],
) -> Result<Vec<PostgresSqlColumn>> {
    let first_part = parquet_part_path(parquet_file_path, 0);

    // 1) Does table already exist?
    let regclass: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(qualify_public_default(table_name))
        .fetch_one(db)
        .await
        .context("checking table existence")?;

    let table_missing = regclass.is_none();

    // 2) Read schema from first parquet part
    let schema = read_parquet_schema(&first_part)
        .await
        .with_context(|| format!("reading parquet file {first_part}"))?;

    // 3) Compute Postgres columns (plain or t_mode)
    let pg_cols = if let Some(names) = column_names {
        get_t_postgresql_columns(schema.as_ref(), names, optional_columns)?
    } else {
        get_postgresql_columns(schema.as_ref(), optional_columns)?
    };

    // 4) Create table if missing (uses IF NOT EXISTS to handle race conditions gracefully)
    if table_missing {
        let create_sql = build_create_table_sql(table_name, &pg_cols, unique_columns)?;
        sqlx::query(&create_sql)
            .execute(db)
            .await
            .context("creating table from parquet schema")?;

        // 5) Optional inserts across parts: _xyz0, _xyz1, ...
        if insert_rows {
            let parts = collect_existing_parts(parquet_file_path);
            if parts.is_empty() {
                println!("⚠️ No parquet parts found for {}", parquet_file_path);
            }
            for part in parts {
                let df = register_parquet_and_table(&part, "tbl_ins")
                    .await
                    .with_context(|| format!("reading parquet for insert: {part}"))?;
                if t_mode {
                    let col_names = pg_cols.iter().map(|c| c.column_name.clone()).collect::<Vec<_>>();
                    insert_t_all_rows(df, table_name, db, &col_names).await?;
                } else {
                    insert_all_rows(df, table_name, db).await?;
                }
            }
        }
    }

    Ok(pg_cols)
}

// ──────────────────────────────────────────────────────────────────────────────
// Schema → Columns helpers
// ──────────────────────────────────────────────────────────────────────────────

fn get_postgresql_columns(schema: &Schema, optional_columns: &[String]) -> Result<Vec<PostgresSqlColumn>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let is_optional = optional_columns.contains(field.name());
            Ok(PostgresSqlColumn {
                column_name: field.name().to_string(),
                sql_type: pg_type_from_arrow(field.data_type()).parse().map_err(|e: String| {
                    anyhow::anyhow!("invalid mapped pg type for {}: {e}", field.name())
                })?,
                nullable: is_optional,
            })
        })
        .collect()
}

fn get_t_postgresql_columns(schema: &Schema, column_names: Vec<String>, optional_columns: &[String]) -> Result<Vec<PostgresSqlColumn>> {
    let base = get_postgresql_columns(schema, optional_columns)?;
    if (column_names.len() + 1) != base.len() {
        bail!(
            "Expected {} columns, got {} (t_mode expects parquet to have 1 extra leading column)",
            column_names.len() + 1,
            base.len()
        );
    }

    let mut out = Vec::with_capacity(base.len());
    for (idx, col) in base.iter().enumerate() {
        if idx == 0 {
            out.push(PostgresSqlColumn {
                column_name: "c0".into(),
                sql_type: PostgreSqlColumnType::Text,
                nullable: false,
            });
        } else {
            let name = column_names[idx - 1].clone();
            let is_optional = optional_columns.contains(&name);
            out.push(PostgresSqlColumn {
                column_name: name,
                sql_type: col.sql_type,
                nullable: is_optional,
            });
        }
    }
    Ok(out)
}

/// Map Arrow types → Postgres column types (as &str)
fn pg_type_from_arrow(dtype: &DataType) -> &'static str {
    match dtype {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "INTEGER",
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "BIGINT",
        DataType::Float16 | DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Boolean => "BOOLEAN",
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Binary | DataType::LargeBinary => "BYTEA",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        _ => "TEXT",
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// — SQL helpers
// ──────────────────────────────────────────────────────────────────────────────

fn build_create_table_sql(table_name: &str, columns: &[PostgresSqlColumn], unique_columns: &[String]) -> Result<String> {
    if columns.is_empty() {
        bail!("Parquet file has no columns");
    }

    let qualified = qualify_public_default(table_name);
    let mut parts = qualified.splitn(2, '.');
    let schema = parts.next().unwrap_or("public");
    let table = parts.next().unwrap_or(qualified.as_str());

    let cols = columns
        .iter()
        .map(|c| {
            let is_unique = unique_columns.contains(&c.column_name);
            let unique_suffix = if is_unique { " UNIQUE" } else { "" };
            let nullability = if c.nullable { "" } else { " NOT NULL" };
            format!("{} {}{}{}", qident(&c.column_name), c.sql_type, nullability, unique_suffix)
        })
        .collect::<Vec<_>>()
        .join(",\n  ");

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\n  {}\n);",
        qident(schema),
        qident(table),
        cols
    ))
}

fn qident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn qualify_public_default(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("public.{name}")
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Insert helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Maximum rows per INSERT statement to avoid building massive SQL statements
const INSERT_BATCH_SIZE: usize = 2000;

async fn insert_all_rows(df: DataFrame, table_name: &str, db: &PgPool) -> Result<()> {
    let batches = df.collect().await?;

    // Use a single transaction for all batches to reduce overhead and ensure atomicity
    let mut tx = db.begin().await.context("starting insert transaction")?;

    for batch in batches {
        let nrows = batch.num_rows();
        if nrows == 0 {
            continue;
        }

        let schema = batch.schema();
        let col_names: Vec<String> = schema.fields().iter().map(|f| qident(f.name())).collect();
        let insert_prefix = format!(
            "INSERT INTO {} ({}) VALUES ",
            qualify_public_default(table_name),
            col_names.join(", ")
        );

        // Split into smaller chunks to avoid building massive SQL statements
        for chunk_start in (0..nrows).step_by(INSERT_BATCH_SIZE) {
            let chunk_end = (chunk_start + INSERT_BATCH_SIZE).min(nrows);

            let mut qb = sqlx::QueryBuilder::new(&insert_prefix);

            for r in chunk_start..chunk_end {
                if r > chunk_start {
                    qb.push(", ");
                }
                qb.push("(");
                for c in 0..batch.num_columns() {
                    if c > 0 {
                        qb.push(", ");
                    }
                    bind_arrow_value(&mut qb, batch.column(c).as_ref(), r)?;
                }
                qb.push(")");
            }

            qb.build().execute(&mut *tx).await?;
        }
    }

    tx.commit().await.context("committing insert transaction")?;

    Ok(())
}

async fn insert_t_all_rows(
    df: DataFrame,
    table_name: &str,
    db: &PgPool,
    col_names: &[String],
) -> Result<()> {
    let batches = df.collect().await?;

    // Use a single transaction for all batches to reduce overhead and ensure atomicity
    let mut tx = db.begin().await.context("starting insert transaction")?;

    let insert_prefix = format!(
        "INSERT INTO {} ({}) VALUES ",
        qualify_public_default(table_name),
        col_names.join(", ")
    );

    for batch in batches {
        let nrows = batch.num_rows();
        if nrows == 0 {
            continue;
        }

        // Pre-generate UUIDs for first column
        let ids: Vec<String> = (0..nrows).map(|_| Uuid::new_v4().to_string()).collect();
        debug_assert_eq!(ids.len(), nrows);

        // Split into smaller chunks to avoid building massive SQL statements
        for chunk_start in (0..nrows).step_by(INSERT_BATCH_SIZE) {
            let chunk_end = (chunk_start + INSERT_BATCH_SIZE).min(nrows);

            let mut qb = sqlx::QueryBuilder::new(&insert_prefix);

            for (i, id) in ids[chunk_start..chunk_end].iter().enumerate() {
                if i > 0 {
                    qb.push(", ");
                }
                qb.push("(");

                // First logical column: generated UUID
                qb.push_bind(id);

                // Remaining columns map 1:1 to parquet columns [1..]
                let row_idx = chunk_start + i;
                for c in 1..batch.num_columns() {
                    qb.push(", ");
                    bind_arrow_value(&mut qb, batch.column(c).as_ref(), row_idx)?;
                }

                qb.push(")");
            }

            qb.build().execute(&mut *tx).await?;
        }
    }

    tx.commit().await.context("committing insert transaction")?;

    Ok(())
}


// Bind Arrow value at `idx` to the SQL query builder
fn bind_arrow_value<'a>(
    qb: &mut sqlx::QueryBuilder<'a, sqlx::Postgres>,
    array: &'a dyn Array,
    idx: usize,
) -> anyhow::Result<()> {
    use DataType::*;

    if array.is_null(idx) {
        qb.push_bind::<Option<String>>(None);
        return Ok(());
    }

    match array.data_type() {
        Int64 => {
            let v = array.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Int64Array"))?
                .value(idx);
            qb.push_bind(v);
        }
        Int32 => {
            let v = array.as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Int32Array"))?
                .value(idx);
            qb.push_bind(v);
        }
        Float64 => {
            let v = array.as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Float64Array"))?
                .value(idx);
            qb.push_bind(v);
        }
        Float32 => {
            let v = array.as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Float32Array"))?
                .value(idx) as f64;
            qb.push_bind(v);
        }
        Boolean => {
            let v = array.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected BooleanArray"))?
                .value(idx);
            qb.push_bind(v);
        }
        Utf8 => {
            let v = array.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected StringArray"))?
                .value(idx);
            qb.push_bind(v);
        }
        LargeUtf8 => {
            let v = array.as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected LargeStringArray"))?
                .value(idx);
            qb.push_bind(v);
        }
        Date32 => {
            let days = array.as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Date32Array"))?
                .value(idx);
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| anyhow::anyhow!("Invalid epoch date"))?
                + chrono::Duration::days(days as i64);
            qb.push_bind(date.format("%Y-%m-%d").to_string());
        }
        Date64 => {
            let ms = array.as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Date64Array"))?
                .value(idx);
            let date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp millis: {}", ms))?
                .date_naive();
            qb.push_bind(date.format("%Y-%m-%d").to_string());
        }
        Timestamp(unit, _) => {
            let s = match unit {
                TimeUnit::Second => {
                    let v = array.as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Expected TimestampSecondArray"))?
                        .value(idx);
                    chrono::DateTime::<chrono::Utc>::from_timestamp(v, 0)
                        .ok_or_else(|| anyhow::anyhow!("Invalid timestamp seconds: {}", v))?
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string()
                }
                TimeUnit::Millisecond => {
                    let v = array.as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Expected TimestampMillisecondArray"))?
                        .value(idx);
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(v)
                        .ok_or_else(|| anyhow::anyhow!("Invalid timestamp millis: {}", v))?
                        .format("%Y-%m-%d %H:%M:%S%.3f")
                        .to_string()
                }
                TimeUnit::Microsecond => {
                    let v = array.as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Expected TimestampMicrosecondArray"))?
                        .value(idx);
                    let secs = v / 1_000_000;
                    let sub_us = (v % 1_000_000) as u32;
                    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, sub_us * 1_000)
                        .ok_or_else(|| anyhow::anyhow!("Invalid timestamp micros: {}", v))?
                        .format("%Y-%m-%d %H:%M:%S%.6f")
                        .to_string()
                }
                TimeUnit::Nanosecond => {
                    let v = array.as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Expected TimestampNanosecondArray"))?
                        .value(idx);
                    let secs = v / 1_000_000_000;
                    let nanos = (v % 1_000_000_000) as u32;
                    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                        .ok_or_else(|| anyhow::anyhow!("Invalid timestamp nanos: {}", v))?
                        .format("%Y-%m-%d %H:%M:%S%.9f")
                        .to_string()
                }
            };
            qb.push_bind(s);
        }
        _ => {
            qb.push_bind(array_value_to_string(array, idx)?);
        }
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// Parquet helpers
// ──────────────────────────────────────────────────────────────────────────────

fn parquet_part_path(base: &str, idx: usize) -> String {
    base.replace(".parquet", &format!("_xyz{idx}.parquet"))
}

fn collect_existing_parts(base: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 0usize.. {
        let p = parquet_part_path(base, i);
        if std::path::Path::new(&p).exists() {
            out.push(p);
        } else {
            break;
        }
    }
    out
}

async fn read_parquet_schema(path: &str) -> Result<SchemaRef> {
    let ctx = SessionContext::new();
    ctx.register_parquet("tbl", path, Default::default())
        .await
        .with_context(|| format!("register_parquet({path})"))?;
    let df = ctx.table("tbl").await?;
    // df.schema(): &DFSchema
    // .as_arrow(): &SchemaRef (i.e., &Arc<Schema>)
    Ok(SchemaRef::from(df.schema().as_arrow().clone()))
}

async fn register_parquet_and_table(path: &str, table_name: &str) -> Result<DataFrame> {
    let ctx = SessionContext::new();
    ctx.register_parquet(table_name, path, Default::default())
        .await
        .with_context(|| format!("register_parquet({path})"))?;
    Ok(ctx.table(table_name).await?)
}
