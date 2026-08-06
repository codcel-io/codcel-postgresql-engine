// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT and LICENSE-APACHE in the project root.

use anyhow::{bail, Context, Result};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;
use sqlx::{AssertSqlSafe, PgPool};
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
        sqlx::query(AssertSqlSafe(create_sql))
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
fn bind_arrow_value(
    qb: &mut sqlx::QueryBuilder<sqlx::Postgres>,
    array: &dyn Array,
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

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Postgres, QueryBuilder};
    use std::sync::Arc;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn col(name: &str, sql_type: PostgreSqlColumnType, nullable: bool) -> PostgresSqlColumn {
        PostgresSqlColumn {
            column_name: name.to_string(),
            sql_type,
            nullable,
        }
    }

    /// `PostgreSqlColumnType` derives no `PartialEq`, so compare via the SQL spelling.
    fn ty(c: &PostgresSqlColumn) -> &'static str {
        c.sql_type.as_str()
    }

    fn schema_of(fields: &[(&str, DataType, bool)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(name, dt, nullable)| Field::new(*name, dt.clone(), *nullable))
                .collect::<Vec<_>>(),
        )
    }

    /// Bind one Arrow value into a fresh builder and return the SQL fragment emitted.
    fn bind_one(array: &dyn Array, idx: usize) -> Result<String> {
        let mut qb = QueryBuilder::<Postgres>::new("");
        bind_arrow_value(&mut qb, array, idx)?;
        Ok(qb.into_string())
    }

    // ── PostgreSqlColumnType: as_str / Display / FromStr ──────────────────────

    const ALL_TYPES: [PostgreSqlColumnType; 9] = [
        PostgreSqlColumnType::Integer,
        PostgreSqlColumnType::BigInt,
        PostgreSqlColumnType::Real,
        PostgreSqlColumnType::DoublePrecision,
        PostgreSqlColumnType::Boolean,
        PostgreSqlColumnType::Text,
        PostgreSqlColumnType::Bytea,
        PostgreSqlColumnType::Date,
        PostgreSqlColumnType::Timestamp,
    ];

    #[test]
    fn column_type_as_str_is_exhaustive() {
        use PostgreSqlColumnType as P;
        assert_eq!(P::Integer.as_str(), "INTEGER");
        assert_eq!(P::BigInt.as_str(), "BIGINT");
        assert_eq!(P::Real.as_str(), "REAL");
        assert_eq!(P::DoublePrecision.as_str(), "DOUBLE PRECISION");
        assert_eq!(P::Boolean.as_str(), "BOOLEAN");
        assert_eq!(P::Text.as_str(), "TEXT");
        assert_eq!(P::Bytea.as_str(), "BYTEA");
        assert_eq!(P::Date.as_str(), "DATE");
        assert_eq!(P::Timestamp.as_str(), "TIMESTAMP");
    }

    #[test]
    fn column_type_display_matches_as_str() {
        for t in ALL_TYPES {
            assert_eq!(t.to_string(), t.as_str());
        }
    }

    #[test]
    fn column_type_round_trips_through_from_str() {
        // This is the exact round-trip `get_postgresql_columns` performs:
        // `pg_type_from_arrow` returns a &str which is immediately `.parse()`d.
        for t in ALL_TYPES {
            let parsed: PostgreSqlColumnType = t.as_str().parse().unwrap();
            assert_eq!(parsed.as_str(), t.as_str());
        }
    }

    #[test]
    fn column_type_from_str_is_case_insensitive_and_end_trimmed() {
        let parsed: PostgreSqlColumnType = "  double precision  ".parse().unwrap();
        assert_eq!(parsed.as_str(), "DOUBLE PRECISION");
        let parsed: PostgreSqlColumnType = "BiGiNt".parse().unwrap();
        assert_eq!(parsed.as_str(), "BIGINT");
    }

    #[test]
    fn column_type_from_str_rejects_unknown_with_exact_message() {
        let err = "VARCHAR".parse::<PostgreSqlColumnType>().unwrap_err();
        assert_eq!(err, "Unknown column type: VARCHAR");
    }

    #[test]
    fn column_type_from_str_does_not_collapse_interior_whitespace() {
        // Only leading/trailing whitespace is stripped, so a doubled interior space fails.
        assert!("DOUBLE  PRECISION".parse::<PostgreSqlColumnType>().is_err());
    }

    // ── pg_type_from_arrow ────────────────────────────────────────────────────

    #[test]
    fn arrow_to_pg_type_mapping() {
        assert_eq!(pg_type_from_arrow(&DataType::Int8), "INTEGER");
        assert_eq!(pg_type_from_arrow(&DataType::Int16), "INTEGER");
        assert_eq!(pg_type_from_arrow(&DataType::Int32), "INTEGER");
        assert_eq!(pg_type_from_arrow(&DataType::Int64), "BIGINT");
        assert_eq!(pg_type_from_arrow(&DataType::UInt8), "BIGINT");
        assert_eq!(pg_type_from_arrow(&DataType::UInt16), "BIGINT");
        assert_eq!(pg_type_from_arrow(&DataType::UInt32), "BIGINT");
        assert_eq!(pg_type_from_arrow(&DataType::Float16), "REAL");
        assert_eq!(pg_type_from_arrow(&DataType::Float32), "REAL");
        assert_eq!(pg_type_from_arrow(&DataType::Float64), "DOUBLE PRECISION");
        assert_eq!(pg_type_from_arrow(&DataType::Boolean), "BOOLEAN");
        assert_eq!(pg_type_from_arrow(&DataType::Utf8), "TEXT");
        assert_eq!(pg_type_from_arrow(&DataType::LargeUtf8), "TEXT");
        assert_eq!(pg_type_from_arrow(&DataType::Binary), "BYTEA");
        assert_eq!(pg_type_from_arrow(&DataType::LargeBinary), "BYTEA");
        assert_eq!(pg_type_from_arrow(&DataType::Date32), "DATE");
        assert_eq!(pg_type_from_arrow(&DataType::Date64), "DATE");
    }

    #[test]
    fn arrow_to_pg_type_uint64_narrows_to_bigint() {
        // Lossy by design: Arrow UInt64 values above i64::MAX cannot round-trip
        // through a PostgreSQL BIGINT.
        assert_eq!(pg_type_from_arrow(&DataType::UInt64), "BIGINT");
    }

    #[test]
    fn arrow_to_pg_type_timestamps_drop_the_time_zone() {
        // A tz-aware Arrow timestamp still maps to a naive TIMESTAMP, and
        // `bind_arrow_value` formats the value as UTC — so the offset is lost.
        assert_eq!(
            pg_type_from_arrow(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "TIMESTAMP"
        );
        assert_eq!(
            pg_type_from_arrow(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("+05:30".into())
            )),
            "TIMESTAMP"
        );
    }

    #[test]
    fn arrow_to_pg_type_unmapped_types_fall_back_to_text() {
        assert_eq!(pg_type_from_arrow(&DataType::Decimal128(10, 2)), "TEXT");
        assert_eq!(pg_type_from_arrow(&DataType::Time64(TimeUnit::Microsecond)), "TEXT");
        assert_eq!(
            pg_type_from_arrow(&DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true
            )))),
            "TEXT"
        );
    }

    #[test]
    fn arrow_to_pg_type_output_always_parses() {
        // Guarantees the `map_err` arm in `get_postgresql_columns` is unreachable.
        for dt in [
            DataType::Int8,
            DataType::Int64,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Boolean,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Decimal128(10, 2),
        ] {
            assert!(pg_type_from_arrow(&dt).parse::<PostgreSqlColumnType>().is_ok());
        }
    }

    // ── get_postgresql_columns ────────────────────────────────────────────────

    #[test]
    fn postgresql_columns_preserve_name_type_and_order() {
        let schema = schema_of(&[
            ("id", DataType::Utf8, false),
            ("qty", DataType::Int64, false),
            ("price", DataType::Float64, false),
        ]);
        let cols = get_postgresql_columns(&schema, &[]).unwrap();
        let names: Vec<&str> = cols.iter().map(|c| c.column_name.as_str()).collect();
        assert_eq!(names, vec!["id", "qty", "price"]);
        let types: Vec<&str> = cols.iter().map(ty).collect();
        assert_eq!(types, vec!["TEXT", "BIGINT", "DOUBLE PRECISION"]);
    }

    #[test]
    fn postgresql_columns_nullability_ignores_arrow_field_nullability() {
        // Nullability comes ONLY from the `optional_columns` allowlist. An Arrow field
        // declared nullable but absent from that list still becomes NOT NULL, so a null
        // in the data will be rejected at insert time.
        let schema = schema_of(&[
            ("a", DataType::Int64, true),  // Arrow says nullable
            ("b", DataType::Int64, false), // Arrow says required
        ]);
        let cols = get_postgresql_columns(&schema, &["b".to_string()]).unwrap();
        assert!(!cols[0].nullable, "arrow-nullable 'a' should still be NOT NULL");
        assert!(cols[1].nullable, "'b' is nullable only because it is listed");
    }

    #[test]
    fn postgresql_columns_optional_list_may_name_missing_columns() {
        let schema = schema_of(&[("a", DataType::Int64, false)]);
        let cols = get_postgresql_columns(&schema, &["nonexistent".to_string()]).unwrap();
        assert_eq!(cols.len(), 1);
        assert!(!cols[0].nullable);
    }

    #[test]
    fn postgresql_columns_empty_schema_yields_no_columns() {
        assert!(get_postgresql_columns(&Schema::empty(), &[]).unwrap().is_empty());
    }

    // ── get_t_postgresql_columns ──────────────────────────────────────────────

    #[test]
    fn t_columns_synthesise_c0_and_rename_the_rest() {
        // Four distinct types so the shift-by-one alignment is actually proven:
        // out[i] takes base[i]'s TYPE but column_names[i - 1]'s NAME.
        let schema = schema_of(&[
            ("ignored", DataType::Int64, false),
            ("f1", DataType::Int32, false),
            ("f2", DataType::Float64, false),
            ("f3", DataType::Utf8, false),
        ]);
        let names = vec!["qty".to_string(), "price".to_string(), "label".to_string()];
        let cols = get_t_postgresql_columns(&schema, names, &[]).unwrap();

        let got: Vec<(&str, &str)> = cols.iter().map(|c| (c.column_name.as_str(), ty(c))).collect();
        assert_eq!(
            got,
            vec![
                ("c0", "TEXT"),
                ("qty", "INTEGER"),
                ("price", "DOUBLE PRECISION"),
                ("label", "TEXT"),
            ]
        );
    }

    #[test]
    fn t_columns_force_c0_to_text_not_null_regardless_of_parquet_type() {
        let schema = schema_of(&[
            ("ignored", DataType::Int64, false),
            ("f1", DataType::Int32, false),
        ]);
        let cols =
            get_t_postgresql_columns(&schema, vec!["qty".to_string()], &["c0".to_string()]).unwrap();
        assert_eq!(cols[0].column_name, "c0");
        assert_eq!(ty(&cols[0]), "TEXT");
        assert!(
            !cols[0].nullable,
            "c0 holds the generated UUID and is never nullable"
        );
    }

    #[test]
    fn t_columns_match_optional_against_the_new_name_not_the_parquet_name() {
        let schema = schema_of(&[
            ("ignored", DataType::Int64, false),
            ("f1", DataType::Int32, false),
        ]);

        let by_new = get_t_postgresql_columns(
            &schema,
            vec!["amount".to_string()],
            &["amount".to_string()],
        )
        .unwrap();
        assert!(by_new[1].nullable);

        let by_parquet =
            get_t_postgresql_columns(&schema, vec!["amount".to_string()], &["f1".to_string()])
                .unwrap();
        assert!(
            !by_parquet[1].nullable,
            "the parquet-side name must NOT satisfy the optional list"
        );
    }

    #[test]
    fn t_columns_reject_too_few_names() {
        let schema = schema_of(&[
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
            ("c", DataType::Int64, false),
            ("d", DataType::Int64, false),
        ]);
        let err = get_t_postgresql_columns(&schema, vec!["x".into(), "y".into()], &[]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Expected 3 columns, got 4 (t_mode expects parquet to have 1 extra leading column)"
        );
    }

    #[test]
    fn t_columns_reject_too_many_names() {
        let schema = schema_of(&[
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
        ]);
        let err =
            get_t_postgresql_columns(&schema, vec!["x".into(), "y".into(), "z".into()], &[])
                .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Expected 4 columns, got 2 (t_mode expects parquet to have 1 extra leading column)"
        );
    }

    #[test]
    fn t_columns_reject_empty_schema() {
        // 0 names implies 1 expected column, but an empty schema has none.
        assert!(get_t_postgresql_columns(&Schema::empty(), vec![], &[]).is_err());
    }

    // ── build_create_table_sql ────────────────────────────────────────────────
    //
    // This DDL is handed to sqlx via `AssertSqlSafe`, so the exact text matters.

    #[test]
    fn create_table_sql_basic_shape() {
        let cols = vec![
            col("c0", PostgreSqlColumnType::Text, false),
            col("c1", PostgreSqlColumnType::Integer, false),
        ];
        assert_eq!(
            build_create_table_sql("t", &cols, &[]).unwrap(),
            "CREATE TABLE IF NOT EXISTS \"public\".\"t\" (\n  \"c0\" TEXT NOT NULL,\n  \"c1\" INTEGER NOT NULL\n);"
        );
    }

    #[test]
    fn create_table_sql_nullable_column_omits_not_null() {
        let cols = vec![col("c1", PostgreSqlColumnType::Integer, true)];
        assert_eq!(
            build_create_table_sql("t", &cols, &[]).unwrap(),
            "CREATE TABLE IF NOT EXISTS \"public\".\"t\" (\n  \"c1\" INTEGER\n);"
        );
    }

    #[test]
    fn create_table_sql_places_unique_after_not_null() {
        let cols = vec![col("c1", PostgreSqlColumnType::Integer, false)];
        assert_eq!(
            build_create_table_sql("t", &cols, &["c1".to_string()]).unwrap(),
            "CREATE TABLE IF NOT EXISTS \"public\".\"t\" (\n  \"c1\" INTEGER NOT NULL UNIQUE\n);"
        );
    }

    #[test]
    fn create_table_sql_nullable_and_unique() {
        let cols = vec![col("c1", PostgreSqlColumnType::Integer, true)];
        assert_eq!(
            build_create_table_sql("t", &cols, &["c1".to_string()]).unwrap(),
            "CREATE TABLE IF NOT EXISTS \"public\".\"t\" (\n  \"c1\" INTEGER UNIQUE\n);"
        );
    }

    #[test]
    fn create_table_sql_honours_an_explicit_schema() {
        let cols = vec![col("c1", PostgreSqlColumnType::Text, false)];
        let sql = build_create_table_sql("myschema.t", &cols, &[]).unwrap();
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"myschema\".\"t\" ("), "{sql}");
    }

    #[test]
    fn create_table_sql_splits_on_the_first_dot_only() {
        // Documents current behaviour: `a.b.c` becomes schema "a" and a single
        // table identifier "b.c", not a three-part name.
        let cols = vec![col("c1", PostgreSqlColumnType::Text, false)];
        let sql = build_create_table_sql("a.b.c", &cols, &[]).unwrap();
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"a\".\"b.c\" ("), "{sql}");
    }

    #[test]
    fn create_table_sql_ignores_unique_names_that_match_no_column() {
        let cols = vec![col("c1", PostgreSqlColumnType::Text, false)];
        let sql = build_create_table_sql("t", &cols, &["nonexistent".to_string()]).unwrap();
        assert!(!sql.contains("UNIQUE"), "{sql}");
    }

    #[test]
    fn create_table_sql_rejects_an_empty_column_list() {
        let err = build_create_table_sql("t", &[], &[]).unwrap_err();
        assert_eq!(err.to_string(), "Parquet file has no columns");
    }

    // ── qident / qualify_public_default ───────────────────────────────────────

    #[test]
    fn loader_qident_matches_the_table_module_copy() {
        // Byte-identical duplicate of `postgresql_table::qident`; a test module here
        // cannot reach that private copy, so the expectations are restated.
        assert_eq!(qident("abc"), "\"abc\"");
        assert_eq!(qident("a\"b"), "\"a\"\"b\"");
        assert_eq!(qident("a.b"), "\"a.b\"");
        assert_eq!(qident(""), "\"\"");
    }

    #[test]
    fn qualify_public_default_adds_the_public_schema() {
        assert_eq!(qualify_public_default("t"), "public.t");
    }

    #[test]
    fn qualify_public_default_leaves_qualified_names_alone() {
        assert_eq!(qualify_public_default("s.t"), "s.t");
        assert_eq!(qualify_public_default("a.b.c"), "a.b.c");
    }

    #[test]
    fn qualify_public_default_on_empty_input() {
        assert_eq!(qualify_public_default(""), "public.");
    }

    // ── parquet_part_path / collect_existing_parts ────────────────────────────

    #[test]
    fn parquet_part_path_inserts_the_shard_suffix() {
        assert_eq!(parquet_part_path("data.parquet", 0), "data_xyz0.parquet");
        assert_eq!(parquet_part_path("/x/data.parquet", 12), "/x/data_xyz12.parquet");
    }

    #[test]
    fn parquet_part_path_replaces_every_occurrence() {
        // Documents current behaviour: `.replace` is global, so a directory component
        // ending in `.parquet` is rewritten too.
        assert_eq!(
            parquet_part_path("/a.parquet/b.parquet", 0),
            "/a_xyz0.parquet/b_xyz0.parquet"
        );
    }

    #[test]
    fn parquet_part_path_without_the_suffix_returns_the_input_unchanged() {
        // Every shard index maps to the SAME string, which is what allows
        // `collect_existing_parts` to loop forever if that path exists.
        assert_eq!(parquet_part_path("data", 0), "data");
        assert_eq!(parquet_part_path("data", 7), "data");
    }

    #[test]
    fn collect_existing_parts_returns_nothing_when_no_shard_exists() {
        let missing = "/nonexistent-codcel-test-dir/data.parquet";
        assert!(collect_existing_parts(missing).is_empty());
    }

    // ── bind_arrow_value ──────────────────────────────────────────────────────
    //
    // `QueryBuilder<Postgres>` holds no connection — it is `{ query, init_len, arguments }` —
    // so binding is entirely offline. Bound value BYTES are not readable, but the emitted
    // placeholder text is, which is the property the sqlx 0.9 upgrade actually put at risk.

    #[test]
    fn bind_advances_the_placeholder_counter_across_calls() {
        // The single most upgrade-relevant assertion here: sqlx 0.9 dropped the lifetime
        // from QueryBuilder<'a, DB> and made arguments owned. The bulk-insert loops depend
        // on the counter advancing correctly across successive binds on ONE builder.
        let a = Int64Array::from(vec![1i64, 2, 3]);
        let mut qb = QueryBuilder::<Postgres>::new("");
        for i in 0..3 {
            bind_arrow_value(&mut qb, &a, i).unwrap();
        }
        assert_eq!(qb.into_string(), "$1$2$3");
    }

    #[test]
    fn bind_null_emits_one_placeholder() {
        let a = Int64Array::from(vec![None::<i64>]);
        assert_eq!(bind_one(&a, 0).unwrap(), "$1");
    }

    #[test]
    fn bind_supported_scalar_types() {
        assert_eq!(bind_one(&Int64Array::from(vec![7i64]), 0).unwrap(), "$1");
        assert_eq!(bind_one(&Int32Array::from(vec![7i32]), 0).unwrap(), "$1");
        assert_eq!(bind_one(&Float64Array::from(vec![1.5f64]), 0).unwrap(), "$1");
        assert_eq!(bind_one(&Float32Array::from(vec![1.5f32]), 0).unwrap(), "$1");
        assert_eq!(bind_one(&BooleanArray::from(vec![true]), 0).unwrap(), "$1");
        assert_eq!(bind_one(&StringArray::from(vec!["x"]), 0).unwrap(), "$1");
        assert_eq!(bind_one(&LargeStringArray::from(vec!["x"]), 0).unwrap(), "$1");
    }

    #[test]
    fn bind_date_and_timestamp_types() {
        assert_eq!(bind_one(&Date32Array::from(vec![0i32]), 0).unwrap(), "$1");
        assert_eq!(bind_one(&Date64Array::from(vec![0i64]), 0).unwrap(), "$1");
        assert_eq!(
            bind_one(&TimestampSecondArray::from(vec![0i64]), 0).unwrap(),
            "$1"
        );
        assert_eq!(
            bind_one(&TimestampMillisecondArray::from(vec![0i64]), 0).unwrap(),
            "$1"
        );
        assert_eq!(
            bind_one(&TimestampMicrosecondArray::from(vec![0i64]), 0).unwrap(),
            "$1"
        );
        assert_eq!(
            bind_one(&TimestampNanosecondArray::from(vec![0i64]), 0).unwrap(),
            "$1"
        );
    }

    #[test]
    fn bind_unmapped_type_uses_the_string_fallback() {
        let a = Time64MicrosecondArray::from(vec![1_000i64]);
        assert_eq!(bind_one(&a, 0).unwrap(), "$1");
    }

    #[test]
    fn bind_date64_out_of_range_errors() {
        let a = Date64Array::from(vec![i64::MAX]);
        let err = bind_one(&a, 0).unwrap_err();
        assert!(
            err.to_string().contains("Invalid timestamp millis"),
            "{err}"
        );
    }

    #[test]
    fn bind_timestamp_second_out_of_range_errors() {
        let a = TimestampSecondArray::from(vec![i64::MAX]);
        let err = bind_one(&a, 0).unwrap_err();
        assert!(
            err.to_string().contains("Invalid timestamp seconds"),
            "{err}"
        );
    }

    #[test]
    fn bind_timestamp_millisecond_out_of_range_errors() {
        let a = TimestampMillisecondArray::from(vec![i64::MAX]);
        let err = bind_one(&a, 0).unwrap_err();
        assert!(
            err.to_string().contains("Invalid timestamp millis"),
            "{err}"
        );
    }

    #[test]
    fn bind_timestamp_nanosecond_before_epoch_errors() {
        // The sub-second remainder of a negative value is cast to u32 and wraps, which
        // chrono then rejects. A pre-1970 nanosecond timestamp therefore fails to load
        // with a misleading "Invalid timestamp nanos" message rather than converting.
        let a = TimestampNanosecondArray::from(vec![-1_500_000_000i64]);
        let err = bind_one(&a, 0).unwrap_err();
        assert!(err.to_string().contains("Invalid timestamp nanos"), "{err}");
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn bind_timestamp_microsecond_before_epoch_panics_in_debug() {
        // Same sign bug as the nanosecond branch, but here the wrapped remainder is
        // multiplied by 1_000, which overflows u32: a panic in debug builds, and a
        // silent wrap to a wrong timestamp in release builds.
        let a = TimestampMicrosecondArray::from(vec![-1_500_000i64]);
        let _ = bind_one(&a, 0);
    }

    #[test]
    #[should_panic]
    fn bind_date32_overflow_panics() {
        // `NaiveDate + Duration` is a panicking add. Every sibling branch returns Err
        // on out-of-range input; this one aborts the whole bulk insert instead.
        let a = Date32Array::from(vec![i32::MAX]);
        let _ = bind_one(&a, 0);
    }
}
