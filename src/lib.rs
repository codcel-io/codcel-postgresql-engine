// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0 OR Codcel-Commercial
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT, LICENSE-APACHE, and LICENSE-CODCEL-COMMERCIAL in the project root.

//! PostgreSQL table implementation for the Codcel calculation engine.
//!
//! This crate provides a PostgreSQL-backed table implementation that supports
//! Excel-like lookup and filter operations. It allows you to:
//!
//! - Load data from Parquet files into PostgreSQL tables
//! - Perform VLOOKUP, HLOOKUP, XLOOKUP, INDEX, MATCH, and XMATCH operations
//! - Filter data using complex conditions
//! - Perform CRUD operations (add, read, update, delete rows)
//!
//! # Main Types
//!
//! - [`PostgreSQLTable`] - The main table struct that implements the `CodcelTable` trait
//! - [`PostgreSqlColumnType`] - Enum representing supported PostgreSQL column types
//! - [`PostgresSqlColumn`] - Metadata for a single table column
//!
//! # Example
//!
//! ```rust,ignore
//! use codcel_postgresql_engine::PostgreSQLTable;
//!
//! // Initialize a table from a Parquet file
//! let table = PostgreSQLTable::init(
//!     "data.parquet".to_string(),
//!     "data",
//!     "my_database",
//!     vec![],
//!     "crud.parquet".to_string(),
//!     "crud",
//!     true,
//!     vec![],
//!     vec![],
//! ).await?;
//! ```

pub mod postgresql_table;
pub(crate) mod postgresql_table_loader;

pub use postgresql_table::PostgreSQLTable;
pub use postgresql_table_loader::{PostgreSqlColumnType, PostgresSqlColumn};
