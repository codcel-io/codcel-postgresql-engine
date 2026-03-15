<p align="center">
  <a href="https://codcel.io">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="assets/codcel-logo-lockup-dark.svg">
      <img src="assets/codcel-logo-lockup.svg" alt="Codcel" width="320">
    </picture>
  </a>
</p>

# Codcel PostgreSQL Engine

PostgreSQL table engine for Codcel — full CRUD and Excel-like lookups backed by PostgreSQL, with connection pooling and parameterized queries.

## Overview

Codcel PostgreSQL Engine implements the [`CodcelTable`](https://github.com/codcel-io/codcel-table-engine) trait for PostgreSQL databases. It provides Excel-compatible lookup operations alongside full CRUD support, with production-ready connection pooling, pre-built query templates, and parameterized queries for SQL safety. Tables can be initialized directly from Parquet file schemas.

This is one of the open-source components of [Codcel](https://codcel.io). Codcel converts your Excel spreadsheets into clean, human-readable source code — in Rust, Python, Java, C#, TypeScript, Go, Swift, and more. You get the full source code, and this engine is part of what you get: your generated projects use it directly for production-ready database access — all in transparent, inspectable Rust.

## Features

- **Excel-compatible lookups** — VLOOKUP, HLOOKUP, XLOOKUP, LOOKUP, MATCH, XMATCH, INDEX, FILTER
- **Full CRUD** — add, read, update, and delete rows with UUID-based IDs
- **Table initialization from Parquet** — create PostgreSQL tables from Parquet file schemas with optional data import
- **Connection pooling** — sqlx PgPool with configurable pool sizes, timeouts, and connection lifetimes
- **Parameterized queries** — all user values use bind parameters; no string interpolation
- **Pre-built query templates** — common operations use pre-computed SQL for reduced allocations
- **9 PostgreSQL types** — Integer, BigInt, Real, DoublePrecision, Boolean, Text, Bytea, Date, Timestamp

## Quick Start

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
codcel-postgresql-engine = { git = "https://github.com/codcel-io/codcel-postgresql-engine.git", branch = "main" }
```

Initialize a table:

```rust
use codcel_postgresql_engine::PostgreSQLTable;

let table = PostgreSQLTable::init(
    "my_database",
    "data/products.parquet",
    "products",
    true,  // insert Parquet data into the table
).await?;
```

Then use any `CodcelTable` operation — `v_lookup`, `x_lookup`, `filter`, `add_row`, `update_row`, and more.

Set the `CODCEL_POSTGRESQL_URL` environment variable to configure the connection (defaults to `postgresql://{username}@localhost:5432/`).

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

## About Codcel

[Codcel](https://codcel.io) turns Excel spreadsheets into production-ready software — real source code in Rust, Python, Java, C#, TypeScript, Go, Swift, and more, with zero platform lock-in.

This PostgreSQL engine is one of several open-source components that power Codcel. Learn more at [codcel.io](https://codcel.io).

## Licensing

This project is available under **multiple licenses**:

- **MIT License** (LICENSE-MIT)
- **Apache License 2.0** (LICENSE-APACHE)
- **Codcel Commercial License** (LICENSE-CODCEL-COMMERCIAL)

This licensing model is intended to keep the Excel calculation logic
open and auditable, while protecting Codcel from direct competition
in automated spreadsheet-to-code platforms.

### Which license should I use?

- If you are an individual developer, company, or library author using
  these Excel functions directly in your application, you may choose
  **MIT or Apache-2.0**.

- If you are using this software as part of an **Automated Code Generation
  System (excluding Codcel itself)**, you must use the
  **Codcel Commercial License**.

See LICENSE-CODCEL-COMMERCIAL for details.
