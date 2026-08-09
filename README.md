<p align="center">
  <a href="https://codcel.io">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/codcel-io/codcel-postgresql-engine/refs/tags/release-0.1.9/assets/codcel-logo-lockup-dark.svg">
      <img src="https://raw.githubusercontent.com/codcel-io/codcel-postgresql-engine/refs/tags/release-0.1.9/assets/codcel-logo-lockup.svg" alt="Codcel" width="320">
    </picture>
  </a>
</p>

# Codcel PostgreSQL Engine

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#licensing)

PostgreSQL table engine for Codcel — full CRUD and Excel-like lookups backed by PostgreSQL, with connection pooling and prepared-statement writes.

## Overview

Codcel PostgreSQL Engine implements the [`CodcelTable`](https://github.com/codcel-io/codcel-table-engine) trait for PostgreSQL databases. It provides Excel-compatible lookup operations alongside full CRUD support, with production-ready connection pooling, pre-built query templates, and prepared-statement writes. Lookup and filter paths build SQL through a condition builder that validates identifiers and escapes literals. Tables can be initialized directly from Parquet file schemas.

This is one of the open-source components of [Codcel](https://codcel.io). Codcel converts your Excel spreadsheets into clean, human-readable source code — in Rust, Python, Java, C#, TypeScript, Go, Swift, and more. You get the full source code, and this engine is part of what you get: your generated projects use it directly for production-ready database access — all in transparent, inspectable Rust.

## Features

- **Excel-compatible lookups** — VLOOKUP, HLOOKUP, XLOOKUP, LOOKUP, MATCH, XMATCH, INDEX, FILTER
- **Full CRUD** — add, read, update, and delete rows with UUID-based IDs
- **Table initialization from Parquet** — create PostgreSQL tables from Parquet file schemas with optional data import
- **Connection pooling** — sqlx PgPool with configurable pool sizes, timeouts, and connection lifetimes
- **Prepared-statement writes** — CRUD operations bind all user values as query parameters
- **Pre-built query templates** — common operations use pre-computed SQL for reduced allocations
- **9 PostgreSQL types** — Integer, BigInt, Real, DoublePrecision, Boolean, Text, Bytea, Date, Timestamp

## Quick Start

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
codcel-postgresql-engine = "0.1.9"
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

Licensed under either of

- MIT License ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)

at your option. There are no field-of-use restrictions and no commercial carve-outs.

This crate is the PostgreSQL table backend for [Codcel](https://codcel.io), a
commercial product. It is published under permissive terms so that anyone — including
customers whose generated code depends on it — can read, audit and verify exactly how
their data is queried. Contributions are welcome, but support is best effort.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions. Contributions require
a Developer Certificate of Origin sign-off — see [CONTRIBUTING.md](CONTRIBUTING.md).
