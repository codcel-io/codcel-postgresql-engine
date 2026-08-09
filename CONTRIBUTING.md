# Contributing to Codcel PostgreSQL Engine

Thank you for your interest in contributing to the Codcel PostgreSQL Engine.

This repository contains the Rust implementation of Codcel's PostgreSQL-backed table engine, which enables Excel-like lookup, filter, and CRUD operations on data stored in PostgreSQL databases.

We welcome contributions that improve correctness, performance, compatibility, maintainability, and documentation.

---

## Scope of This Repository

This repository is intended for work related to the PostgreSQL table engine, including:

- Excel-compatible lookup operations (VLOOKUP, HLOOKUP, XLOOKUP, INDEX, MATCH, XMATCH)
- Filtering and conditional query logic
- CRUD operations (add, read, update, delete rows)
- Parquet-to-PostgreSQL data loading
- PostgreSQL connection management and query optimisation
- SQL generation correctness and safety
- Type system and column handling
- Tests and compatibility validation
- Internal engine refactoring
- Developer documentation for the engine

If your issue is about the Codcel product itself rather than this specific Rust engine repository, please use the Codcel contact channels:

- General contact: https://codcel.io/contact
- Product bug reports: https://codcel.io/contact/bugs
- Feature requests: https://codcel.io/contact/features

---

## Before You Start

Before opening a Pull Request, please:

- Check whether a similar issue or pull request already exists
- Keep the change focused and limited in scope
- Add or update tests for any behaviour change
- Avoid unrelated formatting-only changes in the same PR
- Ensure SQL generation remains safe from injection vulnerabilities

---

## Reporting Bugs

If you find a bug in this repository, please open a GitHub issue.

A good bug report includes:

- The operation or function involved (e.g. VLOOKUP, XLOOKUP, filter, data loading)
- The input values or query parameters used
- The actual result
- The expected result
- Whether the expected result was verified against Excel or PostgreSQL directly
- A minimal reproducible example
- Any relevant logs, panic messages, or failing test output

Examples of useful issue titles:

- `XLOOKUP returns incorrect result with approximate match mode`
- `Parquet loader fails for columns with NULL values`
- `Filter operation generates incorrect SQL for multi-condition queries`
- `Connection pool exhaustion under concurrent lookups`

If the problem is in the Codcel application rather than this Rust engine, report it via:

- https://codcel.io/contact/bugs
- bugs@codcel.io

---

## Suggesting Enhancements

Enhancements are welcome.

Examples include:

- New lookup or table operation support
- Better compatibility with Excel edge cases in lookup behaviour
- Query performance improvements
- Connection pool tuning
- Improved Parquet loading capabilities
- Refactoring that improves readability or maintainability
- Improved test coverage
- Developer tooling improvements

For broader product ideas, language targets, or commercial feature requests, please use:

- https://codcel.io/contact/features
- features@codcel.io

---

## Contribution Workflow

All changes must come through Pull Requests.

Direct commits to protected branches should not be used except by repository owners when absolutely necessary.

Typical workflow:

1. Fork the repository
2. Create a branch for your change
3. Make your changes
4. Add or update tests
5. Run the test suite
6. Open a Pull Request

Example branch names:

- `fix-xlookup-approximate-match`
- `add-parquet-null-handling`
- `refactor-query-templates`

---

## Pull Request Guidelines

Please keep Pull Requests focused and clearly explained.

Each Pull Request should ideally include:

- A short summary of the change
- The reason for the change
- The expected behaviour being matched or improved
- Notes on any edge cases
- Tests added or updated
- Any compatibility or migration considerations

Please avoid mixing multiple unrelated changes into a single PR.

---

## SQL Safety Expectations

This project generates and executes SQL against PostgreSQL databases.

When contributing query logic:

- Always use parameterised queries with `PgArguments` for user-supplied values
- Use proper identifier quoting for table and column names
- Never interpolate untrusted values directly into SQL strings
- Review generated SQL for injection risks
- Consider the impact of schema changes on existing queries
- Preserve backward compatibility with existing table structures where practical

---

## Local Development Across Repositories

This crate depends on `codcel-calculation-engine` and `codcel-table-engine` by published
version, not by git. To build against your local checkouts instead of the crates.io
releases, add a `[patch.crates-io]` section to your `~/.cargo/config.toml`:

```toml
[patch.crates-io]
codcel-calculation-engine = { path = "/path/to/codcel-calculation-engine" }
codcel-table-engine       = { path = "/path/to/codcel-table-engine" }
```

This applies to every cargo invocation on your machine, so unreleased changes in one
engine are picked up by the others without a publish. Remove or comment it out before
verifying that a change works against the published crates.

---

## Tests

Tests are required for behaviour changes.

Please add or update tests when you:

- Fix a bug
- Add or change a lookup or table operation
- Change query generation logic
- Refactor logic that could affect results

Where useful, tests should include:

- Standard cases
- Edge cases
- Invalid argument cases
- Excel compatibility cases for lookup functions
- Regression tests for previous bugs

If the repository already has conventions for test placement or naming, follow those conventions.

Before submitting a PR, run:

```bash
cargo test
```

If applicable, also run:

```bash
cargo fmt
cargo clippy --all-targets --all-features
```

Note: the unit tests are hermetic — they need no PostgreSQL instance, no network, and no
`CODCEL_*` environment variables. They cover the SQL-building, schema-mapping and
value-binding helpers. The code paths that talk to a database (the `CodcelTable` methods,
`PostgreSQLTable::init`, and the Parquet loaders) are not covered by automated tests and
must be exercised manually against a real instance; see the environment configuration
section of the README for connection details.

---

## Coding Style

Please follow normal Rust best practices:

- Keep functions focused
- Prefer clear naming over cleverness
- Avoid unnecessary allocations where possible
- Keep public behaviour stable unless intentionally changing it
- Add comments where SQL generation logic is non-obvious
- Add comments where Excel lookup behaviour differs from intuitive expectations
- Prefer small, reviewable refactors

Use `cargo fmt` formatting conventions.

---

## Documentation

If your change affects public behaviour, update relevant documentation as appropriate.

Examples:

- Supported operations list
- Behaviour notes for lookup functions
- Known limitations
- Examples
- Compatibility notes
- Environment variable documentation

If the change is primarily documentation-related, consider whether it belongs in `codcel-docs` instead of this repository.

---

## Confidentiality and Example Files

Do not submit confidential data, customer databases, or regulated data.

If example data is needed:

- Use anonymized or synthetic data
- Reduce the dataset to the smallest reproducible example
- Remove sensitive business information
- Do not include real database connection strings or credentials

---

## Review and Merge Process

All Pull Requests are reviewed by a maintainer.

Maintainers may request changes for:

- correctness
- SQL safety
- test coverage
- code clarity
- repository scope

A Pull Request may be rejected if it:

- lacks tests for behavioural changes
- changes unrelated areas unnecessarily
- introduces SQL injection risks
- introduces unclear behaviour differences
- belongs in another repository

---

## Licensing

This project is dual licensed under the MIT License ([LICENSE-MIT](LICENSE-MIT)) and the
Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)), at the user's option.

Unless you explicitly state otherwise, any contribution you intentionally submit for
inclusion in this repository, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.

### Developer Certificate of Origin

Contributions must be signed off under the [Developer Certificate of Origin](https://developercertificate.org). Signing off certifies that you wrote the
contribution, or otherwise have the right to submit it under the licenses above.

Add the sign-off by committing with `-s`:

```bash
git commit -s -m "Escape identifiers in generated WHERE clause"
```

This appends a trailer to your commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

The name and email must match those on the commit. Pull requests whose commits are not
signed off cannot be merged.

---

## Thank You

We appreciate contributions that help improve correctness, performance, and developer experience in the Codcel PostgreSQL Engine.
