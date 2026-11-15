<!--
SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Changelog

## 0.1.10

Requires `codcel-calculation-engine` 0.1.10 and `codcel-table-engine` 0.1.10.

### Changed

- **The crate denies `clippy::unwrap_used`, `clippy::expect_used` and
  `clippy::panic`**, and production code contains none of them.

### Fixed

- `XLOOKUP` with binary-search mode `2` or `-2` panicked when the column held a
  floating-point `NaN`. Values are now ordered with a total order.
