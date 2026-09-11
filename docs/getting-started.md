# Getting started

This guide assumes you recognize RCDB conditions and CCDB table paths, but does not assume prior experience with `gluex-rs`.

## Install for Python

Install a published wheel with your Python package manager, or build the checkout for development:

```console
uv sync
just build-python
```

The module is named `gluex`; the distribution is named `gluex-rs`.

## Build for Rust and the CLI

```console
cargo build --release
cargo test
```

The executable is `target/release/gluex`. Rust users depend on the `gluex-rs` package and import the `gluex_rs` crate.

## Obtain database snapshots

Queries use unchanged local SQLite snapshots. Point the library at them explicitly:

```python
import gluex

gx = gluex.connect(rcdb="/data/rcdb.sqlite", ccdb="/data/ccdb.sqlite")
print(gx.capabilities.rcdb, gx.capabilities.ccdb)
```

Or configure the conventional variables:

```console
export RCDB_CONNECTION=/data/rcdb.sqlite
export CCDB_CONNECTION=/data/ccdb.sqlite
```

```python
gx = gluex.connect()
```

Pass `gluex.DISABLED` for a source you do not need. A missing environment variable disables that capability; a configured but invalid file is an error.

## The session model

`GlueX` owns the two readers and exposes four useful entry points:

- `gx.runs` selects recorded runs and RCDB conditions.
- `gx.calibrations` discovers and reads CCDB tables.
- `gx.workflows` performs canonical multi-database calculations.
- `gx.sources` exposes database-native readers for advanced or raw access.

Query builders are lazy. Calls such as `select`, `where`, `columns`, `tables`, and `coherent_peak` construct new values; `collect`, `compute`, or iteration performs the read.

Results are immutable and retain provenance. Inspect the associated `report` to distinguish missing source data from runs rejected by a condition.

## Snapshot time and refresh

Opening a session captures a UTC time. CCDB queries without an explicit `as_of` use that time so a long analysis does not silently mix assignments added later. `gx.refresh()` reopens the same files and establishes a new cutoff. Do not replace a SQLite file while a session or result still refers to it.

## Where to go next

Start with [run selection](runs.md), then [calibration reads](calibrations.md). Use the [luminosity workflow](luminosity.md) when both databases are available.
