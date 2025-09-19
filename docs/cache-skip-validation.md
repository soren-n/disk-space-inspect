# Cache Skip & Validation Notes

This document captures the guiding rules for cache-driven directory skips and how to investigate regressions.

## Skip Decision Matrix

A directory is reused from cache when **all** of the following hold:

1. The cached entry exists for the relative path.
2. `flags & 1 == 0` (the entry and its ancestors are clean).
3. The cached `mtime` matches the filesystem `metadata.modified()` value.

If any check fails, the scanner walks the directory normally and writes fresh entries back into SQLite. Validation errors during replay mark the entire root dirty so the next scan recomputes aggregates.

## Validation Flow

After each successful scan we re-run `Cache::validate_aggregate` starting at `.` to verify aggregate sizes. Failures are logged, recorded in `ScanStats.cache_validation_errors`, and force the root into a dirty state.

## Troubleshooting Steps

1. Reproduce with `RUST_LOG=dusk=trace` to capture skip telemetry (`ScanStats` now logs reuse counts).
2. Run `cargo test cache::tests::validate_aggregate_detects_mismatch` to ensure the mismatch guard still fires.
3. Inspect the cache directly via `sqlite3 ~/.cache/dusk/dusk.sqlite 'SELECT path, aggregate_size, flags FROM entries WHERE root_id = ...'`.
4. If aggregates drift, delete the affected root with `dusk --clear-cache <path>` and re-run a cold scan.
