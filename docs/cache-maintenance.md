# Cache Maintenance Guide

This document explains how the SQLite cache is managed and how to debug issues when aggregates drift.

## Automatic Hygiene

- Each scan increments `roots.scan_count`. Every fifth scan (or when more than one hour has elapsed since the last prune) the cache removes stale rows older than 30 days and trims the database back under 512 MB.
- The per-database `PRAGMA user_version` is bumped to `1` on startup to make future migrations deterministic. New migration steps can be added to `CACHE_MIGRATIONS` in `cache.rs`; they will be applied in order and the pragma updated automatically.
- `ScanSession::finish` records `last_scan_utc` for observability and relies on `prune_if_needed` to compact the table opportunistically.

## Clearing a Root

- Use the CLI: `dusk --clear-cache <path>`.
- Or, in-app: right-click the root row and choose **Clear Cache**. The request is routed through the scanner thread so active scans are cancelled before deletion.
- Once the worker finishes, the UI reloads the root metadata and triggers a fresh scan.

## Manual Inspection

```bash
sqlite3 ~/.cache/dusk/dusk.sqlite "SELECT path, aggregate_size, flags FROM entries WHERE root_id = <id>;"
```

Flags are bitfields; the lowest bit indicates a dirty entry that cannot be reused for cache skips.

## When Aggregates Drift

1. Verify the guardrails by running `cargo test cache::tests::validate_aggregate_detects_mismatch`.
2. Inspect recent stats in the status bar (`fs errors` and `cache errs` counters) and in the log line `dusk scan stats ...`.
3. If a root refuses to heal, clear the cache for that root and rerun a cold scan.
