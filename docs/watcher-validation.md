# Watcher Validation Notes

Date: 2025-02-14
Environment: macOS 14.4 (Apple M3 Pro)

## Automated Smoke Test
- Added `tests/watcher.rs` which spawns the watcher against a temporary directory, writes a file, and asserts that at least one `Dirty` or `Rescan` event is observed within 5 seconds.
- Run locally via `cargo test watcher_reports_dirty_event -- --nocapture`.

## Manual Observations
- The watcher delivers a `Dirty` event for file creations within ~200 ms on macOS. No fallback polling was triggered in the sample run.
- Cache ancestry marking occurs for each dirty path; subsequent scans report cache invalidation as expected (see `ScanStats.cached_dirs`).

## Outstanding Coverage
| Platform | Status | Notes |
| --- | --- | --- |
| macOS (FSEvents) | ✅ Verified via automated smoke test and manual logging | Additional testing recommended for directory renames/deletes.
| Linux (inotify) | ⏳ Pending | Need access to Linux environment; expect parity but should confirm permission edge cases.
| Windows (ReadDirectoryChangesW) | ⏳ Pending | `notify` should provide backend; verify path normalization and long-path behaviour.

## Next Actions
1. Extend smoke test to exercise rename and deletion events once cross-platform coverage is available.
2. Capture watcher logs with `RUST_LOG=dusk=trace` during long-running sessions to tune debounce and polling fallback timings. Adjust CLI flags `--watch-poll` and `--watch-max-poll` to experiment with fallback cadence.
