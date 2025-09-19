# Benchmarks

Use `cargo run --bin bench_scan -- <path> [--snapshot benchmarks/<name>.json]` to capture scan statistics
for representative directories. Store the resulting JSON snapshots in this directory so that future runs can
be diffed to spot regressions.

## Baseline 2025-02-14 (MacBook Pro M3, APFS SSD)

Commands executed:

```bash
cargo run --quiet --bin bench_scan -- benchmarks/samples/tiny --snapshot benchmarks/tiny.json
cargo run --quiet --bin bench_scan -- benchmarks/samples/medium --snapshot benchmarks/medium.json
cargo run --quiet --bin bench_scan -- benchmarks/samples/large --snapshot benchmarks/large.json
```

Machine details:
- Apple M3 Pro (12‑core CPU)
- 32 GB RAM
- macOS 14.4
- Internal APFS SSD

All snapshots were generated from a clean cache; rerun the same commands after modifying cache behaviour to collect before/after measurements.
