# Disk Space Inspect

Disk Space Inspect (shipped on the command line as `dusk`) is a native desktop application built in Rust with `eframe`/`egui` for exploring the filesystem, identifying large files, and surfacing opportunities to reclaim disk space. The interface streams results as they arrive so you can start investigating before an entire scan completes.

## Features
- Tree view of directories and files with per-item size and aggregated directory totals
- Background filesystem scanning with responsive UI updates via a worker thread
- Search bar that accepts glob-style patterns (e.g. `~/Downloads/*.zip`) and optional size filters such as `>500MB`
- Incremental streaming of results so large scans become visible immediately
- Inline error reporting for unreadable paths or permissions issues
- Staging workflow: select files/folders for deletion, review in a confirmation modal, and remove them directly from the UI
- SQLite-backed scan cache: previously scanned directories load instantly, and unchanged subtrees are skipped on subsequent runs

## Getting Started
1. Install the Rust toolchain (Rust 1.79 or newer is recommended). The easiest path is [`rustup`](https://rustup.rs/).
2. Run the application:
   ```bash
   cargo run --release
   ```
   The release profile is recommended for smoother UI rendering, but `cargo run` works for development.

   After installing with `cargo install --path .`, you can start the app from any directory with the CLI entry point:
   ```bash
   dusk ~/Downloads
   ```
   If no path is supplied, `dusk` starts in the current working directory.

   The repository ships with a size-optimised release profile (`opt-level = "z"`, `lto = "fat"`, `strip = "symbols"`) so the packaged binary stays lean—rebuild with `cargo build --release` to pick up these settings after any changes.

   Scan metadata is cached under `~/.cache/dusk/dusk.sqlite` (respects `XDG_CACHE_HOME`); the cache is keyed by canonical root path so repeated runs open immediately.

## Search Syntax
The search bar accepts a concise syntax inspired by shell globbing:
- **Path / glob pattern** – Supports `*`, `?`, and `**` wildcards. Prefix the pattern with an absolute or relative path (e.g. `/var/log/**/*.log`, `~/Pictures/*`). All paths are interpreted relative to the working directory that `dusk` was launched with; absolute-style patterns are automatically rebased under that root. If no pattern is supplied the current working directory is scanned.
- **Size filter (optional)** – Append comparisons like `>500MB`, `< 2GiB`, or `>=1.5GB`. Both decimal (GB) and binary (GiB) units are accepted.
- Examples:
  - `~/Downloads/*.zip >500MB`
  - `/var/log/**/*.log <50MiB`
  - `*` (scan the current directory tree)

Directories are always streamed so that matching files retain their structure; size filters are applied to files, and aggregate directory sizes are computed as results arrive.

## Staging & Deletion
- Use the checkbox column in the tree to stage files or folders you want to remove.
- Review staged items via the `Commit staged` button in the footer; a confirmation modal lists everything slated for deletion.
- Choosing **Confirm delete** removes the entries from disk and triggers a fresh scan; failures are surfaced inline so you can retry after resolving permissions or locking issues.

## Architecture Notes
- **UI:** `eframe` / `egui` renders the desktop interface, including the tree grid and status panels.
- **Background worker:** a dedicated thread walks the filesystem with `walkdir`, sending incremental updates over `crossbeam-channel` to keep the UI responsive.
- **Filtering:** glob patterns are handled by `globset`, while size constraints are parsed into byte comparisons before dispatching a scan.
- **Caching:** scan results are persisted to a per-root SQLite database; unchanged directories are rehydrated from cache and skipped during later walks, dramatically reducing full-disk rescans.
- **Formatting:** timestamps are displayed in local time with `chrono`, and byte counts are converted into human-friendly units on the fly.

## Next Steps
Planned enhancements include richer query operators (age, file type), persisted workspace settings, and batch file operations for reclaiming space directly from the UI.
