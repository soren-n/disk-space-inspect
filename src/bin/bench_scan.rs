use std::path::{Path, PathBuf};
use std::time::Instant;

use disk_space_inspect::cache::Cache;
use disk_space_inspect::query::SearchQuery;
use disk_space_inspect::scanner::{self, ScanMessage, ScanStats};
use pico_args::Arguments;
use serde::Serialize;

#[derive(Serialize)]
struct Snapshot {
    root: String,
    total_size: u64,
    files_scanned: u64,
    dirs_scanned: u64,
    cached_dirs: u64,
    cached_entries: u64,
    elapsed_ms: u128,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("bench_scan: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = Arguments::from_env();
    let snapshot_path: Option<PathBuf> = args
        .opt_value_from_str("--snapshot")
        .map_err(|e| e.to_string())?;
    let root_arg: Option<String> = args.opt_free_from_str().map_err(|e| e.to_string())?;
    let leftover = args.finish();
    if !leftover.is_empty() {
        return Err("unexpected positional arguments".into());
    }

    let root = match root_arg {
        Some(raw) => PathBuf::from(expand_path(&raw)?),
        None => std::env::current_dir().map_err(|e| e.to_string())?,
    };
    if !root.exists() {
        return Err(format!("{} does not exist", root.display()));
    }

    let canonical = root
        .canonicalize()
        .map_err(|err| format!("failed to canonicalize {}: {err}", root.display()))?;

    let cache = Cache::open().map_err(|err| err.to_string())?;
    let root_cache = cache.load_root(&canonical).map_err(|err| err.to_string())?;

    let (scanner, rx) = scanner::spawn();

    let mut query = SearchQuery::default();
    query.root = canonical.clone();

    let cache_ctx = scanner::CacheContext {
        cache: cache.clone(),
        root_id: root_cache.root_id,
        canonical_root: canonical.clone(),
    };

    let start = Instant::now();
    let job_id = scanner.request_scan(query, Some(cache_ctx));

    let mut stats: Option<ScanStats> = None;
    let mut entries = 0usize;

    while let Ok(message) = rx.recv() {
        match message {
            ScanMessage::Entry {
                job_id: msg_job, ..
            } if msg_job == job_id => {
                entries += 1;
            }
            ScanMessage::Stats {
                job_id: msg_job,
                stats: s,
            } if msg_job == job_id => {
                stats = Some(s);
            }
            ScanMessage::Complete { job_id: msg_job } if msg_job == job_id => {
                break;
            }
            _ => {}
        }
    }

    let elapsed = start.elapsed();
    let stats = stats.unwrap_or_default();

    println!(
        "Scan complete: {} entries, {} files, {} dirs (cached dirs: {}, cached entries: {}) in {:?}",
        entries,
        stats.files_scanned,
        stats.dirs_scanned,
        stats.cached_dirs,
        stats.cached_entries,
        elapsed,
    );

    let summary = cache
        .validate_aggregate(root_cache.root_id, Path::new("."))
        .map_err(|err| err.to_string())?;

    if let Some(path) = snapshot_path {
        let snapshot = Snapshot {
            root: canonical.display().to_string(),
            total_size: summary.total_size,
            files_scanned: stats.files_scanned,
            dirs_scanned: stats.dirs_scanned,
            cached_dirs: stats.cached_dirs,
            cached_entries: stats.cached_entries,
            elapsed_ms: elapsed.as_millis(),
        };
        let json = serde_json::to_string_pretty(&snapshot).map_err(|err| err.to_string())?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
            }
        }
        std::fs::write(&path, json).map_err(|err| err.to_string())?;
        println!("Snapshot written to {}", path.display());
    }

    Ok(())
}

fn expand_path(raw: &str) -> Result<String, String> {
    shellexpand::full(raw)
        .map(|cow| cow.into_owned())
        .map_err(|err| err.to_string())
}
