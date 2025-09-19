use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use disk_space_inspect::cache::Cache;
use disk_space_inspect::query::SearchQuery;
use disk_space_inspect::scanner::{self, CacheContext, ScanMessage, ScanStats};
use serde::Deserialize;
use tempfile::TempDir;

#[derive(Debug, Deserialize)]
struct Snapshot {
    #[serde(default)]
    _root: String,
    total_size: u64,
    files_scanned: u64,
    dirs_scanned: u64,
    #[serde(default)]
    _cached_dirs: u64,
    #[serde(default)]
    _cached_entries: u64,
    #[serde(default)]
    _elapsed_ms: u128,
}

fn load_snapshot(name: &str) -> Snapshot {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("benchmarks");
    path.push(format!("{name}.json"));
    let data = fs::read_to_string(path).expect("read snapshot");
    serde_json::from_str(&data).expect("parse snapshot")
}

fn sample_root(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("benchmarks/samples");
    path.push(name);
    path.canonicalize().expect("canonicalize sample")
}

fn make_cache() -> (Cache, TempDir) {
    let dir = tempfile::tempdir().expect("cache tempdir");
    let path = dir.path().join("cache.sqlite");
    let cache = Cache::open_in_path(path).expect("open cache");
    (cache, dir)
}

fn make_query(root: &Path) -> SearchQuery {
    let mut query = SearchQuery::default();
    query.root = root.to_path_buf();
    query.raw = root.display().to_string();
    query
}

fn run_scan(
    handle: &scanner::ScannerHandle,
    rx: &crossbeam_channel::Receiver<ScanMessage>,
    query: SearchQuery,
    ctx: CacheContext,
) -> ScanStats {
    let job_id = handle.request_scan(query, Some(ctx));
    let mut stats = ScanStats::default();
    let timeout = Duration::from_secs(5);
    while let Ok(message) = rx.recv_timeout(timeout) {
        match message {
            ScanMessage::Stats {
                job_id: msg_id,
                stats: s,
            } if msg_id == job_id => {
                stats = s;
            }
            ScanMessage::Complete { job_id: msg_id } if msg_id == job_id => {
                break;
            }
            _ => {}
        }
    }
    stats
}

#[test]
fn benchmark_snapshots_stay_stable() {
    for name in ["tiny", "medium", "large"] {
        let snapshot = load_snapshot(name);
        let root = sample_root(name);
        let (cache, _cache_dir) = make_cache();
        let root_cache = cache.load_root(&root).expect("load root cache");
        let (scanner, rx) = scanner::spawn();
        let ctx = CacheContext {
            cache: cache.clone(),
            root_id: root_cache.root_id,
            canonical_root: root.clone(),
        };

        let query = make_query(&root);
        let stats = run_scan(&scanner, &rx, query, ctx);
        assert_eq!(
            stats.files_scanned, snapshot.files_scanned,
            "files scanned mismatch for {name}"
        );
        assert_eq!(
            stats.dirs_scanned, snapshot.dirs_scanned,
            "dirs scanned mismatch for {name}"
        );
        let summary = cache
            .validate_aggregate(root_cache.root_id, Path::new("."))
            .expect("aggregate validation");
        assert_eq!(
            summary.total_size, snapshot.total_size,
            "total size mismatch for {name}"
        );
    }
}
