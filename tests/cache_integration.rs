use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use disk_space_inspect::cache::Cache;
use disk_space_inspect::query::SearchQuery;
use disk_space_inspect::scanner::{self, CacheContext, ScanMessage, ScanStats};
use tempfile::TempDir;

fn create_file(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().unwrap()).expect("create parent");
    fs::write(path, contents).expect("write file");
}

fn canonical_temp_dir() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let canonical = dir.path().canonicalize().expect("canonicalize temp root");
    (dir, canonical)
}

fn make_query(root: &Path) -> SearchQuery {
    let mut query = SearchQuery::default();
    query.root = root.to_path_buf();
    query.raw = root.display().to_string();
    query
}

fn make_cache() -> (Cache, TempDir) {
    let cache_dir = tempfile::tempdir().expect("cache temp");
    let cache_path = cache_dir.path().join("cache.sqlite");
    let cache = Cache::open_in_path(cache_path).expect("open cache");
    (cache, cache_dir)
}

fn next_scan(
    handle: &scanner::ScannerHandle,
    rx: &crossbeam_channel::Receiver<ScanMessage>,
    query: SearchQuery,
    ctx: CacheContext,
) -> ScanStats {
    let job_id = handle.request_scan(query, Some(ctx));
    let timeout = Duration::from_secs(5);
    let mut stats = ScanStats::default();
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
fn cache_reuses_directories_on_subsequent_scans() {
    let (temp_root, canonical_root) = canonical_temp_dir();
    let dir_a = canonical_root.join("dir_a");
    let dir_b = canonical_root.join("dir_b");
    create_file(&dir_a.join("file1.txt"), "first run");
    create_file(&dir_b.join("file2.txt"), "first run");

    let (cache, _cache_dir) = make_cache();
    let root_cache = cache.load_root(&canonical_root).expect("load root cache");

    let (scanner, rx) = scanner::spawn();
    let ctx = CacheContext {
        cache: cache.clone(),
        root_id: root_cache.root_id,
        canonical_root: canonical_root.clone(),
    };

    let query = make_query(&canonical_root);

    // Warm cache
    let warm_stats = next_scan(&scanner, &rx, query.clone(), ctx.clone());
    assert_eq!(
        warm_stats.cached_dirs, 0,
        "first scan should not reuse cache"
    );

    // Second run should reuse both directories
    let reuse_stats = next_scan(&scanner, &rx, query.clone(), ctx.clone());
    assert!(
        reuse_stats.cached_dirs >= 2,
        "expected cached directories on second scan, got {}",
        reuse_stats.cached_dirs
    );
    assert_eq!(reuse_stats.cache_validation_errors, 0);

    // Mutate one directory
    create_file(&dir_a.join("file1.txt"), "updated contents");

    let mutate_stats = next_scan(&scanner, &rx, query, ctx);
    assert!(
        mutate_stats.cached_dirs >= 1,
        "other directories should still be reused"
    );
    assert_eq!(mutate_stats.cache_validation_errors, 0);

    drop(temp_root);
}
