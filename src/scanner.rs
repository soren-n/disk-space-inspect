use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use crossbeam_channel::{Receiver, Sender, unbounded};
use globset::{Glob, GlobBuilder, GlobSet, GlobSetBuilder};
use rusqlite::Error as SqliteError;
use walkdir::WalkDir;

use crate::cache::{self, AggregateSummary, Cache, CacheValidationError};
use crate::fs::{FileEntry, FileKind};
use crate::query::{SearchQuery, SizeFilter};

#[derive(Clone)]
pub struct CacheContext {
    pub cache: Cache,
    pub root_id: i64,
    pub canonical_root: PathBuf,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ScanStats {
    pub files_scanned: u64,
    pub dirs_scanned: u64,
    pub cached_dirs: u64,
    pub cached_entries: u64,
    pub cached_bytes: u64,
    pub fs_errors: u64,
    pub cache_validation_errors: u64,
}

#[derive(Debug, Default, Clone)]
struct DirectoryFrame {
    relative: PathBuf,
    parent: Option<PathBuf>,
    direct_size: u64,
    aggregate_size: u64,
    modified: Option<i64>,
    created: Option<i64>,
}

#[derive(Debug, Default, Clone, Copy)]
struct EmitStats {
    aggregate_size: u64,
    entries: usize,
    directories: usize,
    files: usize,
}

#[derive(Debug)]
enum CachedReplayError {
    Cache(CacheValidationError),
    Storage(SqliteError),
}

impl From<CacheValidationError> for CachedReplayError {
    fn from(value: CacheValidationError) -> Self {
        CachedReplayError::Cache(value)
    }
}

impl From<SqliteError> for CachedReplayError {
    fn from(value: SqliteError) -> Self {
        CachedReplayError::Storage(value)
    }
}

pub struct ScannerHandle {
    cmd_tx: Sender<ScanCommand>,
    job_counter: Arc<AtomicU64>,
}

impl ScannerHandle {
    pub fn request_scan(&self, query: SearchQuery, cache: Option<CacheContext>) -> u64 {
        let job_id = self.job_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.cmd_tx.send(ScanCommand::Run {
            job_id,
            query,
            cache,
        });
        job_id
    }

    pub fn request_cache_clear(&self, ctx: CacheContext) -> u64 {
        let job_id = self.job_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.cmd_tx.send(ScanCommand::ClearCache { job_id, ctx });
        job_id
    }
}

pub enum ScanCommand {
    Run {
        job_id: u64,
        query: SearchQuery,
        cache: Option<CacheContext>,
    },
    ClearCache {
        job_id: u64,
        ctx: CacheContext,
    },
}

#[derive(Debug)]
pub enum ScanMessage {
    Begin {
        job_id: u64,
        root: PathBuf,
    },
    Entry {
        job_id: u64,
        entry: FileEntry,
    },
    Error {
        job_id: u64,
        path: PathBuf,
        message: String,
    },
    Stats {
        job_id: u64,
        stats: ScanStats,
    },
    CacheCleared {
        job_id: u64,
        root: PathBuf,
        cleared: bool,
    },
    Complete {
        job_id: u64,
    },
}

pub fn spawn() -> (ScannerHandle, Receiver<ScanMessage>) {
    let (cmd_tx, cmd_rx) = unbounded();
    let (msg_tx, msg_rx) = unbounded();
    let job_counter = Arc::new(AtomicU64::new(0));
    let worker_counter = job_counter.clone();
    let worker_cmd = cmd_rx.clone();

    thread::Builder::new()
        .name("disk-space-scanner".into())
        .spawn(move || worker_loop(worker_cmd, msg_tx, worker_counter))
        .expect("failed to spawn scanner thread");

    (
        ScannerHandle {
            cmd_tx,
            job_counter,
        },
        msg_rx,
    )
}

fn worker_loop(
    cmd_rx: Receiver<ScanCommand>,
    msg_tx: Sender<ScanMessage>,
    job_counter: Arc<AtomicU64>,
) {
    while let Ok(command) = cmd_rx.recv() {
        match command {
            ScanCommand::Run {
                job_id,
                query,
                cache,
            } => {
                let _ = msg_tx.send(ScanMessage::Begin {
                    job_id,
                    root: query.root.clone(),
                });
                let stats = run_scan(job_id, query, cache, &msg_tx, &job_counter);
                let _ = msg_tx.send(ScanMessage::Stats { job_id, stats });
                let _ = msg_tx.send(ScanMessage::Complete { job_id });
            }
            ScanCommand::ClearCache { job_id, ctx } => {
                let cleared = match ctx.cache.clear_root_path(&ctx.canonical_root) {
                    Ok(result) => result,
                    Err(err) => {
                        let _ = msg_tx.send(ScanMessage::Error {
                            job_id,
                            path: ctx.canonical_root.clone(),
                            message: format!("cache clear failed: {err}"),
                        });
                        false
                    }
                };
                let _ = msg_tx.send(ScanMessage::CacheCleared {
                    job_id,
                    root: ctx.canonical_root,
                    cleared,
                });
            }
        }
    }
}

fn run_scan(
    job_id: u64,
    query: SearchQuery,
    cache_ctx: Option<CacheContext>,
    msg_tx: &Sender<ScanMessage>,
    job_counter: &Arc<AtomicU64>,
) -> ScanStats {
    let matcher = compile_matcher(query.relative_pattern.as_deref());
    let size_filter = query.size_filter.clone();
    let mut session = cache_ctx
        .as_ref()
        .and_then(|ctx| ctx.cache.begin_scan(ctx.root_id).ok());

    let mut walker = WalkDir::new(&query.root).follow_links(false).into_iter();
    let mut dir_stack: Vec<DirectoryFrame> = Vec::new();
    let mut stats = ScanStats::default();
    let mut aborted = false;

    while let Some(entry_result) = walker.next() {
        if job_counter.load(Ordering::SeqCst) != job_id {
            aborted = true;
            break;
        }

        let entry = match entry_result {
            Ok(entry) => entry,
            Err(err) => {
                if let Some(path) = err.path() {
                    let _ = msg_tx.send(ScanMessage::Error {
                        job_id,
                        path: path.to_path_buf(),
                        message: err.to_string(),
                    });
                }
                stats.fs_errors += 1;
                continue;
            }
        };

        let path = entry.path().to_path_buf();
        let depth = entry.depth();

        while dir_stack.len() > depth {
            if let Some(frame) = dir_stack.pop() {
                if let Err(err) = finalize_directory(frame, dir_stack.last_mut(), session.as_mut())
                {
                    eprintln!("dusk cache finalize error: {err}");
                }
            }
        }

        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(err) => {
                let _ = msg_tx.send(ScanMessage::Error {
                    job_id,
                    path: path.clone(),
                    message: err.to_string(),
                });
                stats.fs_errors += 1;
                continue;
            }
        };

        let kind = if metadata.is_dir() {
            FileKind::Directory
        } else if metadata.is_file() {
            FileKind::File
        } else {
            continue;
        };

        let direct_size = if kind == FileKind::File {
            metadata.len()
        } else {
            0
        };
        let modified_ts = cache::timestamp_from_system(metadata.modified().ok());
        let created_ts = cache::timestamp_from_system(metadata.created().ok());

        let mut rel_path = None;
        let mut parent_rel = None;
        if let Some(ref ctx) = cache_ctx {
            let relative = relative_path(&ctx.canonical_root, &path);
            parent_rel = parent_relative(&relative);
            rel_path = Some(relative.clone());

            // Skip decision matrix: reuse the cached subtree when the entry is clean (`flags & 1 == 0`)
            // and the on-disk mtime matches what we stored previously. Any validation failure drops
            // back to a full walk and marks the ancestry dirty so subsequent scans re-evaluate.
            if kind == FileKind::Directory {
                if let Ok(Some(cached)) = ctx.cache.entry(ctx.root_id, &relative) {
                    let cached_mtime = cached.modified;
                    if cached.flags & 1 == 0 && cached_mtime == modified_ts {
                        let session_ptr =
                            session.as_mut().map(|sess| sess as *mut cache::ScanSession);
                        match emit_cached_subtree(
                            job_id,
                            ctx,
                            &relative,
                            session_ptr,
                            matcher.as_ref(),
                            size_filter.as_ref(),
                            msg_tx,
                        ) {
                            Ok(emit_stats) => {
                                stats.cached_dirs += emit_stats.directories as u64;
                                stats.cached_entries += emit_stats.entries as u64;
                                stats.cached_bytes += emit_stats.aggregate_size;
                                if let Some(parent) = dir_stack.last_mut() {
                                    parent.aggregate_size += emit_stats.aggregate_size;
                                }
                                walker.skip_current_dir();
                                continue;
                            }
                            Err(CachedReplayError::Cache(err)) => {
                                eprintln!("dusk cache validation failure: {err}");
                                let _ = ctx.cache.mark_ancestors_dirty(ctx.root_id, &relative);
                            }
                            Err(CachedReplayError::Storage(err)) => {
                                eprintln!("dusk cache replay error: {err}");
                            }
                        }
                    }
                }
            }
        }

        if !should_include(
            &path,
            kind,
            direct_size,
            matcher.as_ref(),
            &query.root,
            size_filter.as_ref(),
        ) {
            continue;
        }

        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| path.display().to_string());

        let entry = FileEntry::new(
            path.clone(),
            file_name,
            kind,
            direct_size,
            metadata.modified().ok(),
            metadata.created().ok(),
        );

        let _ = msg_tx.send(ScanMessage::Entry { job_id, entry });

        if let (Some(session), Some(rel)) = (session.as_mut(), rel_path.as_ref()) {
            let parent_ref = parent_rel.as_ref().map(|p| p.as_path());
            if let Err(err) = session.upsert_entry(
                rel,
                parent_ref,
                kind,
                direct_size,
                if kind == FileKind::File {
                    direct_size
                } else {
                    0
                },
                modified_ts,
                created_ts,
            ) {
                eprintln!("dusk cache upsert error: {err}");
            }
        }

        match kind {
            FileKind::File => {
                stats.files_scanned += 1;
                if let Some(parent) = dir_stack.last_mut() {
                    parent.aggregate_size += direct_size;
                }
            }
            FileKind::Directory => {
                stats.dirs_scanned += 1;
                if let Some(rel) = rel_path {
                    dir_stack.push(DirectoryFrame {
                        relative: rel,
                        parent: parent_rel,
                        direct_size,
                        aggregate_size: 0,
                        modified: modified_ts,
                        created: created_ts,
                    });
                }
            }
        }
    }

    if !aborted {
        while let Some(frame) = dir_stack.pop() {
            if let Err(err) = finalize_directory(frame, dir_stack.last_mut(), session.as_mut()) {
                eprintln!("dusk cache finalize error: {err}");
            }
        }
        if let Some(session) = session {
            if let Err(err) = session.finish() {
                eprintln!("dusk cache flush error: {err}");
            }
        }

        if let Some(ctx) = cache_ctx.as_ref() {
            match verify_cache_root(ctx) {
                Ok(_summary) => {}
                Err(err) => {
                    stats.cache_validation_errors += 1;
                    eprintln!("dusk cache validation error: {err}");
                    let _ = ctx.cache.mark_dirty(ctx.root_id, Path::new("."));
                }
            }
        }
    }

    eprintln!(
        "dusk scan stats job={job_id} aborted={aborted} files={} dirs={} cached_dirs={} cached_entries={} cached_bytes={} fs_errors={} cache_validation_errors={}",
        stats.files_scanned,
        stats.dirs_scanned,
        stats.cached_dirs,
        stats.cached_entries,
        stats.cached_bytes,
        stats.fs_errors,
        stats.cache_validation_errors
    );

    stats
}

fn emit_cached_subtree(
    job_id: u64,
    ctx: &CacheContext,
    relative: &Path,
    session_ptr: Option<*mut cache::ScanSession>,
    matcher: Option<&GlobSet>,
    size_filter: Option<&SizeFilter>,
    msg_tx: &Sender<ScanMessage>,
) -> Result<EmitStats, CachedReplayError> {
    let entry = ctx
        .cache
        .entry(ctx.root_id, relative)?
        .ok_or_else(|| CacheValidationError::MissingEntry(relative.to_path_buf()))?;

    let abs_path = absolute_from_relative(&ctx.canonical_root, &entry.path);
    let include = should_include(
        &abs_path,
        entry.kind,
        entry.direct_size,
        matcher,
        &ctx.canonical_root,
        size_filter,
    );

    let mut stats = EmitStats::default();

    if include {
        let file_name = abs_path
            .file_name()
            .and_then(|f| f.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| abs_path.display().to_string());

        let file_entry = FileEntry::new(
            abs_path.clone(),
            file_name,
            entry.kind,
            entry.direct_size,
            cache::timestamp_to_system(entry.modified),
            cache::timestamp_to_system(entry.created),
        );

        let _ = msg_tx.send(ScanMessage::Entry {
            job_id,
            entry: file_entry,
        });
    }

    if let Some(ptr) = session_ptr {
        unsafe {
            let parent_buf = entry.parent.clone();
            let parent_ref = parent_buf.as_deref();
            (*ptr).upsert_entry(
                &entry.path,
                parent_ref,
                entry.kind,
                entry.direct_size,
                entry.aggregate_size,
                entry.modified,
                entry.created,
            )?;
        }
    }

    let mut computed_total = entry.direct_size;

    if entry.kind == FileKind::Directory {
        stats.directories += 1;
        let children = ctx.cache.children_of(ctx.root_id, &entry.path)?;
        for child in children {
            let child_stats = emit_cached_subtree(
                job_id,
                ctx,
                &child.path,
                session_ptr,
                matcher,
                size_filter,
                msg_tx,
            )?;
            computed_total += child_stats.aggregate_size;
            stats.aggregate_size += child_stats.aggregate_size;
            stats.entries += child_stats.entries;
            stats.directories += child_stats.directories;
            stats.files += child_stats.files;
        }
    } else {
        stats.files += 1;
    }

    if computed_total != entry.aggregate_size {
        return Err(CacheValidationError::AggregateMismatch {
            path: entry.path,
            expected: computed_total,
            cached: entry.aggregate_size,
        }
        .into());
    }

    stats.aggregate_size = entry.aggregate_size;
    stats.entries += 1;

    Ok(stats)
}

fn verify_cache_root(ctx: &CacheContext) -> Result<AggregateSummary, CacheValidationError> {
    ctx.cache.validate_aggregate(ctx.root_id, Path::new("."))
}

fn finalize_directory(
    frame: DirectoryFrame,
    parent_frame: Option<&mut DirectoryFrame>,
    session: Option<&mut cache::ScanSession>,
) -> Result<(), SqliteError> {
    let DirectoryFrame {
        relative,
        parent,
        direct_size,
        aggregate_size,
        modified,
        created,
    } = frame;

    let total = aggregate_size + direct_size;

    if let Some(session) = session {
        let parent_ref = parent.as_deref();
        session.upsert_entry(
            &relative,
            parent_ref,
            FileKind::Directory,
            direct_size,
            total,
            modified,
            created,
        )?;
    }

    if let Some(parent_frame) = parent_frame {
        parent_frame.aggregate_size += total;
    }

    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> PathBuf {
    match path.strip_prefix(root) {
        Ok(rel) if rel.as_os_str().is_empty() => PathBuf::from("."),
        Ok(rel) => rel.to_path_buf(),
        Err(_) => PathBuf::from("."),
    }
}

fn absolute_from_relative(root: &Path, relative: &Path) -> PathBuf {
    if relative.as_os_str().is_empty() || relative == Path::new(".") {
        root.to_path_buf()
    } else {
        root.join(relative)
    }
}

fn parent_relative(path: &Path) -> Option<PathBuf> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        None
    } else if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            Some(PathBuf::from("."))
        } else {
            Some(parent.to_path_buf())
        }
    } else {
        Some(PathBuf::from("."))
    }
}

fn should_include(
    path: &Path,
    kind: FileKind,
    direct_size: u64,
    matcher: Option<&GlobSet>,
    root: &Path,
    size_filter: Option<&SizeFilter>,
) -> bool {
    if kind == FileKind::Directory {
        return true;
    }

    if let Some(filter) = size_filter {
        if !filter.matches(direct_size) {
            return false;
        }
    }

    if let Some(matcher) = matcher {
        let absolute = path.to_string_lossy();
        if matcher.is_match(absolute.as_ref()) {
            return true;
        }

        if let Ok(relative) = path.strip_prefix(root) {
            if !relative.as_os_str().is_empty() {
                if let Some(relative_str) = relative.to_str() {
                    if matcher.is_match(relative_str) {
                        return true;
                    }
                }
            }
        }

        false
    } else {
        true
    }
}

fn compile_matcher(pattern: Option<&str>) -> Option<GlobSet> {
    let pattern = pattern?;
    let pattern = if pattern.is_empty() { "**" } else { pattern };

    let mut builder = GlobSetBuilder::new();
    let glob = build_glob(pattern).ok()?;
    builder.add(glob);
    builder.build().ok()
}

fn build_glob(pattern: &str) -> Result<Glob, globset::Error> {
    let mut builder = GlobBuilder::new(pattern);
    builder.literal_separator(true);
    builder.build()
}
