use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use dirs::cache_dir;
use rusqlite::{Connection, OptionalExtension, params};

use crate::fs::FileKind;

const CACHE_SCHEMA_VERSION: i64 = 1;
const CACHE_USER_VERSION: i32 = 1;
const CACHE_MIGRATIONS: &[(i32, &str)] = &[];
const CACHE_MAX_AGE: Duration = Duration::from_secs(60 * 60 * 24 * 30); // 30 days
const CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024; // 512 MB safety ceiling

#[derive(Clone, Debug)]
pub struct CachedEntry {
    pub path: PathBuf,
    pub parent: Option<PathBuf>,
    pub kind: FileKind,
    pub direct_size: u64,
    pub aggregate_size: u64,
    pub modified: Option<i64>,
    pub created: Option<i64>,
    pub flags: i64,
}

#[derive(Clone)]
pub struct Cache {
    db_path: PathBuf,
}

pub struct RootCache {
    pub root_id: i64,
    pub entries: Vec<CachedEntry>,
}

pub struct ScanSession {
    conn: Connection,
    root_id: i64,
    scan_ts: i64,
    db_path: PathBuf,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AggregateSummary {
    pub entry_count: usize,
    pub directory_count: usize,
    pub total_size: u64,
}

#[derive(Debug)]
pub enum CacheValidationError {
    MissingEntry(PathBuf),
    AggregateMismatch {
        path: PathBuf,
        expected: u64,
        cached: u64,
    },
    Sqlite(rusqlite::Error),
}

impl std::fmt::Display for CacheValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheValidationError::MissingEntry(path) => {
                write!(f, "missing cache entry for {}", path.display())
            }
            CacheValidationError::AggregateMismatch {
                path,
                expected,
                cached,
            } => {
                write!(
                    f,
                    "aggregate mismatch for {} (expected {}, cached {})",
                    path.display(),
                    expected,
                    cached
                )
            }
            CacheValidationError::Sqlite(err) => write!(f, "sqlite error: {err}"),
        }
    }
}

impl std::error::Error for CacheValidationError {}

impl From<rusqlite::Error> for CacheValidationError {
    fn from(value: rusqlite::Error) -> Self {
        CacheValidationError::Sqlite(value)
    }
}

impl Cache {
    pub fn open() -> rusqlite::Result<Self> {
        let mut base = cache_dir().unwrap_or_else(|| PathBuf::from("."));
        base.push("dusk");
        fs::create_dir_all(&base).ok();
        base.push("dusk.sqlite");
        let cache = Self { db_path: base };
        cache.initialize_schema()?;
        Ok(cache)
    }

    pub fn open_in_path(db_path: PathBuf) -> rusqlite::Result<Self> {
        let cache = Self { db_path };
        cache.initialize_schema()?;
        Ok(cache)
    }

    pub fn resolve_root(&self, canonical_root: &Path) -> rusqlite::Result<i64> {
        let root_str = canonical_root.to_string_lossy();
        let conn = self.connection()?;
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT OR IGNORE INTO roots (
                canonical_root, last_scan_utc, schema_version, scan_count, last_pruned_utc
            ) VALUES (?1, ?2, ?3, 0, 0)",
            params![root_str.as_ref(), now, CACHE_SCHEMA_VERSION],
        )?;
        conn.execute(
            "UPDATE roots SET schema_version = ?1 WHERE canonical_root = ?2",
            params![CACHE_SCHEMA_VERSION, root_str.as_ref()],
        )?;
        conn.query_row(
            "SELECT id FROM roots WHERE canonical_root = ?1",
            params![root_str.as_ref()],
            |row| row.get(0),
        )
    }

    pub fn load_root(&self, canonical_root: &Path) -> rusqlite::Result<RootCache> {
        let root_id = self.resolve_root(canonical_root)?;
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT path, parent, kind, direct_size, aggregate_size, mtime_utc, ctime_utc, flags \
             FROM entries WHERE root_id = ?1",
        )?;
        let rows = stmt.query_map(params![root_id], |row| {
            let path: String = row.get(0)?;
            let parent: Option<String> = row.get(1)?;
            let kind: i64 = row.get(2)?;
            let direct_size: i64 = row.get(3)?;
            let aggregate_size: i64 = row.get(4)?;
            let modified: Option<i64> = row.get(5)?;
            let created: Option<i64> = row.get(6)?;
            let flags: i64 = row.get(7)?;

            Ok(CachedEntry {
                path: PathBuf::from(path),
                parent: parent.map(PathBuf::from),
                kind: if kind == 0 {
                    FileKind::File
                } else {
                    FileKind::Directory
                },
                direct_size: direct_size as u64,
                aggregate_size: aggregate_size as u64,
                modified,
                created,
                flags,
            })
        })?;

        let mut entries = Vec::new();
        for entry in rows {
            entries.push(entry?);
        }

        Ok(RootCache { root_id, entries })
    }

    pub fn clear_root_path(&self, canonical_root: &Path) -> rusqlite::Result<bool> {
        let root_str = canonical_root.to_string_lossy();
        let conn = self.connection()?;
        let mut stmt = conn.prepare("SELECT id FROM roots WHERE canonical_root = ?1")?;
        let root_id: Option<i64> = stmt
            .query_row(params![root_str.as_ref()], |row| row.get(0))
            .optional()?;

        let Some(root_id) = root_id else {
            return Ok(false);
        };

        conn.execute("DELETE FROM entries WHERE root_id = ?1", params![root_id])?;
        conn.execute("DELETE FROM ui_state WHERE root_id = ?1", params![root_id])?;
        let affected = conn.execute("DELETE FROM roots WHERE id = ?1", params![root_id])?;
        Ok(affected > 0)
    }

    pub fn mark_ancestors_dirty(&self, root_id: i64, relative: &Path) -> rusqlite::Result<()> {
        let conn = self.connection()?;
        let mut current = Some(relative.to_path_buf());
        while let Some(path) = current {
            let rel = path.to_string_lossy();
            conn.execute(
                "UPDATE entries SET flags = flags | 1 WHERE root_id = ?1 AND path = ?2",
                params![root_id, rel.as_ref()],
            )?;
            current = parent_relative(&path);
        }
        Ok(())
    }

    pub fn validate_aggregate(
        &self,
        root_id: i64,
        relative: &Path,
    ) -> Result<AggregateSummary, CacheValidationError> {
        let conn = self.connection()?;
        let entry = Self::fetch_entry(&conn, root_id, relative)?
            .ok_or_else(|| CacheValidationError::MissingEntry(relative.to_path_buf()))?;

        self.verify_entry_with_conn(&conn, root_id, entry)
    }

    fn verify_entry_with_conn(
        &self,
        conn: &Connection,
        root_id: i64,
        entry: CachedEntry,
    ) -> Result<AggregateSummary, CacheValidationError> {
        let mut summary = AggregateSummary {
            entry_count: 1,
            directory_count: usize::from(entry.kind == FileKind::Directory),
            total_size: entry.direct_size,
        };

        if entry.kind == FileKind::Directory {
            let children = Self::fetch_children(conn, root_id, &entry.path)?;
            let mut child_total = 0;
            for child in children {
                let child_summary = self.verify_entry_with_conn(conn, root_id, child)?;
                summary.entry_count += child_summary.entry_count;
                summary.directory_count += child_summary.directory_count;
                child_total += child_summary.total_size;
            }
            summary.total_size += child_total;
        }

        if entry.aggregate_size != summary.total_size {
            return Err(CacheValidationError::AggregateMismatch {
                path: entry.path,
                expected: summary.total_size,
                cached: entry.aggregate_size,
            });
        }

        summary.total_size = entry.aggregate_size;
        Ok(summary)
    }

    pub fn load_ui_state(&self, root_id: i64) -> rusqlite::Result<Option<(String, i64)>> {
        let conn = self.connection()?;
        conn.query_row(
            "SELECT state_json, state_version FROM ui_state WHERE root_id = ?1",
            params![root_id],
            |row| {
                let json: String = row.get(0)?;
                let version: i64 = row.get(1)?;
                Ok((json, version))
            },
        )
        .optional()
    }

    pub fn save_ui_state(
        &self,
        root_id: i64,
        state_json: &str,
        state_version: i64,
    ) -> rusqlite::Result<()> {
        let conn = self.connection()?;
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO ui_state (root_id, state_json, state_version, updated_utc)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(root_id) DO UPDATE SET
                state_json = excluded.state_json,
                state_version = excluded.state_version,
                updated_utc = excluded.updated_utc",
            params![root_id, state_json, state_version, now],
        )?;
        Ok(())
    }

    pub fn remove_entry(&self, root_id: i64, relative: &Path) -> rusqlite::Result<()> {
        let conn = self.connection()?;
        let rel = relative.to_string_lossy();
        conn.execute(
            "DELETE FROM entries WHERE root_id = ?1 AND path = ?2",
            params![root_id, rel.as_ref()],
        )?;
        Ok(())
    }

    pub fn mark_dirty(&self, root_id: i64, relative: &Path) -> rusqlite::Result<()> {
        let conn = self.connection()?;
        let rel = relative.to_string_lossy();
        conn.execute(
            "UPDATE entries SET flags = flags | 1 WHERE root_id = ?1 AND path = ?2",
            params![root_id, rel.as_ref()],
        )?;
        Ok(())
    }

    pub fn entry(&self, root_id: i64, relative: &Path) -> rusqlite::Result<Option<CachedEntry>> {
        let conn = self.connection()?;
        Self::fetch_entry(&conn, root_id, relative)
    }

    pub fn children_of(&self, root_id: i64, parent: &Path) -> rusqlite::Result<Vec<CachedEntry>> {
        let conn = self.connection()?;
        Self::fetch_children(&conn, root_id, parent)
    }

    fn fetch_entry(
        conn: &Connection,
        root_id: i64,
        relative: &Path,
    ) -> rusqlite::Result<Option<CachedEntry>> {
        let rel = relative.to_string_lossy();
        conn.query_row(
            "SELECT path, parent, kind, direct_size, aggregate_size, mtime_utc, ctime_utc, flags \
             FROM entries WHERE root_id = ?1 AND path = ?2",
            params![root_id, rel.as_ref()],
            |row| Self::map_cached_entry(row),
        )
        .optional()
    }

    fn fetch_children(
        conn: &Connection,
        root_id: i64,
        parent: &Path,
    ) -> rusqlite::Result<Vec<CachedEntry>> {
        let parent_str = if parent.as_os_str().is_empty() {
            None
        } else {
            Some(parent.to_string_lossy().to_string())
        };
        let mut stmt = conn.prepare(
            "SELECT path, parent, kind, direct_size, aggregate_size, mtime_utc, ctime_utc, flags \
             FROM entries WHERE root_id = ?1 AND parent IS ?2",
        )?;
        let rows = stmt.query_map(params![root_id, parent_str], |row| {
            Self::map_cached_entry(row)
        })?;

        let mut entries = Vec::new();
        for entry in rows {
            entries.push(entry?);
        }
        Ok(entries)
    }

    fn map_cached_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<CachedEntry> {
        let path: String = row.get(0)?;
        let parent: Option<String> = row.get(1)?;
        let kind: i64 = row.get(2)?;
        let direct_size: i64 = row.get(3)?;
        let aggregate_size: i64 = row.get(4)?;
        let modified: Option<i64> = row.get(5)?;
        let created: Option<i64> = row.get(6)?;
        let flags: i64 = row.get(7)?;

        Ok(CachedEntry {
            path: PathBuf::from(path),
            parent: parent.map(PathBuf::from),
            kind: if kind == 0 {
                FileKind::File
            } else {
                FileKind::Directory
            },
            direct_size: direct_size as u64,
            aggregate_size: aggregate_size as u64,
            modified,
            created,
            flags,
        })
    }

    fn initialize_schema(&self) -> rusqlite::Result<()> {
        let conn = Connection::open(&self.db_path)?;
        Self::configure_connection(&conn)?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS roots (
                id INTEGER PRIMARY KEY,
                canonical_root TEXT UNIQUE NOT NULL,
                last_scan_utc INTEGER NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 0,
                scan_count INTEGER NOT NULL DEFAULT 0,
                last_pruned_utc INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS entries (
                root_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                parent TEXT,
                kind INTEGER NOT NULL,
                direct_size INTEGER NOT NULL,
                aggregate_size INTEGER NOT NULL,
                mtime_utc INTEGER,
                ctime_utc INTEGER,
                last_seen_utc INTEGER NOT NULL,
                flags INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(root_id, path),
                FOREIGN KEY(root_id) REFERENCES roots(id)
            );
            CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(root_id, parent);
            CREATE TABLE IF NOT EXISTS ui_state (
                root_id INTEGER PRIMARY KEY,
                state_json TEXT NOT NULL,
                state_version INTEGER NOT NULL,
                updated_utc INTEGER NOT NULL,
                FOREIGN KEY(root_id) REFERENCES roots(id)
            );
            "#,
        )?;
        Self::upgrade_schema(&conn)?;
        Self::apply_global_migrations(&conn)?;
        Ok(())
    }

    fn connection(&self) -> rusqlite::Result<Connection> {
        let conn = Connection::open(&self.db_path)?;
        Self::configure_connection(&conn)?;
        Ok(conn)
    }

    fn configure_connection(conn: &Connection) -> rusqlite::Result<()> {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;",
        )?;
        Ok(())
    }

    fn upgrade_schema(conn: &Connection) -> rusqlite::Result<()> {
        let mut stmt = conn.prepare("PRAGMA table_info(roots)")?;
        let existing: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<_, _>>()?;

        if !existing.iter().any(|c| c == "schema_version") {
            conn.execute(
                "ALTER TABLE roots ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        if !existing.iter().any(|c| c == "scan_count") {
            conn.execute(
                "ALTER TABLE roots ADD COLUMN scan_count INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        if !existing.iter().any(|c| c == "last_pruned_utc") {
            conn.execute(
                "ALTER TABLE roots ADD COLUMN last_pruned_utc INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        conn.execute(
            "UPDATE roots SET schema_version = ?1 WHERE schema_version <> ?1",
            params![CACHE_SCHEMA_VERSION],
        )?;

        let mut stmt = conn.prepare("PRAGMA table_info(ui_state)")?;
        let _ = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    fn apply_global_migrations(conn: &Connection) -> rusqlite::Result<()> {
        let mut current: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        for (version, sql) in CACHE_MIGRATIONS {
            if *version > current {
                conn.execute_batch(sql)?;
                conn.pragma_update(None, "user_version", *version)?;
                current = *version;
            }
        }

        if current < CACHE_USER_VERSION {
            conn.pragma_update(None, "user_version", CACHE_USER_VERSION)?;
        }

        Ok(())
    }

    pub fn begin_scan(&self, root_id: i64) -> rusqlite::Result<ScanSession> {
        let conn = self.connection()?;
        Ok(ScanSession {
            conn,
            root_id,
            scan_ts: Utc::now().timestamp(),
            db_path: self.db_path.clone(),
        })
    }
}

impl ScanSession {
    pub fn upsert_entry(
        &mut self,
        relative: &Path,
        parent: Option<&Path>,
        kind: FileKind,
        direct_size: u64,
        aggregate_size: u64,
        modified: Option<i64>,
        created: Option<i64>,
    ) -> rusqlite::Result<()> {
        let path = relative.to_string_lossy();
        let parent = parent.map(|p| p.to_string_lossy().to_string());
        let kind_val = match kind {
            FileKind::File => 0,
            FileKind::Directory => 1,
        };

        self.conn.execute(
            "INSERT INTO entries (
                root_id, path, parent, kind, direct_size, aggregate_size,
                mtime_utc, ctime_utc, last_seen_utc
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(root_id, path) DO UPDATE SET
                parent = excluded.parent,
                kind = excluded.kind,
                direct_size = excluded.direct_size,
                aggregate_size = excluded.aggregate_size,
                mtime_utc = excluded.mtime_utc,
                ctime_utc = excluded.ctime_utc,
                last_seen_utc = excluded.last_seen_utc,
                flags = 0",
            params![
                self.root_id,
                path.as_ref(),
                parent,
                kind_val,
                direct_size as i64,
                aggregate_size as i64,
                modified,
                created,
                self.scan_ts,
            ],
        )?;
        Ok(())
    }

    pub fn finish(self) -> rusqlite::Result<()> {
        let Self {
            mut conn,
            root_id,
            scan_ts,
            db_path,
        } = self;

        conn.execute(
            "DELETE FROM entries WHERE root_id = ?1 AND last_seen_utc <> ?2",
            params![root_id, scan_ts],
        )?;
        conn.execute(
            "UPDATE roots SET last_scan_utc = ?1, scan_count = scan_count + 1 WHERE id = ?2",
            params![scan_ts, root_id],
        )?;
        Self::prune_if_needed(&mut conn, root_id, scan_ts, &db_path)?;
        Ok(())
    }

    fn prune_if_needed(
        conn: &mut Connection,
        root_id: i64,
        scan_ts: i64,
        db_path: &Path,
    ) -> rusqlite::Result<()> {
        let (last_pruned, scan_count): (i64, i64) = conn.query_row(
            "SELECT last_pruned_utc, scan_count FROM roots WHERE id = ?1",
            params![root_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        let elapsed = scan_ts.saturating_sub(last_pruned);
        let should_prune = scan_ts <= last_pruned || elapsed >= 3600 || scan_count % 5 == 0;
        if !should_prune {
            return Ok(());
        }

        let cutoff = scan_ts - CACHE_MAX_AGE.as_secs() as i64;
        conn.execute(
            "DELETE FROM entries WHERE root_id = ?1 AND last_seen_utc < ?2",
            params![root_id, cutoff],
        )?;

        let mut db_size = fs::metadata(db_path).map(|meta| meta.len()).unwrap_or(0);
        if db_size > CACHE_MAX_BYTES {
            loop {
                let removed = conn.execute(
                    "DELETE FROM entries WHERE rowid IN (
                        SELECT rowid FROM entries
                        WHERE root_id = ?1
                        ORDER BY last_seen_utc ASC
                        LIMIT 512
                    )",
                    params![root_id],
                )?;

                if removed == 0 {
                    break;
                }

                db_size = fs::metadata(db_path).map(|meta| meta.len()).unwrap_or(0);
                if db_size <= CACHE_MAX_BYTES {
                    break;
                }
            }
        }

        conn.execute(
            "UPDATE roots SET last_pruned_utc = ?1 WHERE id = ?2",
            params![scan_ts, root_id],
        )?;
        Ok(())
    }
}

pub fn timestamp_from_system(time: Option<std::time::SystemTime>) -> Option<i64> {
    time.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
}

pub fn timestamp_to_system(ts: Option<i64>) -> Option<std::time::SystemTime> {
    ts.map(|secs| std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs as u64))
}

fn parent_relative(path: &Path) -> Option<PathBuf> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return None;
    }

    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            Some(PathBuf::from("."))
        } else {
            Some(parent.to_path_buf())
        }
    } else {
        Some(PathBuf::from("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_cache() -> (Cache, TempDir, i64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("cache.sqlite");
        let cache = Cache::open_in_path(db_path).expect("open cache");
        let canonical_root = dir.path().canonicalize().expect("canonical root");
        let root_id = cache.resolve_root(&canonical_root).expect("resolve root");
        (cache, dir, root_id)
    }

    #[test]
    fn mark_ancestors_dirty_marks_full_chain() {
        let (cache, _dir, root_id) = temp_cache();
        let mut session = cache.begin_scan(root_id).expect("begin scan");

        session
            .upsert_entry(Path::new("."), None, FileKind::Directory, 0, 0, None, None)
            .expect("root upsert");
        session
            .upsert_entry(
                Path::new("dir"),
                Some(Path::new(".")),
                FileKind::Directory,
                0,
                0,
                None,
                None,
            )
            .expect("dir upsert");
        session
            .upsert_entry(
                Path::new("dir/sub"),
                Some(Path::new("dir")),
                FileKind::Directory,
                0,
                0,
                None,
                None,
            )
            .expect("sub upsert");
        session
            .upsert_entry(
                Path::new("dir/sub/file.txt"),
                Some(Path::new("dir/sub")),
                FileKind::File,
                42,
                42,
                None,
                None,
            )
            .expect("file upsert");
        session.finish().expect("finish");

        cache
            .mark_ancestors_dirty(root_id, Path::new("dir/sub/file.txt"))
            .expect("mark dirty");

        let file_flags = cache
            .entry(root_id, Path::new("dir/sub/file.txt"))
            .unwrap()
            .unwrap()
            .flags;
        let sub_flags = cache
            .entry(root_id, Path::new("dir/sub"))
            .unwrap()
            .unwrap()
            .flags;
        let dir_flags = cache
            .entry(root_id, Path::new("dir"))
            .unwrap()
            .unwrap()
            .flags;
        let root_flags = cache.entry(root_id, Path::new(".")).unwrap().unwrap().flags;

        assert_ne!(file_flags & 1, 0, "file flagged");
        assert_ne!(sub_flags & 1, 0, "subdir flagged");
        assert_ne!(dir_flags & 1, 0, "dir flagged");
        assert_ne!(root_flags & 1, 0, "root flagged");
    }

    #[test]
    fn mark_ancestors_dirty_on_directory_marks_root() {
        let (cache, _dir, root_id) = temp_cache();
        let mut session = cache.begin_scan(root_id).expect("begin scan");

        session
            .upsert_entry(Path::new("."), None, FileKind::Directory, 0, 0, None, None)
            .expect("root upsert");
        session
            .upsert_entry(
                Path::new("dir"),
                Some(Path::new(".")),
                FileKind::Directory,
                0,
                0,
                None,
                None,
            )
            .expect("dir upsert");
        session
            .upsert_entry(
                Path::new("dir/sub"),
                Some(Path::new("dir")),
                FileKind::Directory,
                0,
                0,
                None,
                None,
            )
            .expect("sub upsert");
        session.finish().expect("finish");

        cache
            .mark_ancestors_dirty(root_id, Path::new("dir/sub"))
            .expect("mark dirty");

        let sub_flags = cache
            .entry(root_id, Path::new("dir/sub"))
            .unwrap()
            .unwrap()
            .flags;
        let dir_flags = cache
            .entry(root_id, Path::new("dir"))
            .unwrap()
            .unwrap()
            .flags;
        let root_flags = cache.entry(root_id, Path::new(".")).unwrap().unwrap().flags;

        assert_ne!(sub_flags & 1, 0, "subdir flagged");
        assert_ne!(dir_flags & 1, 0, "dir flagged");
        assert_ne!(root_flags & 1, 0, "root flagged");
    }

    #[test]
    fn mark_dirty_after_remove_marks_parent() {
        let (cache, _dir, root_id) = temp_cache();
        let mut session = cache.begin_scan(root_id).expect("begin scan");

        session
            .upsert_entry(Path::new("."), None, FileKind::Directory, 0, 0, None, None)
            .expect("root upsert");
        session
            .upsert_entry(
                Path::new("dir"),
                Some(Path::new(".")),
                FileKind::Directory,
                0,
                0,
                None,
                None,
            )
            .expect("dir upsert");
        session
            .upsert_entry(
                Path::new("dir/file.txt"),
                Some(Path::new("dir")),
                FileKind::File,
                10,
                10,
                None,
                None,
            )
            .expect("file upsert");
        session.finish().expect("finish");

        cache
            .remove_entry(root_id, Path::new("dir/file.txt"))
            .expect("remove entry");
        cache
            .mark_ancestors_dirty(root_id, Path::new("dir"))
            .expect("mark dir dirty");

        let dir_flags = cache
            .entry(root_id, Path::new("dir"))
            .unwrap()
            .unwrap()
            .flags;
        let root_flags = cache.entry(root_id, Path::new(".")).unwrap().unwrap().flags;

        assert_ne!(dir_flags & 1, 0, "dir flagged");
        assert_ne!(root_flags & 1, 0, "root flagged");
    }

    #[test]
    fn validate_aggregate_detects_mismatch() {
        let (cache, _dir, root_id) = temp_cache();
        let mut session = cache.begin_scan(root_id).expect("begin scan");

        session
            .upsert_entry(
                Path::new("."),
                None,
                FileKind::Directory,
                0,
                999,
                None,
                None,
            )
            .expect("root upsert");
        session
            .upsert_entry(
                Path::new("dir"),
                Some(Path::new(".")),
                FileKind::Directory,
                0,
                100,
                None,
                None,
            )
            .expect("dir upsert");
        session
            .upsert_entry(
                Path::new("dir/file.bin"),
                Some(Path::new("dir")),
                FileKind::File,
                100,
                100,
                None,
                None,
            )
            .expect("file upsert");
        session.finish().expect("finish");

        let error = cache
            .validate_aggregate(root_id, Path::new("."))
            .expect_err("expected mismatch");

        match error {
            CacheValidationError::AggregateMismatch { path, .. } => {
                assert_eq!(path, Path::new("."));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
