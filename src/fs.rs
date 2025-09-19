use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FileKind {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: PathBuf,
    pub file_name: String,
    pub kind: FileKind,
    pub direct_size: u64,
    pub modified: Option<SystemTime>,
    pub created: Option<SystemTime>,
}

impl FileEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: PathBuf,
        file_name: String,
        kind: FileKind,
        direct_size: u64,
        modified: Option<SystemTime>,
        created: Option<SystemTime>,
    ) -> Self {
        Self {
            path,
            file_name,
            kind,
            direct_size,
            modified,
            created,
        }
    }
}
