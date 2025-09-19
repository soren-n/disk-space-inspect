use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::fs::{FileEntry, FileKind};

#[derive(Default)]
pub struct TreeStore {
    nodes: BTreeMap<PathBuf, TreeNode>,
}

#[derive(Debug, Clone)]
pub struct TreeNode {
    pub name: String,
    pub kind: FileKind,
    pub direct_size: u64,
    pub modified: Option<SystemTime>,
    pub created: Option<SystemTime>,
    pub children: BTreeSet<PathBuf>,
    pub contains_match: bool,
}

impl TreeStore {
    pub fn clear(&mut self) {
        self.nodes.clear();
    }

    pub fn upsert(&mut self, entry: FileEntry) {
        let path = entry.path.clone();
        let node = self
            .nodes
            .entry(path.clone())
            .or_insert_with(|| TreeNode::new(&entry));

        node.name = entry.file_name;
        node.kind = entry.kind;
        node.direct_size = entry.direct_size;
        node.modified = entry.modified;
        node.created = entry.created;

        if let Some(parent) = path.parent() {
            if let Some(parent_node) = self.nodes.get_mut(parent) {
                parent_node.children.insert(path.clone());
            }
        }

        if entry.kind == FileKind::File {
            self.mark_contains_match_upwards(&path);
        }
    }

    pub fn get(&self, path: &Path) -> Option<&TreeNode> {
        self.nodes.get(path)
    }

    pub fn children(&self, path: &Path) -> Vec<PathBuf> {
        self.nodes
            .get(path)
            .map(|node| node.children.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn aggregated_size_with_cache(
        &self,
        path: &Path,
        cache: &mut BTreeMap<PathBuf, u64>,
    ) -> u64 {
        if let Some(size) = cache.get(path) {
            return *size;
        }

        let Some(node) = self.nodes.get(path) else {
            return 0;
        };

        let mut total = node.direct_size;
        if node.kind == FileKind::Directory {
            for child in &node.children {
                total += self.aggregated_size_with_cache(child, cache);
            }
        }

        cache.insert(path.to_path_buf(), total);
        total
    }

    fn mark_contains_match_upwards(&mut self, start: &Path) {
        let mut current = Some(start.to_path_buf());
        while let Some(path) = current {
            let parent = path.parent().map(|p| p.to_path_buf());
            if let Some(node) = self.nodes.get_mut(&path) {
                if !node.contains_match {
                    node.contains_match = true;
                }
            }
            current = parent;
        }
    }
}

impl TreeNode {
    fn new(entry: &FileEntry) -> Self {
        Self {
            name: entry.file_name.clone(),
            kind: entry.kind,
            direct_size: entry.direct_size,
            modified: entry.modified,
            created: entry.created,
            children: BTreeSet::new(),
            contains_match: entry.kind == FileKind::File,
        }
    }
}
