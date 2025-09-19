use std::env;
use std::path::{Component, MAIN_SEPARATOR, Path, PathBuf};

use shellexpand::tilde;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeOperator {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, Clone)]
pub struct SizeFilter {
    pub operator: SizeOperator,
    pub bytes: u64,
}

impl SizeFilter {
    pub fn matches(&self, size: u64) -> bool {
        match self.operator {
            SizeOperator::GreaterThan => size > self.bytes,
            SizeOperator::GreaterThanOrEqual => size >= self.bytes,
            SizeOperator::LessThan => size < self.bytes,
            SizeOperator::LessThanOrEqual => size <= self.bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchQuery {
    pub raw: String,
    pub root: PathBuf,
    pub relative_pattern: Option<String>,
    pub size_filter: Option<SizeFilter>,
}

impl Default for SearchQuery {
    fn default() -> Self {
        let root = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        Self {
            raw: String::new(),
            root,
            relative_pattern: None,
            size_filter: None,
        }
    }
}

pub fn parse_input(input: &str) -> SearchQuery {
    let mut query = SearchQuery::default();
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return query;
    }

    let base_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut tokens = trimmed.split_whitespace().peekable();
    let mut pattern: Option<String> = None;
    let mut size_filter: Option<SizeFilter> = None;

    while let Some(token) = tokens.next() {
        if is_comparison_prefix(token) {
            if let Some(next) = tokens.next() {
                if let Some(filter) = parse_size_filter_parts(token, next) {
                    size_filter = Some(filter);
                }
            }
            continue;
        }

        if let Some(filter) = parse_size_filter(token) {
            size_filter = Some(filter);
            continue;
        }

        if pattern.is_none() {
            pattern = Some(token.to_string());
            continue;
        }
    }

    if let Some(pat) = pattern {
        let expanded = expand_tilde(&pat);
        let (root, relative) = split_pattern(&expanded);
        query.root = normalize_root_path(root, &base_dir);
        query.relative_pattern = relative;
        query.raw = trimmed.to_string();
    } else {
        query.raw = trimmed.to_string();
    }

    query.size_filter = size_filter;
    query
}

fn expand_tilde(input: &str) -> String {
    tilde(input).into_owned()
}

fn split_pattern(pattern: &str) -> (PathBuf, Option<String>) {
    if pattern.is_empty() {
        let root = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        return (root, None);
    }

    let wildcard_index = pattern
        .char_indices()
        .find(|&(_, ch)| matches!(ch, '*' | '?' | '['))
        .map(|(idx, _)| idx);

    let root_candidate = match wildcard_index {
        Some(idx) => &pattern[..idx],
        None => pattern,
    };

    let mut root_str = root_candidate.trim_end_matches(MAIN_SEPARATOR);
    if root_str.is_empty() {
        root_str = if pattern.starts_with(MAIN_SEPARATOR) {
            "/"
        } else {
            "."
        };
    }

    let root_path = PathBuf::from(root_str);

    let relative_pattern = match wildcard_index {
        Some(idx) => {
            let remainder = pattern[idx..].trim_start_matches(MAIN_SEPARATOR);
            if remainder.is_empty() {
                Some("**".to_string())
            } else {
                Some(remainder.to_string())
            }
        }
        None => None,
    };

    (root_path, relative_pattern)
}

fn normalize_root_path(root: PathBuf, base_dir: &PathBuf) -> PathBuf {
    if root.as_os_str().is_empty() {
        return base_dir.clone();
    }

    let rebased = if root.is_absolute() {
        if let Ok(rel) = root.strip_prefix(base_dir) {
            rebase_path(base_dir, rel)
        } else {
            rebase_path(base_dir, root.as_path())
        }
    } else {
        rebase_path(base_dir, root.as_path())
    };

    if rebased.starts_with(base_dir) {
        rebased
    } else {
        base_dir.clone()
    }
}

fn rebase_path(base: &Path, input: &Path) -> PathBuf {
    let mut result = base.to_path_buf();
    for component in input.components() {
        match component {
            Component::RootDir | Component::Prefix(_) => continue,
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            Component::Normal(part) => result.push(part),
        }
    }
    result
}

fn is_comparison_prefix(token: &str) -> bool {
    matches!(token, ">" | "<" | ">=" | "<=")
}

fn parse_size_filter(token: &str) -> Option<SizeFilter> {
    if token.len() < 2 {
        return None;
    }

    let mut chars = token.chars();
    let first = chars.next()?;
    let second = chars.next();

    let (operator, rest) = match (first, second) {
        ('>', Some('=')) => (SizeOperator::GreaterThanOrEqual, token[2..].trim()),
        ('<', Some('=')) => (SizeOperator::LessThanOrEqual, token[2..].trim()),
        ('>', _) => (SizeOperator::GreaterThan, token[1..].trim()),
        ('<', _) => (SizeOperator::LessThan, token[1..].trim()),
        _ => return None,
    };

    parse_size_value(rest).map(|bytes| SizeFilter { operator, bytes })
}

fn parse_size_filter_parts(op: &str, value: &str) -> Option<SizeFilter> {
    let operator = match op {
        ">" => SizeOperator::GreaterThan,
        ">=" => SizeOperator::GreaterThanOrEqual,
        "<" => SizeOperator::LessThan,
        "<=" => SizeOperator::LessThanOrEqual,
        _ => return None,
    };

    parse_size_value(value).map(|bytes| SizeFilter { operator, bytes })
}

fn parse_size_value(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let split_index = trimmed
        .char_indices()
        .find(|&(_, ch)| !ch.is_ascii_digit() && ch != '.')
        .map(|(idx, _)| idx)
        .unwrap_or(trimmed.len());

    let (number_str, unit_str) = trimmed.split_at(split_index);
    let number: f64 = number_str.parse().ok()?;
    let multiplier = match unit_str.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "kb" | "kib" => 1024.0,
        "mb" | "mib" => 1024.0_f64.powi(2),
        "gb" | "gib" => 1024.0_f64.powi(3),
        "tb" | "tib" => 1024.0_f64.powi(4),
        _ => return None,
    };

    Some((number * multiplier).round() as u64)
}
