use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use eframe::egui::{self, Align, Layout};
use egui_extras::{Column, TableBuilder};
use serde::{Deserialize, Serialize};

use crate::cache::{self, Cache, RootCache};
use crate::fs::{FileEntry, FileKind};
use crate::query::{SearchQuery, parse_input};
use crate::scanner::{CacheContext, ScanMessage, ScanStats, ScannerHandle, spawn};
use crate::tree::TreeStore;
use crate::util::{format_size, format_system_time};
use crate::watcher::{self, WatchEventKind, WatchHandle};

const COLUMN_COUNT: usize = 6;
const DEFAULT_COLUMN_WIDTHS: [f32; COLUMN_COUNT] = [32.0, 260.0, 110.0, 130.0, 150.0, 150.0];
const COLUMN_LABELS: [&str; COLUMN_COUNT] =
    ["Stage", "Name", "Size", "Total", "Modified", "Created"];

pub struct DiskSpaceApp {
    scanner: ScannerHandle,
    scan_rx: Receiver<ScanMessage>,
    tree: TreeStore,
    search_input: String,
    status_text: Option<String>,
    last_error: Option<String>,
    active_job_id: Option<u64>,
    pending_job_id: Option<u64>,
    pending_clear_job_id: Option<u64>,
    active_root: Option<PathBuf>,
    expanded: BTreeSet<PathBuf>,
    entries_seen: usize,
    current_query: SearchQuery,
    staged: BTreeSet<PathBuf>,
    show_commit_modal: bool,
    cache: Cache,
    cache_root_id: i64,
    canonical_root: PathBuf,
    last_stats: Option<ScanStats>,
    watch_enabled: bool,
    watch_handle: Option<WatchHandle>,
    watch_rescan_due: bool,
    ui_state_dirty: bool,
    ui_state_next_save: Option<Instant>,
    watcher_config: watcher::WatcherConfig,
    sort_mode: SortMode,
    column_widths: [f32; COLUMN_COUNT],
    show_layout_modal: bool,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub enable_watchers: bool,
    pub watcher_config: watcher::WatcherConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            enable_watchers: false,
            watcher_config: watcher::WatcherConfig::default(),
        }
    }
}

const UI_STATE_VERSION: i64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SortMode {
    #[default]
    NameAsc,
    SizeDesc,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedUiState {
    version: u32,
    expanded: Vec<String>,
    staged: Vec<String>,
    watch_enabled: bool,
    sort_mode: SortMode,
    column_widths: Vec<f32>,
}

impl DiskSpaceApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        Self::with_config(cc, AppConfig::default())
    }

    pub fn with_config(_cc: &eframe::CreationContext<'_>, config: AppConfig) -> Self {
        let (scanner, scan_rx) = spawn();
        let mut initial_query = SearchQuery::default();
        let cache = Cache::open().expect("failed to open cache");
        let canonical_root = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .canonicalize()
            .unwrap_or_else(|_| PathBuf::from("."));
        let root_cache = cache
            .load_root(&canonical_root)
            .expect("failed to load cache entries");

        initial_query.root = canonical_root.clone();

        let mut app = Self {
            scanner,
            scan_rx,
            tree: TreeStore::default(),
            search_input: initial_query.raw.clone(),
            status_text: None,
            last_error: None,
            active_job_id: None,
            pending_job_id: None,
            pending_clear_job_id: None,
            active_root: None,
            expanded: BTreeSet::new(),
            entries_seen: 0,
            current_query: initial_query.clone(),
            staged: BTreeSet::new(),
            show_commit_modal: false,
            cache,
            cache_root_id: root_cache.root_id,
            canonical_root: canonical_root.clone(),
            last_stats: None,
            watch_enabled: config.enable_watchers,
            watch_handle: None,
            watch_rescan_due: false,
            ui_state_dirty: false,
            ui_state_next_save: None,
            watcher_config: config.watcher_config.clone(),
            sort_mode: SortMode::default(),
            column_widths: DEFAULT_COLUMN_WIDTHS,
            show_layout_modal: false,
        };

        app.expanded.insert(canonical_root.clone());
        app.populate_tree_from_cache(root_cache);
        app.load_persisted_state();
        app.active_root = Some(canonical_root.clone());

        let job_id = app
            .scanner
            .request_scan(initial_query.clone(), Some(app.cache_context()));
        app.pending_job_id = Some(job_id);
        app.status_text = Some(format!("Scanning {}…", initial_query.root.display()));

        app
    }
}

impl eframe::App for DiskSpaceApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_messages(ctx);
        self.sync_watcher();
        self.drain_watch_events();
        self.maybe_trigger_watch_rescan();

        egui::TopBottomPanel::top("top-bar").show(ctx, |ui| {
            self.render_top_bar(ui, ctx);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(root) = self.active_root.clone() {
                self.render_tree(ui, &root);
            } else {
                ui.label("Waiting for scan results…");
            }
        });

        egui::TopBottomPanel::bottom("status-bar").show(ctx, |ui| {
            self.render_status_bar(ui);
        });

        self.render_commit_modal(ctx);
        self.persist_ui_state();
        self.render_layout_modal(ctx);
    }
}

impl DiskSpaceApp {
    fn cache_context(&self) -> CacheContext {
        CacheContext {
            cache: self.cache.clone(),
            root_id: self.cache_root_id,
            canonical_root: self.canonical_root.clone(),
        }
    }

    fn clear_active_cache(&mut self) {
        if self.pending_clear_job_id.is_some() {
            return;
        }

        let ctx = self.cache_context();
        let job_id = self.scanner.request_cache_clear(ctx);
        self.pending_clear_job_id = Some(job_id);
        self.pending_job_id = None;
        self.active_job_id = None;
        self.status_text = Some("Clearing cache…".to_string());
    }

    fn sync_watcher(&mut self) {
        let should_run = self.watch_enabled
            && self
                .active_root
                .as_ref()
                .map(|path| path == &self.canonical_root)
                .unwrap_or(true);

        match (should_run, self.watch_handle.is_some()) {
            (true, false) => match watcher::spawn_with_config(
                self.canonical_root.clone(),
                self.watcher_config.clone(),
            ) {
                Ok(handle) => {
                    self.watch_handle = Some(handle);
                    self.watch_rescan_due = true;
                }
                Err(err) => {
                    self.last_error = Some(format!("Watcher start failed: {err:?}"));
                    self.watch_enabled = false;
                }
            },
            (false, true) => {
                if let Some(handle) = self.watch_handle.take() {
                    handle.stop();
                }
            }
            _ => {}
        }
    }

    fn drain_watch_events(&mut self) {
        if let Some(handle) = self.watch_handle.as_ref() {
            let mut buffered = Vec::new();
            while let Ok(event) = handle.events.try_recv() {
                buffered.push(event);
            }

            for event in buffered {
                self.handle_watch_event(event.path, event.kind);
            }
        }
    }

    fn cycle_sort_mode(&mut self) {
        self.sort_mode = match self.sort_mode {
            SortMode::NameAsc => SortMode::SizeDesc,
            SortMode::SizeDesc => SortMode::NameAsc,
        };
        self.schedule_ui_state_save();
    }

    fn handle_watch_event(&mut self, path: PathBuf, kind: WatchEventKind) {
        match kind {
            WatchEventKind::Dirty => {
                if let Some(relative) = self.relative_to_root(&path) {
                    eprintln!("dusk watcher dirty: {}", relative.display());
                    let target = self
                        .parent_relative(&relative)
                        .unwrap_or_else(|| PathBuf::from("."));
                    let _ = self.cache.mark_ancestors_dirty(self.cache_root_id, &target);
                    self.watch_rescan_due = true;
                }
            }
            WatchEventKind::Rescan => {
                eprintln!("dusk watcher rescan requested");
                self.watch_rescan_due = true;
            }
            WatchEventKind::Error(message) => {
                self.last_error = Some(format!("Watcher error: {message}"));
            }
        }
    }

    fn relative_to_root(&self, path: &Path) -> Option<PathBuf> {
        path.strip_prefix(&self.canonical_root)
            .map(|rel| {
                if rel.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    rel.to_path_buf()
                }
            })
            .ok()
    }

    fn maybe_trigger_watch_rescan(&mut self) {
        if self.watch_rescan_due && self.pending_job_id.is_none() && self.active_job_id.is_none() {
            self.watch_rescan_due = false;
            self.status_text = Some("Watcher detected changes; rescanning…".to_string());
            self.trigger_scan();
        }
    }

    fn schedule_ui_state_save(&mut self) {
        self.ui_state_dirty = true;
        self.ui_state_next_save = Some(Instant::now() + Duration::from_millis(400));
    }

    fn reset_layout(&mut self) {
        self.column_widths = DEFAULT_COLUMN_WIDTHS;
        self.sort_mode = SortMode::default();
        self.schedule_ui_state_save();
    }

    fn load_persisted_state(&mut self) {
        let state = match self.cache.load_ui_state(self.cache_root_id) {
            Ok(Some((json, version))) if version == UI_STATE_VERSION => {
                match serde_json::from_str::<PersistedUiState>(&json) {
                    Ok(state) => Some(state),
                    Err(err) => {
                        eprintln!("dusk ui state parse error: {err}");
                        None
                    }
                }
            }
            Ok(Some((_json, version))) => {
                eprintln!(
                    "dusk ui state version mismatch ({} vs {})",
                    version, UI_STATE_VERSION
                );
                None
            }
            Ok(None) => None,
            Err(err) => {
                eprintln!("dusk ui state load error: {err}");
                None
            }
        };

        let Some(state) = state else {
            return;
        };

        self.expanded.clear();
        self.expanded.insert(self.canonical_root.clone());
        for rel in state.expanded {
            if rel == "." {
                continue;
            }
            let absolute = self.canonical_root.join(rel);
            self.expanded.insert(absolute);
        }

        self.staged.clear();
        for rel in state.staged {
            let absolute = self.canonical_root.join(rel);
            self.staged.insert(absolute);
        }

        if state.watch_enabled {
            self.watch_enabled = true;
        }

        if state.version >= 2 {
            self.sort_mode = state.sort_mode;
            if state.column_widths.len() == COLUMN_COUNT {
                for (idx, value) in state.column_widths.iter().enumerate() {
                    self.column_widths[idx] = *value;
                }
            } else {
                self.column_widths = DEFAULT_COLUMN_WIDTHS;
            }
        }
    }

    fn persist_ui_state(&mut self) {
        if !self.ui_state_dirty {
            return;
        }

        if let Some(deadline) = self.ui_state_next_save {
            if Instant::now() < deadline {
                return;
            }
        }

        let expanded: Vec<String> = self
            .expanded
            .iter()
            .filter_map(|path| path.strip_prefix(&self.canonical_root).ok())
            .map(|rel| {
                if rel.as_os_str().is_empty() {
                    ".".to_string()
                } else {
                    rel.to_string_lossy().into_owned()
                }
            })
            .collect();

        let staged: Vec<String> = self
            .staged
            .iter()
            .filter_map(|path| path.strip_prefix(&self.canonical_root).ok())
            .map(|rel| rel.to_string_lossy().into_owned())
            .collect();

        let state = PersistedUiState {
            version: UI_STATE_VERSION as u32,
            expanded,
            staged,
            watch_enabled: self.watch_enabled,
            sort_mode: self.sort_mode,
            column_widths: self.column_widths.iter().copied().collect(),
        };

        match serde_json::to_string(&state) {
            Ok(json) => {
                if let Err(err) =
                    self.cache
                        .save_ui_state(self.cache_root_id, &json, UI_STATE_VERSION)
                {
                    self.last_error = Some(format!("Failed to persist UI state: {err}"));
                } else {
                    self.ui_state_dirty = false;
                    self.ui_state_next_save = None;
                }
            }
            Err(err) => {
                self.last_error = Some(format!("Failed to serialise UI state: {err}"));
            }
        }
    }

    fn parent_relative(&self, relative: &Path) -> Option<PathBuf> {
        if relative.as_os_str().is_empty() || relative == Path::new(".") {
            None
        } else if let Some(parent) = relative.parent() {
            if parent.as_os_str().is_empty() {
                Some(PathBuf::from("."))
            } else {
                Some(parent.to_path_buf())
            }
        } else {
            Some(PathBuf::from("."))
        }
    }

    fn render_top_bar(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
            ui.set_width(ui.available_width());
            let response = ui.add(
                egui::TextEdit::singleline(&mut self.search_input)
                    .hint_text("Pattern, e.g. ~/Downloads/*.zip >500MB")
                    .desired_width(f32::INFINITY),
            );

            let pressed_enter =
                response.lost_focus() && ctx.input(|i| i.key_pressed(egui::Key::Enter));
            if pressed_enter {
                ctx.memory_mut(|mem| mem.request_focus(response.id));
                self.trigger_scan();
            }

            ui.add_space(12.0);
            let checkbox = egui::Checkbox::new(&mut self.watch_enabled, "Watch FS");
            let response = ui
                .add(checkbox)
                .on_hover_text("Enable live filesystem updates for the current root");
            if response.changed() {
                self.schedule_ui_state_save();
            }

            ui.add_space(12.0);
            let sort_label = match self.sort_mode {
                SortMode::NameAsc => "Sort: Name",
                SortMode::SizeDesc => "Sort: Size",
            };
            if ui.button(sort_label).clicked() {
                self.cycle_sort_mode();
            }

            ui.add_space(8.0);
            if ui.button("Layout").clicked() {
                self.show_layout_modal = true;
            }
        });
    }

    fn render_tree(&mut self, ui: &mut egui::Ui, root: &Path) {
        if self.tree.get(root).is_none() {
            ui.label("Waiting for scan results…");
            return;
        }

        let mut rows = Vec::new();
        let mut size_cache = BTreeMap::new();
        self.collect_rows(root, 0, &mut rows, root, &mut size_cache);

        if rows.is_empty() {
            ui.label("No entries yet.");
            return;
        }

        egui::ScrollArea::both()
            .auto_shrink([false, false])
            .show(ui, |scroll_ui| {
                let table = TableBuilder::new(scroll_ui)
                    .striped(true)
                    .column(Column::exact(self.column_widths[0]).clip(false))
                    .column(
                        Column::remainder()
                            .at_least(self.column_widths[1])
                            .clip(false),
                    )
                    .column(Column::exact(self.column_widths[2]).clip(false))
                    .column(Column::exact(self.column_widths[3]).clip(false))
                    .column(Column::exact(self.column_widths[4]).clip(false))
                    .column(Column::exact(self.column_widths[5]).clip(false));

                table
                    .header(24.0, |mut header| {
                        header.col(|ui| {
                            ui.strong("Stage");
                        });
                        header.col(|ui| {
                            ui.strong("Name");
                        });
                        header.col(|ui| {
                            ui.strong("Size");
                        });
                        header.col(|ui| {
                            ui.strong("Total");
                        });
                        header.col(|ui| {
                            ui.strong("Modified");
                        });
                        header.col(|ui| {
                            ui.strong("Created");
                        });
                    })
                    .body(|body| {
                        let row_count = rows.len();
                        body.rows(24.0, row_count, |mut row| {
                            let (path, depth) = &rows[row.index()];
                            let node = {
                                let Some(node_ref) = self.tree.get(path) else {
                                    return;
                                };
                                node_ref.clone()
                            };
                            let path_buf = path.clone();
                            let is_directory = node.kind == FileKind::Directory;
                            let is_expanded_initial = self.expanded.contains(&path_buf);
                            let is_staged_initial = self.staged.contains(&path_buf);

                            let mut staged_action = None;
                            row.col(|ui| {
                                let mut staged_state = is_staged_initial;
                                if ui.add(egui::Checkbox::new(&mut staged_state, "")).changed() {
                                    staged_action = Some(staged_state);
                                }
                            });
                            if let Some(new_state) = staged_action {
                                if new_state {
                                    self.staged.insert(path_buf.clone());
                                } else {
                                    self.staged.remove(&path_buf);
                                }
                                self.schedule_ui_state_save();
                            }

                            let mut expand_action: Option<bool> = None;
                            let mut label_response: Option<egui::Response> = None;
                            row.col(|ui| {
                                let _ = ui.horizontal(|ui| {
                                    ui.add_space((*depth as f32) * 16.0);
                                    if is_directory {
                                        let icon = if is_expanded_initial { "▾" } else { "▸" };
                                        let button = egui::Button::new(icon)
                                            .frame(false)
                                            .min_size(egui::vec2(16.0, 16.0));
                                        if ui.add(button).clicked() {
                                            expand_action = Some(!is_expanded_initial);
                                        }
                                        ui.add_space(4.0);
                                    } else {
                                        ui.add_space(20.0);
                                    }
                                    let response = ui.label(node.name.clone());
                                    label_response = Some(response);
                                });

                                if path == root {
                                    if let Some(resp) = label_response.take() {
                                        resp.context_menu(|ui| {
                                            if ui.button("Clear Cache").clicked() {
                                                self.clear_active_cache();
                                                ui.close_menu();
                                            }
                                        });
                                    }
                                }
                            });
                            if let Some(open) = expand_action {
                                if open {
                                    self.expanded.insert(path_buf.clone());
                                } else {
                                    self.expanded.remove(&path_buf);
                                }
                                self.schedule_ui_state_save();
                            }

                            row.col(|ui| {
                                ui.label(format_size(node.direct_size));
                            });

                            let aggregated =
                                self.tree.aggregated_size_with_cache(path, &mut size_cache);
                            row.col(|ui| {
                                ui.label(format_size(aggregated));
                            });

                            row.col(|ui| {
                                ui.label(format_system_time(node.modified));
                            });

                            row.col(|ui| {
                                ui.label(format_system_time(node.created));
                            });
                        });
                    });
            });
    }

    fn collect_rows(
        &mut self,
        path: &Path,
        depth: usize,
        rows: &mut Vec<(PathBuf, usize)>,
        root: &Path,
        size_cache: &mut BTreeMap<PathBuf, u64>,
    ) {
        let node = {
            let Some(node_ref) = self.tree.get(path) else {
                return;
            };
            node_ref.clone()
        };

        let is_root = path == root;
        let should_show = match node.kind {
            FileKind::File => true,
            FileKind::Directory => is_root || node.contains_match,
        };

        if !should_show {
            return;
        }

        rows.push((path.to_path_buf(), depth));

        if node.kind == FileKind::Directory && self.expanded.contains(path) {
            let mut children = self.tree.children(path);
            match self.sort_mode {
                SortMode::NameAsc => {
                    children.sort_by(|lhs, rhs| compare_paths(&self.tree, lhs, rhs));
                }
                SortMode::SizeDesc => {
                    children.sort_by(|lhs, rhs| {
                        let lhs_size = self.tree.aggregated_size_with_cache(lhs, size_cache);
                        let rhs_size = self.tree.aggregated_size_with_cache(rhs, size_cache);
                        rhs_size
                            .cmp(&lhs_size)
                            .then_with(|| compare_paths(&self.tree, lhs, rhs))
                    });
                }
            }
            for child in children {
                self.collect_rows(&child, depth + 1, rows, root, size_cache);
            }
        }
    }

    fn render_status_bar(&mut self, ui: &mut egui::Ui) {
        ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
            if let Some(status) = &self.status_text {
                ui.label(status);
            } else {
                ui.label("Ready");
            }

            if let Some(error) = &self.last_error {
                ui.add_space(12.0);
                ui.colored_label(egui::Color32::from_rgb(200, 64, 64), error);
            }

            if !self.staged.is_empty() {
                ui.add_space(16.0);
                let label = format!("Commit staged ({})", self.staged.len());
                if ui.button(label).clicked() {
                    self.show_commit_modal = true;
                }
            }

            if let Some(stats) = &self.last_stats {
                ui.add_space(16.0);
                let reused_bytes = format_size(stats.cached_bytes);
                let mut label = format!(
                    "cache: {} dirs, {} entries, {}",
                    stats.cached_dirs, stats.cached_entries, reused_bytes
                );
                if stats.fs_errors > 0 || stats.cache_validation_errors > 0 {
                    label.push_str(&format!(
                        " (fs errors: {}, cache errs: {})",
                        stats.fs_errors, stats.cache_validation_errors
                    ));
                }
                ui.label(label);
            }
        });
    }

    fn render_commit_modal(&mut self, ctx: &egui::Context) {
        if !self.show_commit_modal {
            return;
        }

        let staged_paths: Vec<PathBuf> = self.staged.iter().cloned().collect();
        let mut open = self.show_commit_modal;

        egui::Window::new("Confirm Deletion")
            .collapsible(false)
            .resizable(false)
            .open(&mut open)
            .show(ctx, |ui| {
                if staged_paths.is_empty() {
                    ui.label("No staged items.");
                } else {
                    ui.label("The following items will be deleted:");
                    ui.add_space(8.0);
                    egui::ScrollArea::vertical()
                        .max_height(220.0)
                        .show(ui, |scroll| {
                            for path in &staged_paths {
                                scroll.label(path.display().to_string());
                            }
                        });
                }

                ui.add_space(12.0);
                ui.separator();
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Cancel").clicked() {
                        self.show_commit_modal = false;
                    }

                    ui.add_space(12.0);

                    let confirm_enabled = !staged_paths.is_empty();
                    ui.add_enabled_ui(confirm_enabled, |ui| {
                        let button = egui::Button::new("Confirm delete")
                            .fill(egui::Color32::from_rgb(200, 80, 80));
                        if ui.add(button).clicked() {
                            self.execute_commit(&staged_paths);
                            self.show_commit_modal = false;
                        }
                    });
                });
            });

        self.show_commit_modal = open && self.show_commit_modal;
    }

    fn render_layout_modal(&mut self, ctx: &egui::Context) {
        if !self.show_layout_modal {
            return;
        }

        let mut open_flag = true;
        egui::Window::new("Layout Settings")
            .collapsible(false)
            .resizable(false)
            .open(&mut open_flag)
            .show(ctx, |ui| {
                ui.label("Adjust column widths (points):");
                ui.add_space(8.0);
                for (index, label) in COLUMN_LABELS.iter().enumerate() {
                    let mut value = self.column_widths[index];
                    let mut changed = false;
                    ui.horizontal(|ui| {
                        ui.label(*label);
                        let response = ui.add(
                            egui::DragValue::new(&mut value)
                                .clamp_range(20.0..=800.0)
                                .speed(4.0),
                        );
                        if response.changed() {
                            changed = true;
                        }
                    });
                    if changed {
                        self.column_widths[index] = value;
                        self.schedule_ui_state_save();
                    }
                }

                ui.add_space(12.0);
                ui.label("Sort order:");
                ui.horizontal(|ui| {
                    let name_selected = self.sort_mode == SortMode::NameAsc;
                    if ui.selectable_label(name_selected, "Name (A→Z)").clicked() {
                        if self.sort_mode != SortMode::NameAsc {
                            self.sort_mode = SortMode::NameAsc;
                            self.schedule_ui_state_save();
                        }
                    }
                    let size_selected = self.sort_mode == SortMode::SizeDesc;
                    if ui.selectable_label(size_selected, "Size (desc)").clicked() {
                        if self.sort_mode != SortMode::SizeDesc {
                            self.sort_mode = SortMode::SizeDesc;
                            self.schedule_ui_state_save();
                        }
                    }
                });

                ui.add_space(12.0);
                ui.separator();
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Reset layout").clicked() {
                        self.reset_layout();
                    }
                    if ui.button("Close").clicked() {
                        self.show_layout_modal = false;
                    }
                });
            });

        if !open_flag {
            self.show_layout_modal = false;
        }
    }

    fn execute_commit(&mut self, staged_paths: &[PathBuf]) {
        if staged_paths.is_empty() {
            return;
        }

        let mut deleted = Vec::new();
        let mut errors = Vec::new();

        for path in staged_paths {
            if !path.starts_with(&self.current_query.root) {
                errors.push((path.clone(), "outside current root".to_string()));
                continue;
            }

            let result = match fs::metadata(path) {
                Ok(metadata) => {
                    if metadata.is_dir() {
                        fs::remove_dir_all(path)
                    } else {
                        fs::remove_file(path)
                    }
                }
                Err(err) => Err(err),
            };

            match result {
                Ok(()) => {
                    deleted.push(path.clone());
                    if let Ok(relative) = path.strip_prefix(&self.canonical_root) {
                        let _ = self.cache.remove_entry(self.cache_root_id, relative);
                        if let Some(parent) = self.parent_relative(relative) {
                            let _ = self.cache.mark_ancestors_dirty(self.cache_root_id, &parent);
                        }
                    }
                }
                Err(err) => {
                    errors.push((path.clone(), err.to_string()));
                }
            }
        }

        for path in &deleted {
            self.staged.remove(path);
        }
        if !deleted.is_empty() {
            self.schedule_ui_state_save();
        }

        if !deleted.is_empty() {
            let count = deleted.len();
            self.trigger_scan();
            self.status_text = Some(format!("Deleted {count} item(s); rescanning…"));
        }

        if !errors.is_empty() {
            let mut message = String::from("Deletion errors:\n");
            for (path, err) in errors {
                message.push_str(&format!("{}: {err}\n", path.display()));
            }
            self.last_error = Some(message.trim_end().to_string());
        }
    }

    fn trigger_scan(&mut self) {
        let query = parse_input(&self.search_input);
        self.current_query = query.clone();
        self.entries_seen = 0;
        self.tree.clear();
        self.expanded.clear();
        self.last_stats = None;
        self.watch_rescan_due = false;
        self.schedule_ui_state_save();
        let cache_ctx = if query.root == self.canonical_root {
            Some(self.cache_context())
        } else {
            None
        };
        let job_id = self.scanner.request_scan(query.clone(), cache_ctx);
        self.pending_job_id = Some(job_id);
        self.status_text = Some(format!("Scanning {}…", query.root.display()));
        self.active_root = None;
        self.last_error = None;
    }

    fn drain_messages(&mut self, ctx: &egui::Context) {
        let mut updated = false;
        while let Ok(message) = self.scan_rx.try_recv() {
            updated = true;
            match message {
                ScanMessage::Begin { job_id, root } => {
                    let is_newer = self
                        .active_job_id
                        .map(|active| job_id >= active)
                        .unwrap_or(true);

                    if !is_newer {
                        continue;
                    }

                    if self.pending_job_id == Some(job_id) {
                        self.pending_job_id = None;
                    }

                    self.active_job_id = Some(job_id);
                    self.active_root = Some(root.clone());
                    self.entries_seen = 0;
                    self.tree.clear();
                    self.expanded.clear();
                    self.expanded.insert(root.clone());
                    self.last_error = None;
                    self.status_text = Some(format!("Scanning {}…", root.display()));
                    self.last_stats = None;
                }
                ScanMessage::Entry { job_id, mut entry } => {
                    if Some(job_id) == self.active_job_id {
                        if entry.file_name.is_empty() {
                            entry.file_name = entry
                                .path
                                .file_name()
                                .and_then(|f| f.to_str())
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| entry.path.display().to_string());
                        }
                        self.tree.upsert(entry);
                        self.entries_seen += 1;
                    }
                }
                ScanMessage::Error {
                    job_id,
                    path,
                    message,
                } => {
                    if Some(job_id) == self.active_job_id {
                        self.last_error = Some(format!("{}: {message}", path.display()));
                    }
                }
                ScanMessage::Stats { job_id, stats } => {
                    if Some(job_id) == self.active_job_id {
                        self.last_stats = Some(stats);
                    }
                }
                ScanMessage::CacheCleared {
                    job_id,
                    root,
                    cleared,
                } => {
                    if Some(job_id) == self.pending_clear_job_id {
                        self.pending_clear_job_id = None;
                        match self.cache.load_root(&root) {
                            Ok(root_cache) => {
                                self.cache_root_id = root_cache.root_id;
                                self.tree.clear();
                                self.expanded.clear();
                                self.expanded.insert(root.clone());
                                self.populate_tree_from_cache(root_cache);
                                let status = if cleared {
                                    "Cache cleared; rescanning…"
                                } else {
                                    "Cache already clean; rescanning…"
                                };
                                self.status_text = Some(status.to_string());
                                self.schedule_ui_state_save();
                                self.trigger_scan();
                            }
                            Err(err) => {
                                self.last_error =
                                    Some(format!("Failed to reload cache after clearing: {err}"));
                            }
                        }
                    }
                }
                ScanMessage::Complete { job_id } => {
                    if Some(job_id) == self.active_job_id {
                        if self.pending_job_id.is_none() {
                            if let Some(root) = self.active_root.as_ref() {
                                let status = if let Some(stats) = self.last_stats {
                                    format!(
                                        "Scan complete for {} ({} entries; reused {} cached dirs)",
                                        root.display(),
                                        self.entries_seen,
                                        stats.cached_dirs
                                    )
                                } else {
                                    format!(
                                        "Scan complete for {} ({} entries)",
                                        root.display(),
                                        self.entries_seen
                                    )
                                };
                                self.status_text = Some(status);
                            } else {
                                let status =
                                    format!("Scan complete ({} entries)", self.entries_seen);
                                self.status_text = Some(status);
                            }
                        }
                        self.active_job_id = None;
                    }

                    if Some(job_id) == self.pending_job_id {
                        self.pending_job_id = None;
                    }
                }
            }
        }

        if updated {
            ctx.request_repaint();
        }
    }

    fn populate_tree_from_cache(&mut self, mut root_cache: RootCache) {
        if root_cache.entries.is_empty() {
            return;
        }

        root_cache
            .entries
            .sort_by_key(|entry| entry.path.components().count());

        for entry in root_cache.entries {
            let absolute = if entry.path.as_os_str().is_empty() || entry.path == PathBuf::from(".")
            {
                self.canonical_root.clone()
            } else {
                self.canonical_root.join(&entry.path)
            };

            let file_name = absolute
                .file_name()
                .and_then(|f| f.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| absolute.display().to_string());

            let file_entry = FileEntry::new(
                absolute,
                file_name,
                entry.kind,
                entry.direct_size,
                cache::timestamp_to_system(entry.modified),
                cache::timestamp_to_system(entry.created),
            );

            self.tree.upsert(file_entry);
        }
    }
}

fn compare_paths(store: &TreeStore, lhs: &Path, rhs: &Path) -> std::cmp::Ordering {
    let lhs_node = store.get(lhs);
    let rhs_node = store.get(rhs);

    match (lhs_node, rhs_node) {
        (Some(a), Some(b)) => {
            if a.kind != b.kind {
                return match (a.kind, b.kind) {
                    (FileKind::Directory, FileKind::File) => std::cmp::Ordering::Less,
                    (FileKind::File, FileKind::Directory) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                };
            }
            a.name.to_lowercase().cmp(&b.name.to_lowercase())
        }
        _ => lhs.cmp(rhs),
    }
}
