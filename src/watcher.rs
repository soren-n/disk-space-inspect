use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, unbounded};
use log::{debug, trace};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

#[derive(Debug, Clone)]
pub struct WatcherConfig {
    pub notify_poll_interval: Duration,
    pub fallback_initial: Duration,
    pub fallback_max: Duration,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            notify_poll_interval: Duration::from_secs(2),
            fallback_initial: Duration::from_secs(5),
            fallback_max: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WatchEventKind {
    Dirty,
    Rescan,
    Error(String),
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub path: PathBuf,
    pub kind: WatchEventKind,
    pub timestamp: Instant,
}

impl WatchEvent {
    pub fn dirty(path: PathBuf) -> Self {
        Self {
            path,
            kind: WatchEventKind::Dirty,
            timestamp: Instant::now(),
        }
    }

    pub fn rescan(path: PathBuf) -> Self {
        Self {
            path,
            kind: WatchEventKind::Rescan,
            timestamp: Instant::now(),
        }
    }

    pub fn error(path: PathBuf, message: String) -> Self {
        Self {
            path,
            kind: WatchEventKind::Error(message),
            timestamp: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub enum WatcherError {
    ThreadSpawn(std::io::Error),
}

pub struct WatchHandle {
    pub events: Receiver<WatchEvent>,
    shutdown: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}

impl WatchHandle {
    pub fn stop(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

pub fn spawn(root: PathBuf) -> Result<WatchHandle, WatcherError> {
    spawn_with_config(root, WatcherConfig::default())
}

pub fn spawn_with_config(
    root: PathBuf,
    config: WatcherConfig,
) -> Result<WatchHandle, WatcherError> {
    let (event_tx, event_rx) = unbounded();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    let config_clone = config.clone();

    let join = thread::Builder::new()
        .name("dusk-watcher".into())
        .spawn(move || {
            if let Err(err) = run_notify_loop(&root, &shutdown_clone, &event_tx, &config_clone) {
                debug!("dusk watcher falling back to polling: {err}");
                let _ = event_tx.send(WatchEvent::error(root.clone(), err));
                run_polling_loop(&root, &shutdown_clone, &event_tx, &config_clone);
            }
        })
        .map_err(WatcherError::ThreadSpawn)?;

    Ok(WatchHandle {
        events: event_rx,
        shutdown,
        join: Some(join),
    })
}

fn run_notify_loop(
    root: &PathBuf,
    shutdown: &Arc<AtomicBool>,
    event_tx: &Sender<WatchEvent>,
    config: &WatcherConfig,
) -> Result<(), String> {
    let tx = event_tx.clone();
    let root_clone = root.clone();
    let mut watcher = RecommendedWatcher::new(
        move |event: Result<Event, notify::Error>| match event {
            Ok(event) => {
                for path in &event.paths {
                    let mapped = map_event_kind(&event.kind, path.clone(), &root_clone);
                    if let Some(ev) = mapped {
                        trace!(
                            "dusk watcher event kind={:?} path={}",
                            event.kind,
                            path.display()
                        );
                        let _ = tx.send(ev);
                    }
                }
            }
            Err(err) => {
                let _ = tx.send(WatchEvent::error(root_clone.clone(), err.to_string()));
            }
        },
        Config::default()
            .with_poll_interval(config.notify_poll_interval)
            .with_compare_contents(false),
    )
    .map_err(|err| format!("failed to initialise watcher: {err}"))?;

    watcher
        .watch(root, RecursiveMode::Recursive)
        .map_err(|err| format!("failed to watch {}: {err}", root.display()))?;

    while !shutdown.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(250));
    }

    Ok(())
}

fn run_polling_loop(
    root: &PathBuf,
    shutdown: &Arc<AtomicBool>,
    event_tx: &Sender<WatchEvent>,
    config: &WatcherConfig,
) {
    let mut interval = config.fallback_initial;
    let max_interval = config.fallback_max;

    while !shutdown.load(Ordering::SeqCst) {
        thread::sleep(interval);
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        trace!("dusk watcher polling tick interval={:?}", interval);
        let _ = event_tx.send(WatchEvent::rescan(root.clone()));
        interval = (interval * 2).min(max_interval);
    }
}

fn map_event_kind(kind: &EventKind, path: PathBuf, root: &PathBuf) -> Option<WatchEvent> {
    match kind {
        EventKind::Remove(_) => Some(WatchEvent::dirty(path)),
        EventKind::Create(_) => Some(WatchEvent::dirty(path)),
        EventKind::Modify(_) => Some(WatchEvent::dirty(path)),
        EventKind::Access(_) => None,
        EventKind::Other => Some(WatchEvent::rescan(root.clone())),
        EventKind::Any => Some(WatchEvent::rescan(root.clone())),
    }
}
