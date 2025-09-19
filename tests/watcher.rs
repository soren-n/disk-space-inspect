use std::time::{Duration, Instant};

#[cfg(target_os = "macos")]
#[test]
fn watcher_reports_dirty_event() {
    use disk_space_inspect::watcher;
    let temp = tempfile::tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();

    let handle = watcher::spawn(root.clone()).expect("spawn watcher");
    let events = handle.events.clone();

    std::thread::sleep(Duration::from_millis(500));
    let file_path = root.join("touch.txt");
    std::fs::write(&file_path, "hello").expect("write file");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_dirty = false;

    while Instant::now() < deadline {
        match events.recv_timeout(Duration::from_millis(200)) {
            Ok(event) => match event.kind {
                watcher::WatchEventKind::Dirty => {
                    saw_dirty = true;
                    break;
                }
                watcher::WatchEventKind::Rescan => {
                    saw_dirty = true;
                    break;
                }
                watcher::WatchEventKind::Error(message) => {
                    panic!("watcher error: {message}");
                }
            },
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(err) => panic!("watch channel error: {err}"),
        }
    }

    assert!(
        saw_dirty,
        "expected watcher to emit a dirty or rescan event"
    );

    handle.stop();
}

#[cfg(not(target_os = "macos"))]
#[test]
fn watcher_reports_dirty_event() {
    eprintln!("watcher smoke test skipped: unsupported OS for automated verification");
}
