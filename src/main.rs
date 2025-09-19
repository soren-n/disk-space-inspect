use std::path::PathBuf;
use std::process;
use std::time::Duration;

use disk_space_inspect::{app, cache, watcher};
use eframe::{NativeOptions, egui};
use env_logger::Env;
use pico_args::Arguments;
use shellexpand::full;

fn main() -> eframe::Result<()> {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or(""))
        .format_timestamp_secs()
        .try_init();

    let mut args = Arguments::from_env();

    let clear_target = match args.opt_value_from_str::<_, String>("--clear-cache") {
        Ok(value) => value,
        Err(err) => {
            eprintln!("dusk: {err}");
            process::exit(1);
        }
    };

    let watch_poll_secs = match args.opt_value_from_str::<_, u64>("--watch-poll") {
        Ok(value) => value,
        Err(err) => {
            eprintln!("dusk: {err}");
            process::exit(1);
        }
    };

    let watch_max_secs = match args.opt_value_from_str::<_, u64>("--watch-max-poll") {
        Ok(value) => value,
        Err(err) => {
            eprintln!("dusk: {err}");
            process::exit(1);
        }
    };

    let watch_enabled = args.contains("--watch");

    if let Some(raw) = clear_target {
        if let Err(err) = clear_cache_for_root(&raw) {
            eprintln!("dusk: {err}");
            process::exit(1);
        }
        return Ok(());
    }

    let mut watcher_config = watcher::WatcherConfig::default();
    if let Some(secs) = watch_poll_secs {
        let secs = secs.max(1);
        let duration = Duration::from_secs(secs);
        watcher_config.notify_poll_interval = duration;
        watcher_config.fallback_initial = duration;
    }

    if let Some(secs) = watch_max_secs {
        let secs = secs.max(1);
        watcher_config.fallback_max = Duration::from_secs(secs);
    }

    if watcher_config.fallback_initial > watcher_config.fallback_max {
        watcher_config.fallback_initial = watcher_config.fallback_max;
    }

    let cwd_arg: Option<String> = match args.opt_free_from_str() {
        Ok(value) => value,
        Err(err) => {
            eprintln!("dusk: {err}");
            process::exit(1);
        }
    };

    let leftover = args.finish();
    if !leftover.is_empty() {
        let extras: Vec<String> = leftover
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        eprintln!("dusk: unexpected arguments: {}", extras.join(" "));
        process::exit(1);
    }

    if let Err(err) = configure_working_directory(cwd_arg) {
        eprintln!("dusk: {err}");
        process::exit(1);
    }

    let app_config = app::AppConfig {
        enable_watchers: watch_enabled,
        watcher_config,
    };

    let native_options = NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Dusk",
        native_options,
        Box::new(move |cc| Box::new(app::DiskSpaceApp::with_config(cc, app_config.clone()))),
    )
}

fn configure_working_directory(cwd_arg: Option<String>) -> Result<(), String> {
    let Some(raw_path) = cwd_arg else {
        return Ok(());
    };

    let expanded = full(raw_path.as_str()).map_err(|err| err.to_string())?;
    let path = PathBuf::from(expanded.as_ref());
    if !path.exists() {
        return Err(format!("{} does not exist", path.display()));
    }
    if !path.is_dir() {
        return Err(format!("{} is not a directory", path.display()));
    }

    let canonical = path
        .canonicalize()
        .map_err(|err| format!("failed to canonicalize {}: {err}", path.display()))?;

    std::env::set_current_dir(&canonical)
        .map_err(|err| format!("failed to enter {}: {err}", canonical.display()))?;

    Ok(())
}

fn clear_cache_for_root(raw: &str) -> Result<(), String> {
    let expanded = full(raw).map_err(|err| err.to_string())?;
    let path = PathBuf::from(expanded.as_ref());
    let canonical = path
        .canonicalize()
        .map_err(|err| format!("failed to canonicalize {}: {err}", path.display()))?;

    let cache = cache::Cache::open().map_err(|err| err.to_string())?;
    let cleared = cache
        .clear_root_path(&canonical)
        .map_err(|err| err.to_string())?;

    if cleared {
        println!("Cleared cache for {}", canonical.display());
    } else {
        println!("No cache entries for {}", canonical.display());
    }

    Ok(())
}
