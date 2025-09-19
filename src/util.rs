use std::time::SystemTime;

use chrono::Local;

pub fn format_size(bytes: u64) -> String {
    let units: [(&str, f64); 6] = [
        ("B", 1.0),
        ("KiB", 1024.0),
        ("MiB", 1024.0f64.powi(2)),
        ("GiB", 1024.0f64.powi(3)),
        ("TiB", 1024.0f64.powi(4)),
        ("PiB", 1024.0f64.powi(5)),
    ];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let bytes_f64 = bytes as f64;
    let mut unit = units[0];
    for candidate in units.iter() {
        unit = *candidate;
        if bytes_f64 < candidate.1 * 1024.0 {
            break;
        }
    }

    let value = bytes_f64 / unit.1;
    if unit.0 == "B" {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", unit.0)
    }
}

pub fn format_system_time(time: Option<SystemTime>) -> String {
    match time {
        Some(time) => {
            let datetime: chrono::DateTime<Local> = time.into();
            datetime.format("%Y-%m-%d %H:%M").to_string()
        }
        None => "-".to_string(),
    }
}
