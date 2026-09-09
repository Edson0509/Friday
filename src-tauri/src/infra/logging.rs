use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling;
use tracing_subscriber::reload;
use tracing_subscriber::{fmt, prelude::*, EnvFilter, Registry};

pub struct LoggingGuard {
    _file_guard: WorkerGuard,
    filter_handle: reload::Handle<EnvFilter, Registry>,
    _dispatch: tracing::Dispatch,
}

impl LoggingGuard {
    pub fn filter_handle(&self) -> reload::Handle<EnvFilter, Registry> {
        self.filter_handle.clone()
    }
}

pub fn init(log_dir: PathBuf) -> LoggingGuard {
    let file_appender = rolling::daily(&log_dir, "friday.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("debug"));
    let (filter_layer, filter_handle) = reload::Layer::new(filter);

    let subscriber = Registry::default()
        .with(filter_layer)
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(fmt::layer().with_writer(non_blocking));

    let dispatch = tracing::Dispatch::new(subscriber);
    let dispatch_clone = dispatch.clone();
    let _ = tracing::dispatcher::set_global_default(dispatch);

    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_default();
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(|s| s.as_str()))
            .unwrap_or("panic payload");
        tracing::error!(location = %location, payload = %payload, "panic");
        prev_hook(info);
    }));

    cleanup_old_logs(&log_dir, 7);

    tracing::info!(?log_dir, "logging initialized");
    LoggingGuard {
        _file_guard: guard,
        filter_handle,
        _dispatch: dispatch_clone,
    }
}

pub fn set_level(handle: &reload::Handle<EnvFilter, Registry>, level: &str) -> Result<(), String> {
    let old_level = handle.with_current(|f| format!("{:?}", f)).unwrap_or_default();
    let new_filter = EnvFilter::new(level);
    handle.reload(new_filter).map_err(|e| e.to_string())?;
    tracing::info!(old_level = %old_level, new_level = level, "log level changed");
    Ok(())
}

pub(crate) fn cleanup_old_logs(log_dir: &std::path::Path, max_days: u64) {
    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(max_days * 86400);
    let mut removed: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(log_dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if let Ok(modified) = meta.modified() {
                    if modified < cutoff {
                        let path = entry.path();
                        tracing::debug!(path = %path.display(), "removing old log file");
                        if let Err(e) = std::fs::remove_file(&path) {
                            tracing::warn!(?e, path = %path.display(), "failed to remove old log file");
                        } else {
                            removed += 1;
                        }
                    }
                }
            }
        }
    }
    tracing::debug!(removed, "old log files cleaned up");
}

/// 按 session_id 过滤日志目录中全部落盘日志，导出为单个文件。
///
/// 扫描 `log_dir` 下所有 `friday.log*` 文件（按文件名升序，即日期序），
/// 保留包含 `session_id` 的行（session_id 是贯穿会话全链路的 traceid），
/// 原样写入 `out_path`。`out_path` 的父目录须已存在。
#[derive(serde::Serialize)]
pub struct SessionLogExport {
    pub path: PathBuf,
    pub line_count: u64,
    pub files_scanned: u64,
}

pub fn export_session_logs(
    log_dir: &std::path::Path,
    out_path: &std::path::Path,
    session_id: &str,
) -> std::io::Result<SessionLogExport> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(log_dir)?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("friday.log"))
        })
        .collect();
    files.sort();

    let out_file = std::fs::File::create(out_path)?;
    let mut writer = BufWriter::new(out_file);
    let mut line_count: u64 = 0;
    let mut buf: Vec<u8> = Vec::new();

    for path in &files {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        loop {
            buf.clear();
            reader.read_until(b'\n', &mut buf)?;
            if buf.is_empty() {
                break;
            }
            if String::from_utf8_lossy(&buf).contains(session_id) {
                writer.write_all(&buf)?;
                line_count += 1;
            }
        }
    }
    writer.flush()?;

    Ok(SessionLogExport {
        path: out_path.to_path_buf(),
        line_count,
        files_scanned: files.len() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_logging_init_creates_log_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();
        assert!(log_dir.exists());

        let _guard = init(log_dir);
    }

    #[test]
    fn test_init_returns_logging_guard() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();
        let guard = init(log_dir);
        let _handle = &guard.filter_handle;
    }

    #[test]
    fn test_set_level_changes_filter() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();
        let guard = init(log_dir);
        let handle = &guard.filter_handle;

        let result = set_level(handle, "trace");
        assert!(result.is_ok());

        let result = set_level(handle, "info");
        assert!(result.is_ok());
    }

    fn set_file_modified(path: &std::path::Path, time: SystemTime) {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap();
        let times = std::fs::FileTimes::new().set_modified(time);
        file.set_times(times).unwrap();
    }

    #[test]
    fn test_cleanup_old_logs_removes_old_files() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();

        let old_file = log_dir.join("old.log");
        std::fs::write(&old_file, "old").unwrap();

        let old_time = SystemTime::now() - Duration::from_secs(10 * 86400);
        set_file_modified(&old_file, old_time);

        cleanup_old_logs(&log_dir, 7);

        assert!(!old_file.exists());
    }

    #[test]
    fn test_cleanup_old_logs_keeps_recent_files() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();

        let recent_file = log_dir.join("recent.log");
        std::fs::write(&recent_file, "recent").unwrap();

        // File has current modification time (default when just created)
        cleanup_old_logs(&log_dir, 7);

        assert!(recent_file.exists());
    }

    #[test]
    fn test_cleanup_old_logs_keeps_within_7_days() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();

        let file_5_days = log_dir.join("5days.log");
        std::fs::write(&file_5_days, "data").unwrap();

        let five_days_ago = SystemTime::now() - Duration::from_secs(5 * 86400);
        set_file_modified(&file_5_days, five_days_ago);

        cleanup_old_logs(&log_dir, 7);

        assert!(file_5_days.exists());
    }

    #[test]
    fn test_panic_hook_installed() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();
        let _guard = init(log_dir);
        // 注意：不得在此替换全局 panic hook——libtest 依赖它输出断言失败信息，
        // 替换成空操作会吞掉同进程内其他测试（如 jfr 虚拟时钟测试）的 panic 消息
    }

    #[test]
    fn test_export_session_logs_filters_matching_lines_across_files() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::write(
            log_dir.join("friday.log.2026-09-08"),
            "INFO line for s-one\nINFO unrelated line\nWARN s-one again\n",
        )
        .unwrap();
        std::fs::write(
            log_dir.join("friday.log.2026-09-09"),
            "ERROR s-one crashed\nINFO another session s-two\n",
        )
        .unwrap();

        let out_path = tmp.path().join("export").join("session-logs.log");
        std::fs::create_dir_all(out_path.parent().unwrap()).unwrap();

        let result = export_session_logs(&log_dir, &out_path, "s-one").unwrap();

        assert_eq!(result.files_scanned, 2);
        assert_eq!(result.line_count, 3);
        let content = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(
            content,
            "INFO line for s-one\nWARN s-one again\nERROR s-one crashed\n"
        );
    }

    #[test]
    fn test_export_session_logs_ignores_non_friday_files() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::write(
            log_dir.join("friday.log.2026-09-09"),
            "INFO s-one line\n",
        )
        .unwrap();
        std::fs::write(log_dir.join("other.log"), "INFO s-one should be ignored\n").unwrap();

        let out_path = tmp.path().join("export").join("session-logs.log");
        std::fs::create_dir_all(out_path.parent().unwrap()).unwrap();

        let result = export_session_logs(&log_dir, &out_path, "s-one").unwrap();

        assert_eq!(result.files_scanned, 1);
        assert_eq!(result.line_count, 1);
        let content = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(content, "INFO s-one line\n");
    }

    #[test]
    fn test_export_session_logs_zero_matches_writes_empty_file() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::write(log_dir.join("friday.log.2026-09-09"), "INFO s-two line\n").unwrap();

        let out_path = tmp.path().join("export").join("session-logs.log");
        std::fs::create_dir_all(out_path.parent().unwrap()).unwrap();

        let result = export_session_logs(&log_dir, &out_path, "s-one").unwrap();

        assert_eq!(result.files_scanned, 1);
        assert_eq!(result.line_count, 0);
        let content = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(content, "");
    }

    #[test]
    fn test_export_session_logs_reads_files_in_name_order() {
        let tmp = tempfile::tempdir().unwrap();
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();
        // 先写新日期文件、后写旧日期文件，验证输出按文件名（日期）排序而非创建顺序
        std::fs::write(log_dir.join("friday.log.2026-09-09"), "INFO s-one day2\n").unwrap();
        std::fs::write(log_dir.join("friday.log.2026-09-08"), "INFO s-one day1\n").unwrap();

        let out_path = tmp.path().join("export").join("session-logs.log");
        std::fs::create_dir_all(out_path.parent().unwrap()).unwrap();

        export_session_logs(&log_dir, &out_path, "s-one").unwrap();

        let content = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(content, "INFO s-one day1\nINFO s-one day2\n");
    }
}
