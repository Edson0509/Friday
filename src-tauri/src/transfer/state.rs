use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Download,
    Upload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pending,
    Connecting,
    Transferring,
    Retrying,
    Completed,
    Failed,
    Cancelled,
}

impl Status {
    /// 终态：completed / failed / cancelled
    pub fn is_terminal(self) -> bool {
        matches!(self, Status::Completed | Status::Failed | Status::Cancelled)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferState {
    pub id: String,
    pub direction: Direction,
    pub session_id: String,
    pub env_id: String,
    /// k8s 目标定位（容器内传输时两跳通道用；VM 目标 None）
    pub pod: Option<String>,
    pub container: Option<String>,
    pub remote_path: String,
    pub local_path: PathBuf,
    pub status: Status,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub speed_bps: u64,
    pub attempt: u32,
    pub error: Option<String>,
    pub cleanup_remote_on_success: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl TransferState {
    pub fn new(
        direction: Direction,
        session_id: &str,
        env_id: &str,
        remote_path: &str,
        local_path: PathBuf,
        cleanup_remote_on_success: bool,
        pod: Option<&str>,
        container: Option<&str>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            direction,
            session_id: session_id.to_string(),
            env_id: env_id.to_string(),
            pod: pod.map(|s| s.to_string()),
            container: container.map(|s| s.to_string()),
            remote_path: remote_path.to_string(),
            local_path,
            status: Status::Pending,
            total_bytes: 0,
            transferred_bytes: 0,
            speed_bps: 0,
            attempt: 0,
            error: None,
            cleanup_remote_on_success,
            created_at: chrono::Utc::now(),
            completed_at: None,
        }
    }
}

/// 下载场景本地落盘的临时文件路径：<local>.part
pub fn part_path_for(local: &std::path::Path) -> PathBuf {
    let mut s = local.as_os_str().to_os_string();
    s.push(".part");
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_is_terminal() {
        assert!(Status::Completed.is_terminal());
        assert!(Status::Failed.is_terminal());
        assert!(Status::Cancelled.is_terminal());
        assert!(!Status::Pending.is_terminal());
        assert!(!Status::Transferring.is_terminal());
        assert!(!Status::Retrying.is_terminal());
        assert!(!Status::Connecting.is_terminal());
    }

    #[test]
    fn test_new_state_defaults() {
        let s = TransferState::new(
            Direction::Download,
            "sess",
            "env",
            "/tmp/a.hprof",
            PathBuf::from("/local/a.hprof"),
            false,
            None,
            None,
        );
        assert_eq!(s.status, Status::Pending);
        assert_eq!(s.attempt, 0);
        assert_eq!(s.transferred_bytes, 0);
        assert!(!s.cleanup_remote_on_success);
        assert!(s.error.is_none());
        assert!(uuid::Uuid::parse_str(&s.id).is_ok());
        assert!(s.pod.is_none());
        assert!(s.container.is_none());
    }

    #[test]
    fn test_new_state_carries_pod_target() {
        // VM 目标 None / None；k8s 目标 Some(pod) / Some(container) 原样透传
        let s = TransferState::new(
            Direction::Download,
            "sess",
            "env",
            "/opt/log/dump/coredump/a.hprof",
            PathBuf::from("/local/a.hprof"),
            true,
            Some("pod-1"),
            Some("main"),
        );
        assert_eq!(s.pod.as_deref(), Some("pod-1"));
        assert_eq!(s.container.as_deref(), Some("main"));
        // 空串在工具层已过滤，这里只验透传语义：None 序列化/反序列化不炸
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"pod\":\"pod-1\""));
    }

    #[test]
    fn test_part_path_appends_suffix() {
        assert_eq!(
            part_path_for(std::path::Path::new("/x/a.hprof")),
            PathBuf::from("/x/a.hprof.part")
        );
    }

    #[test]
    fn test_direction_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&Direction::Download).unwrap(),
            "\"download\""
        );
        assert_eq!(
            serde_json::to_string(&Status::Retrying).unwrap(),
            "\"retrying\""
        );
    }
}
