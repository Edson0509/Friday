//! K8s 容器通道：包装到宿主机的 SSH 通道，把命令透明转发进 Pod。
//! 分发规则：pod 参数存在 → K8sChannel（spec：2026-09-08-k8s-container-tooling-design.md）。

use async_trait::async_trait;
use std::sync::Arc;

use super::channel::{ExecChannel, ExecOutput};
use super::ssh::shell_quote_single;

/// 宿主机侧暂存目录（两跳传输中转）
pub const STAGING_DIR: &str = "/tmp/friday-tools/staging";

/// Pod 内 Friday 工具目录（用户指定：该目录不会触发 ephemeral-storage 驱逐）
pub const POD_TOOLS_DIR: &str = "/opt/log/dump/heapdump/friday-tools";

/// Pod 内 dump 产物目录（Phase 2 的 heap dump / JFR 落这里）
pub const POD_DUMP_DIR: &str = "/opt/log/dump/heapdump";

/// 文件属组要求：非 ossgroup 无法被目标 JVM 用户使用（用户约束，exec 用户 = ossadm 非 root）
pub const OSS_GROUP: &str = "ossgroup";

/// 构造 `kubectl exec ... -- sh -c ...`（纯函数）。
/// container 缺省时省略 -c（kubectl 默认容器 = spec 第一个容器）。
/// 整条命令会再经 SshTransport 的 bash -lc 包装，内嵌单引号由 shell_quote_single 转义。
pub fn wrap_exec_command(pod: &str, container: Option<&str>, cmd: &str) -> String {
    let ctr = match container {
        Some(c) => format!("-c {} ", shell_quote_single(c)),
        None => String::new(),
    };
    format!(
        "kubectl exec {}{} -- sh -c {}",
        ctr,
        shell_quote_single(pod),
        shell_quote_single(cmd)
    )
}

/// pkill -f 的 pattern：转义 ERE 元字符（pkill -f 按扩展正则匹配整条命令行，
/// 命令串里的 `. + ( )` 等字面量必须转义，防止误杀无关进程）
pub fn pkill_pattern(command: &str) -> String {
    let mut out = String::with_capacity(command.len() + 8);
    for c in command.chars() {
        if matches!(
            c,
            '\\' | '.' | '^' | '$' | '|' | '?' | '*' | '+' | '(' | ')' | '[' | ']' | '{' | '}'
        ) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Pod 内路径校验：必须绝对路径且不含 NUL
pub fn validate_pod_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/') {
        return Err(format!("pod path must be absolute: {path:?}"));
    }
    if path.contains('\0') {
        return Err("pod path must not contain NUL".to_string());
    }
    Ok(())
}

/// K8s 容器通道：run 语义 = 在 Pod 容器内执行（sh -c，busybox 无 bash）；
/// upload = 两跳注入（Task 2 实现）。连接生命周期完全委托 base SSH 通道。
pub struct K8sChannel {
    pub base: Arc<dyn ExecChannel>,
    pub pod: String,
    pub container: Option<String>,
}

impl K8sChannel {
    pub fn ctr_flag(&self) -> String {
        match &self.container {
            Some(c) => format!("-c {} ", shell_quote_single(c)),
            None => String::new(),
        }
    }
}

#[async_trait]
impl ExecChannel for K8sChannel {
    async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
        let wrapped = wrap_exec_command(&self.pod, self.container.as_deref(), cmd);
        tracing::debug!(pod = %self.pod, "k8s exec");
        self.base.run(&wrapped).await
    }

    async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.base.connect().await
    }

    async fn disconnect(&self) {
        self.base.disconnect().await;
    }

    async fn is_alive(&self) -> bool {
        self.base.is_alive().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_exec_command_without_container() {
        assert_eq!(
            wrap_exec_command("svc-abc", None, "jstat -gcutil 1"),
            "kubectl exec 'svc-abc' -- sh -c 'jstat -gcutil 1'"
        );
    }

    #[test]
    fn test_wrap_exec_command_with_container() {
        assert_eq!(
            wrap_exec_command("svc-abc", Some("main"), "ps -ef"),
            "kubectl exec -c 'main' 'svc-abc' -- sh -c 'ps -ef'"
        );
    }

    #[test]
    fn test_wrap_exec_command_escapes_inner_quotes() {
        let wrapped = wrap_exec_command("p", None, "grep 'x' /log/a");
        assert!(wrapped.contains(r"-- sh -c 'grep '\''x'\'' /log/a'"), "got: {wrapped}");
    }

    #[test]
    fn test_pkill_pattern_escapes_regex_metachars() {
        assert_eq!(pkill_pattern("/jdk-21.0.11+9/bin/jcmd 1 GC.heap_dump"), "/jdk-21\\.0\\.11\\+9/bin/jcmd 1 GC\\.heap_dump");
    }

    #[test]
    fn test_validate_pod_path() {
        assert!(validate_pod_path("/opt/log/dump/heapdump/x").is_ok());
        assert!(validate_pod_path("relative/x").is_err());
        assert!(validate_pod_path("/a\0b").is_err());
    }

    mod run_tests {
        use super::super::*;
        use crate::exec::channel::ExecChannel;
        use async_trait::async_trait;

        /// 记录所有 run 调用的 mock base
        struct RecordingBase {
            runs: tokio::sync::Mutex<Vec<String>>,
        }

        #[async_trait]
        impl ExecChannel for RecordingBase {
            async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
                self.runs.lock().await.push(cmd.to_string());
                Ok(ExecOutput { stdout: String::new(), stderr: String::new(), exit_code: 0 })
            }
            async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
            async fn disconnect(&self) {}
            async fn is_alive(&self) -> bool { true }
        }

        #[tokio::test]
        async fn test_run_delegates_wrapped_command_to_base() {
            let base = Arc::new(RecordingBase { runs: tokio::sync::Mutex::new(Vec::new()) });
            let ch = K8sChannel { base: base.clone(), pod: "svc-1".into(), container: None };
            ch.run("jstat -gcutil 7").await.unwrap();
            let runs = base.runs.lock().await;
            assert_eq!(runs[0], "kubectl exec 'svc-1' -- sh -c 'jstat -gcutil 7'");
        }

        #[tokio::test]
        async fn test_run_with_container_flag() {
            let base = Arc::new(RecordingBase { runs: tokio::sync::Mutex::new(Vec::new()) });
            let ch = K8sChannel { base: base.clone(), pod: "svc-1".into(), container: Some("main".into()) };
            ch.run("ps -ef").await.unwrap();
            let runs = base.runs.lock().await;
            assert_eq!(runs[0], "kubectl exec -c 'main' 'svc-1' -- sh -c 'ps -ef'");
        }
    }
}
