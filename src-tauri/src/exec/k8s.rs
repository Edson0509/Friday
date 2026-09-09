//! K8s 容器通道：包装到宿主机的 SSH 通道，把命令透明转发进 Pod。
//! 分发规则：pod 参数存在 → K8sChannel（spec：2026-09-08-k8s-container-tooling-design.md）。

use async_trait::async_trait;
use std::sync::Arc;

use super::channel::{ExecChannel, ExecOutput};
use super::ssh::shell_quote_single;

/// 宿主机侧暂存目录（两跳传输中转）
pub const STAGING_DIR: &str = "/tmp/friday-tools/staging";

/// Pod 内 Friday 工具目录（用户指定：该目录不会触发 ephemeral-storage 驱逐）
pub const POD_TOOLS_DIR: &str = "/opt/log/dump/coredump/friday-tools";

/// Pod 内 dump 产物目录（heap dump / JFR 容器目标落这里；用户环境实际只有该目录）
pub const POD_DUMP_DIR: &str = "/opt/log/dump/coredump";

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

/// pkill -f 的 pattern：ERE 元字符转义 + 首字符 bracket 化。
/// bracket 化是 pkill 自排除惯用法：wrapper `sh -c 'pkill -f [j]stat ...'`
/// 的 cmdline 含 `[j]stat` 字面量，pattern `[j]stat` 匹配 `jstat` 但不匹配
/// 自身 cmdline，避免 wrapper 被 SIGTERM 导致 exit 143 误导日志。
pub fn pkill_pattern(command: &str) -> String {
    let mut out = String::with_capacity(command.len() + 8);
    for (i, c) in command.char_indices() {
        if i == 0 {
            out.push('[');
            out.push(c);
            out.push(']');
            continue;
        }
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
        tracing::debug!(pod = %self.pod, container = self.container.as_deref().unwrap_or("-"), "k8s exec");
        self.base.run(&wrapped).await
    }

    async fn upload(
        &self,
        local: &std::path::Path,
        remote_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        validate_pod_path(remote_path)?;
        let basename = remote_path.rsplit('/').next().unwrap_or("file");
        let staging = format!("{}/{}-{}", STAGING_DIR, uuid::Uuid::new_v4(), basename);

        // ① 宿主机 staging 目录
        self.base.run(&format!("mkdir -p {}", shell_quote_single(STAGING_DIR))).await?;

        // ② leg A：SFTP → 宿主机 staging
        self.base.upload(local, &staging).await?;

        // ③ leg B：kubectl exec -i 注入容器。stdin 重定向发生在宿主机 bash 上，
        //    数据不流经 Friday 内存（dump 级大文件安全）；容器内依赖仅 sh + cat。
        let parent = match remote_path.rfind('/') {
            Some(0) => "/".to_string(),
            Some(i) => remote_path[..i].to_string(),
            None => "/".to_string(),
        };
        let inner = format!(
            "mkdir -p {} && cat > {}",
            shell_quote_single(&parent),
            shell_quote_single(remote_path)
        );
        let host_cmd = format!(
            "kubectl exec -i {}{} -- sh -c {} < {}",
            self.ctr_flag(),
            shell_quote_single(&self.pod),
            shell_quote_single(&inner),
            shell_quote_single(&staging)
        );
        let out = self.base.run(&host_cmd).await?;

        // staging 清理（成败都清）
        let _ = self
            .base
            .run(&format!("rm -f {}", shell_quote_single(&staging)))
            .await;

        if out.exit_code != 0 {
            tracing::warn!(pod = %self.pod, remote_path, exit_code = out.exit_code, stderr = %out.stderr, "k8s upload: kubectl exec -i failed");
            // 半截文件兜底清理（经 kubectl exec，容器内）
            let _ = self.run(&format!("rm -f {}", shell_quote_single(remote_path))).await;
            return Err(format!(
                "k8s upload: kubectl exec -i failed (exit {}): {}",
                out.exit_code, out.stderr
            )
            .into());
        }

        // ④ 属组修正（spec：chgrp 失败 = 上传失败，清理目标文件）
        let q = shell_quote_single(remote_path);
        let fix = format!("chgrp {OSS_GROUP} {q} && chmod g+r {q}");
        let gout = self.run(&fix).await?;
        if gout.exit_code != 0 {
            tracing::warn!(pod = %self.pod, remote_path, stderr = %gout.stderr, "k8s upload: chgrp failed");
            let _ = self.run(&format!("rm -f {q}")).await;
            return Err(format!(
                "k8s upload: chgrp {OSS_GROUP} failed (exec 用户可能不在 {OSS_GROUP} 组): {}",
                gout.stderr
            )
            .into());
        }
        Ok(())
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
        assert_eq!(pkill_pattern("/jdk-21.0.11+9/bin/jcmd 1 GC.heap_dump"), "[/]jdk-21\\.0\\.11\\+9/bin/jcmd 1 GC\\.heap_dump");
    }

    #[test]
    fn test_pkill_pattern_brackets_first_char_for_self_exclusion() {
        let p = pkill_pattern("jstat -gcutil 1");
        assert!(p.starts_with("[j]"), "first char must be bracketed for self-exclusion: {p}");
        // wrapper cmdline 含 [j]stat 字面量，pattern [j]stat 不匹配它
        assert!(!p.contains("[j][j]"), "no double bracketing: {p}");
    }

    #[test]
    fn test_validate_pod_path() {
        assert!(validate_pod_path("/opt/log/dump/coredump/x").is_ok());
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

    mod upload_tests {
        use super::super::*;
        use crate::exec::channel::ExecChannel;
        use async_trait::async_trait;
        use std::path::Path;

        /// 可编排响应的 base：run 第 n 次返回脚本第 n 条 (stdout, exit_code)；
        /// upload 记录 (local, remote)。默认 run 返回 exit 0。
        struct ScriptedBase {
            script: std::sync::Mutex<std::collections::VecDeque<(String, i32)>>,
            runs: tokio::sync::Mutex<Vec<String>>,
            uploads: tokio::sync::Mutex<Vec<(std::path::PathBuf, String)>>,
        }

        impl ScriptedBase {
            fn new(script: Vec<(&str, i32)>) -> Self {
                Self {
                    script: std::sync::Mutex::new(
                        script.into_iter().map(|(s, c)| (s.to_string(), c)).collect(),
                    ),
                    runs: tokio::sync::Mutex::new(Vec::new()),
                    uploads: tokio::sync::Mutex::new(Vec::new()),
                }
            }
        }

        #[async_trait]
        impl ExecChannel for ScriptedBase {
            async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
                self.runs.lock().await.push(cmd.to_string());
                let (stdout, exit_code) = self
                    .script
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or((String::new(), 0));
                Ok(ExecOutput { stdout, stderr: String::new(), exit_code })
            }
            async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
            async fn disconnect(&self) {}
            async fn is_alive(&self) -> bool { true }
            async fn upload(&self, local: &Path, remote: &str)
                -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                self.uploads.lock().await.push((local.to_path_buf(), remote.to_string()));
                Ok(())
            }
        }

        fn chan(script: Vec<(&str, i32)>) -> (Arc<ScriptedBase>, K8sChannel) {
            let base = Arc::new(ScriptedBase::new(script));
            let ch = K8sChannel { base: base.clone(), pod: "svc-1".into(), container: None };
            (base, ch)
        }

        #[tokio::test]
        async fn test_upload_happy_path_two_legs_and_chgrp() {
            let (base, ch) = chan(vec![]);
            ch.upload(Path::new("/local/jdk.tar.gz"), "/opt/log/dump/coredump/friday-tools/jdk.tar.gz")
                .await
                .unwrap();
            // leg A：SFTP 到宿主机 staging（路径含随机前缀）
            let uploads = base.uploads.lock().await;
            assert_eq!(uploads.len(), 1);
            assert!(uploads[0].1.starts_with("/tmp/friday-tools/staging/"), "staging: {}", uploads[0].1);
            assert!(uploads[0].1.ends_with("-jdk.tar.gz"));
            // leg B：kubectl exec -i + host 侧重定向 + 父目录创建
            let runs = base.runs.lock().await;
            let host_leg = runs.iter().find(|c| c.contains("kubectl exec -i")).expect("host leg");
            assert!(host_leg.contains("< "), "host stdin redirect: {host_leg}");
            assert!(host_leg.contains(r"cat > '\''/opt/log/dump/coredump/friday-tools/jdk.tar.gz'\''"), "{host_leg}");
            assert!(host_leg.contains(r"mkdir -p '\''/opt/log/dump/coredump/friday-tools'\''"), "{host_leg}");
            // 属组修正走 kubectl exec（容器内，不是宿主机）
            let chgrp = runs.iter().find(|c| c.contains("chgrp ossgroup")).expect("chgrp leg");
            assert!(chgrp.contains("kubectl exec"), "chgrp must run inside pod: {chgrp}");
            assert!(chgrp.contains("chmod g+r"));
            // staging 清理
            assert!(runs.iter().any(|c| c.contains("rm -f '/tmp/friday-tools/staging/")));
        }

        #[tokio::test]
        async fn test_upload_host_leg_failure_cleans_remote_and_errors() {
            // 脚本顺序：①mkdir staging ②kubectl exec -i（exit 1）③rm staging ④rm remote（补刀清理）
            let (base, ch) = chan(vec![("", 0), ("", 1), ("", 0), ("", 0)]);
            let err = ch
                .upload(Path::new("/local/x"), "/opt/log/dump/coredump/friday-tools/x")
                .await
                .unwrap_err();
            assert!(err.to_string().contains("kubectl exec -i failed"), "err: {err}");
            let runs = base.runs.lock().await;
            assert!(runs.iter().any(|c| c.contains("kubectl exec") && c.contains(r"rm -f '\''/opt/log/dump/coredump/friday-tools/x'\''")), "remote cleanup: {runs:?}");
        }

        #[tokio::test]
        async fn test_upload_chgrp_failure_cleans_remote_and_errors() {
            // ①mkdir ②kubectl -i ok ③rm staging ④chgrp(exit 1) ⑤rm remote
            let (base, ch) = chan(vec![("", 0), ("", 0), ("", 0), ("", 1), ("", 0)]);
            let err = ch
                .upload(Path::new("/local/x"), "/opt/log/dump/coredump/friday-tools/x")
                .await
                .unwrap_err();
            assert!(err.to_string().contains("chgrp ossgroup failed"), "err: {err}");
            let runs = base.runs.lock().await;
            assert!(runs.iter().any(|c| c.contains("kubectl exec") && c.contains(r"rm -f '\''/opt/log/dump/coredump/friday-tools/x'\''")), "remote cleanup: {runs:?}");
        }

        #[tokio::test]
        async fn test_upload_rejects_relative_path() {
            let (_base, ch) = chan(vec![]);
            let err = ch.upload(Path::new("/local/x"), "relative/x").await.unwrap_err();
            assert!(err.to_string().contains("absolute"), "err: {err}");
        }
    }
}
