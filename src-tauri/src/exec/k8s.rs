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
/// upload/download = 两跳传输（宿主机 staging 中转）。连接生命周期完全委托 base SSH 通道。
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

        // ① 宿主机 staging 目录（显式检查退出码：mkdir 失败时后续两跳都会连环失败）
        let mkdir_out = self.base.run(&format!("mkdir -p {}", shell_quote_single(STAGING_DIR))).await?;
        if mkdir_out.exit_code != 0 {
            tracing::warn!(pod = %self.pod, exit_code = mkdir_out.exit_code, stderr = %mkdir_out.stderr, "k8s upload: staging mkdir failed");
            return Err(format!(
                "k8s upload: staging mkdir {} failed (exit {}): {}",
                STAGING_DIR, mkdir_out.exit_code, mkdir_out.stderr
            )
            .into());
        }

        // ② leg A：SFTP → 宿主机 staging
        tracing::info!(
            pod = %self.pod,
            local = %local.display(),
            staging = %staging,
            bytes = std::fs::metadata(local).map(|m| m.len()).unwrap_or(0),
            "k8s upload: leg A sftp to host staging"
        );
        if let Err(e) = self.base.upload(local, &staging).await {
            tracing::warn!(pod = %self.pod, staging = %staging, error = %e, "k8s upload: leg A sftp to staging failed");
            return Err(format!("k8s upload: leg A sftp to staging {staging} failed: {e}").into());
        }

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
        let leg_b_start = std::time::Instant::now();
        let out = self.base.run(&host_cmd).await?;
        let leg_b_elapsed_ms = leg_b_start.elapsed().as_millis() as u64;

        // staging 清理（成败都清）
        let _ = self
            .base
            .run(&format!("rm -f {}", shell_quote_single(&staging)))
            .await;

        if out.exit_code != 0 {
            tracing::warn!(pod = %self.pod, remote_path, staging = %staging, exit_code = out.exit_code, stderr = %out.stderr, elapsed_ms = leg_b_elapsed_ms, "k8s upload: leg B kubectl exec -i failed");
            // 半截文件兜底清理（经 kubectl exec，容器内）
            let _ = self.run(&format!("rm -f {}", shell_quote_single(remote_path))).await;
            return Err(format!(
                "k8s upload: leg B kubectl exec -i failed (exit {}): {}",
                out.exit_code, out.stderr
            )
            .into());
        }
        tracing::info!(pod = %self.pod, remote_path, elapsed_ms = leg_b_elapsed_ms, "k8s upload: leg B kubectl exec -i done, fixing group");

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

    async fn download(
        &self,
        remote_path: &str,
        local: &std::path::Path,
        offset: u64,
        progress: &(dyn Fn(u64, u64) + Sync),
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        validate_pod_path(remote_path)?;
        let basename = remote_path.rsplit('/').next().unwrap_or("file");
        let staging = format!("{}/{}-{}", STAGING_DIR, uuid::Uuid::new_v4(), basename);
        let q = shell_quote_single(remote_path);
        let staging_q = shell_quote_single(&staging);

        // ① Pod 内文件大小（完整性校验基准；失败说明文件不存在或 stat 缺失）
        let stat_out = self
            .run(&format!("stat -c %s {q}"))
            .await
            .map_err(|e| format!("k8s download: stat {remote_path} failed: {e}"))?;
        if stat_out.exit_code != 0 {
            tracing::warn!(pod = %self.pod, remote_path, exit_code = stat_out.exit_code, stderr = %stat_out.stderr, "k8s download: pod-side stat failed");
            return Err(format!(
                "k8s download: 容器内文件不存在或不可读 {remote_path} (exit {}): {}",
                stat_out.exit_code, stat_out.stderr
            )
            .into());
        }
        let pod_size: u64 = stat_out.stdout.trim().parse().map_err(|_| {
            format!("k8s download: unexpected stat output: {:?}", stat_out.stdout)
        })?;

        // ② leg1：kubectl exec cat → 宿主机 staging（重定向在宿主机 bash 侧，
        //    数据不流经 Friday 内存）。容器内依赖仅 cat。
        //    注：base.run 持连接锁期间无法并发轮询 staging 大小，leg1 不产生
        //    进度事件（进度由 leg2 SFTP 驱动）——大文件场景 leg2（跨网）本就是
        //    瓶颈腿，可接受。重试时 leg1 整段重跑（宿主机本地，快）。
        let host_cmd = format!(
            "kubectl exec {}{} -- cat {} > {}",
            self.ctr_flag(),
            shell_quote_single(&self.pod),
            q,
            staging_q,
        );
        let out = self.base.run(&host_cmd).await?;
        if out.exit_code != 0 {
            tracing::warn!(pod = %self.pod, remote_path, staging = %staging, exit_code = out.exit_code, stderr = out.stderr, "k8s download: leg 1 kubectl exec cat failed");
            // staging 半截清理（best-effort）
            let _ = self.base.run(&format!("rm -f {staging_q}")).await;
            return Err(format!(
                "k8s download: leg 1 (kubectl exec cat) failed (exit {}): {}",
                out.exit_code, out.stderr
            )
            .into());
        }

        // ③ leg2：宿主机 staging → Friday 本地（现有 SFTP，offset 续传 + progress）
        if let Err(e) = self.base.download(&staging, local, offset, progress).await {
            tracing::warn!(pod = %self.pod, remote_path, staging = %staging, error = %e, "k8s download: leg 2 sftp failed");
            return Err(format!(
                "k8s download: leg 2 (sftp from host staging {staging}) failed: {e}"
            )
            .into());
        }

        // ④ staging 清理（best-effort，成败不影响结果）
        let _ = self.base.run(&format!("rm -f {staging_q}")).await;

        // ⑤ 完整性校验：本地最终大小 == Pod 内源大小。
        //    offset 续传语义：本地文件 = 已有 offset 字节 + 本次新增，最终应为 pod_size。
        //    Pod 内源文件删除不在这里做——TransferState.cleanup_remote_on_success
        //    走 channel.run（kubectl exec rm）由传输层负责。
        let local_size = tokio::fs::metadata(local).await.map(|m| m.len()).unwrap_or(0);
        if local_size != pod_size {
            tracing::error!(pod = %self.pod, remote_path, pod_size, local_size, offset, "k8s download: size mismatch after two-leg transfer");
            return Err(format!(
                "k8s download: size mismatch (pod {pod_size} bytes, local {local_size} bytes) for {remote_path}"
            )
            .into());
        }
        tracing::info!(pod = %self.pod, remote_path, pod_size, "k8s download: two-leg transfer done");
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

    mod download_tests {
        use super::super::*;
        use crate::exec::channel::ExecChannel;
        use async_trait::async_trait;
        use std::path::Path;

        /// 可编排响应的 base：run 第 n 次返回脚本第 n 条 (stdout, exit_code)；
        /// download 记录 (remote, local, offset) 并按 download_sizes 注入的大小
        /// 写本地文件（模拟 SFTP 落盘的最终状态，供完整性校验用例控制大小）。
        struct ScriptedBase {
            script: std::sync::Mutex<std::collections::VecDeque<(String, i32)>>,
            runs: tokio::sync::Mutex<Vec<String>>,
            downloads: tokio::sync::Mutex<Vec<(String, std::path::PathBuf, u64)>>,
            download_sizes: std::sync::Mutex<std::collections::VecDeque<u64>>,
        }

        impl ScriptedBase {
            fn new(script: Vec<(&str, i32)>) -> Self {
                Self {
                    script: std::sync::Mutex::new(
                        script.into_iter().map(|(s, c)| (s.to_string(), c)).collect(),
                    ),
                    runs: tokio::sync::Mutex::new(Vec::new()),
                    downloads: tokio::sync::Mutex::new(Vec::new()),
                    download_sizes: std::sync::Mutex::new(std::collections::VecDeque::new()),
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
            async fn download(&self, remote: &str, local: &Path, offset: u64, _progress: &(dyn Fn(u64, u64) + Sync))
                -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                self.downloads.lock().await.push((remote.to_string(), local.to_path_buf(), offset));
                let size = self.download_sizes.lock().unwrap().pop_front().unwrap_or(16);
                std::fs::write(local, vec![0u8; size as usize])?;
                Ok(())
            }
        }

        fn chan(script: Vec<(&str, i32)>) -> (Arc<ScriptedBase>, K8sChannel) {
            let base = Arc::new(ScriptedBase::new(script));
            let ch = K8sChannel { base: base.clone(), pod: "svc-1".into(), container: None };
            (base, ch)
        }

        #[tokio::test]
        async fn test_download_happy_path_two_legs() {
            // 脚本顺序：①stat（容器内，输出 1024）②leg1 kubectl exec cat ③staging rm
            let (base, ch) = chan(vec![("1024", 0), ("", 0), ("", 0)]);
            base.download_sizes.lock().unwrap().push_back(1024);
            let tmp = tempfile::tempdir().unwrap();
            let local = tmp.path().join("dump.hprof");
            ch.download("/opt/log/dump/coredump/dump.hprof", &local, 4096, &|_, _| {})
                .await
                .unwrap();
            let runs = base.runs.lock().await;
            // ① 容器内 stat（经 wrap_exec_command，sh -c 包裹 + 单引号转义）
            assert!(runs[0].contains("kubectl exec"), "stat leg: {}", runs[0]);
            assert!(runs[0].contains(r"stat -c %s '\''/opt/log/dump/coredump/dump.hprof'\''"), "stat leg: {}", runs[0]);
            // ② leg1：kubectl exec cat '路径' > 'staging路径'（重定向在宿主机侧）
            let leg1 = runs.iter().find(|c| c.contains(" -- cat ")).expect("leg 1 host cmd");
            assert!(leg1.starts_with("kubectl exec 'svc-1' -- cat "), "leg1: {leg1}");
            assert!(leg1.contains(r"cat '/opt/log/dump/coredump/dump.hprof'"), "leg1: {leg1}");
            assert!(leg1.contains("> '/tmp/friday-tools/staging/"), "leg1 host redirect: {leg1}");
            assert!(leg1.ends_with("-dump.hprof'"), "leg1 staging basename: {leg1}");
            // ③ leg2：base.download 以 staging 为远端，offset 透传
            let downloads = base.downloads.lock().await;
            assert_eq!(downloads.len(), 1);
            assert!(downloads[0].0.starts_with("/tmp/friday-tools/staging/"), "staging: {}", downloads[0].0);
            assert!(downloads[0].0.ends_with("-dump.hprof"), "staging: {}", downloads[0].0);
            assert_eq!(downloads[0].1, local);
            assert_eq!(downloads[0].2, 4096, "offset must pass through to leg 2");
            // ⑤ 完整性：本地最终大小 == Pod 内 stat 大小
            assert_eq!(std::fs::metadata(&local).unwrap().len(), 1024);
            // ④ staging 清理
            assert!(runs.iter().any(|c| c.contains("rm -f '/tmp/friday-tools/staging/")), "staging cleanup: {runs:?}");
        }

        #[tokio::test]
        async fn test_download_pod_file_missing() {
            let (base, ch) = chan(vec![("", 1)]);
            let tmp = tempfile::tempdir().unwrap();
            let err = ch
                .download("/opt/log/dump/coredump/none.hprof", &tmp.path().join("x.hprof"), 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err.to_string().contains("容器内文件不存在"), "err: {err}");
            let runs = base.runs.lock().await;
            assert_eq!(runs.len(), 1, "only the stat leg should run: {runs:?}");
            assert!(base.downloads.lock().await.is_empty(), "no leg 2");
        }

        #[tokio::test]
        async fn test_download_leg1_failure_cleans_staging() {
            // ①stat ok ②leg1 exit 1 ③staging rm
            let (base, ch) = chan(vec![("1024", 0), ("", 1), ("", 0)]);
            let tmp = tempfile::tempdir().unwrap();
            let err = ch
                .download("/opt/log/dump/coredump/d.hprof", &tmp.path().join("d.hprof"), 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err.to_string().contains("leg 1"), "err: {err}");
            let runs = base.runs.lock().await;
            assert!(
                runs.iter().any(|c| c.contains("rm -f '/tmp/friday-tools/staging/")),
                "half-written staging must be cleaned: {runs:?}"
            );
            assert!(base.downloads.lock().await.is_empty(), "no leg 2");
        }

        #[tokio::test]
        async fn test_download_size_mismatch_detected() {
            // stat 说 1024，leg2 只落盘 512
            let (base, ch) = chan(vec![("1024", 0), ("", 0), ("", 0)]);
            base.download_sizes.lock().unwrap().push_back(512);
            let tmp = tempfile::tempdir().unwrap();
            let err = ch
                .download("/opt/log/dump/coredump/d.hprof", &tmp.path().join("d.hprof"), 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err.to_string().contains("size mismatch"), "err: {err}");
        }

        #[tokio::test]
        async fn test_download_rejects_relative_path() {
            let (base, ch) = chan(vec![]);
            let tmp = tempfile::tempdir().unwrap();
            let err = ch
                .download("relative/x", &tmp.path().join("x"), 0, &|_, _| {})
                .await
                .unwrap_err();
            assert!(err.to_string().contains("absolute"), "err: {err}");
            assert!(base.runs.lock().await.is_empty(), "no commands should run");
            assert!(base.downloads.lock().await.is_empty(), "no downloads");
        }
    }
}
