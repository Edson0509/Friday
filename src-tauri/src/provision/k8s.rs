//! K8s 容器内 JDK 装备：与 VM 模式（provision::jdk::JdkPackage）同构，
//! 差异点：musl 保险丝、容器自带工具优先、--no-same-owner 解压、chgrp 属组修正。
//! 固定目录 /opt/log/dump/heapdump/friday-tools（用户指定，不触发驱逐）。

use crate::exec::k8s::OSS_GROUP;
use crate::provision::jdk::{
    bins_for, build_download_url, jdk_home_for, run_remote, try_remote_download, JdkPackage,
    JDK_TOOL_NAME, JvmProbe,
};
use crate::provision::package::{
    emit_progress, ProvisionContext, ProvisionError, ProvisionResult, ToolPackage,
};
use async_trait::async_trait;
use std::time::Duration;

// Task 8 接线（ensure_tool 按 pod 分发）前无生产调用点
#[allow(dead_code)]
const MUSL_PROBE_PATH: &str = "/lib/ld-musl-x86_64.so.1";
#[allow(dead_code)]
const JDK_TARBALL_MIN_BYTES: u64 = 50 * 1024 * 1024;

// Task 8 接线（ensure_tool 按 pod 分发）前无生产调用点
#[allow(dead_code)]
pub struct K8sJdkPackage;

#[async_trait]
impl ToolPackage for K8sJdkPackage {
    fn name(&self) -> &str {
        "jdk"
    }

    async fn probe(&self, ctx: &ProvisionContext, java_bin: &str) -> Result<JvmProbe, ProvisionError> {
        JdkPackage.probe(ctx, java_bin).await
    }

    async fn ensure(&self, ctx: &ProvisionContext, java_bin: &str) -> Result<ProvisionResult, ProvisionError> {
        let start = std::time::Instant::now();
        let dir = ctx.remote_tools_dir.clone(); // ensure_tool 按 pod 分发保证 = POD_TOOLS_DIR
        let probe = self.probe(ctx, java_bin).await?;
        let v = probe.openjdk_version.as_str();
        let home = jdk_home_for(&dir, v);
        let tarball = format!("{dir}/jdk-{v}.tar.gz");

        // ① musl 保险丝（glibc JDK 在 musl 容器报 "No such file or directory"，误导性故障前置拦截）
        emit_progress(ctx, JDK_TOOL_NAME, "musl_check", "checking container libc flavor");
        let musl = run_remote(
            ctx,
            &format!("test -f {MUSL_PROBE_PATH}"),
            Duration::from_secs(ctx.timeouts.probe),
            "musl_check",
        )
        .await?;
        if musl.exit_code == 0 {
            return Err(ProvisionError::new(
                "unsupported_libc",
                "musl_check",
                format!("容器为 musl 底座（存在 {MUSL_PROBE_PATH}），glibc 构建的 JDK 不兼容，本期不支持"),
            ));
        }

        // ② 容器自带工具优先（零上传）
        emit_progress(ctx, JDK_TOOL_NAME, "check_native", "checking container-native jcmd/jstat");
        let native = run_remote(
            ctx,
            "command -v jcmd && command -v jstat",
            Duration::from_secs(ctx.timeouts.probe),
            "check_native",
        )
        .await?;
        if native.exit_code == 0 {
            let lines: Vec<&str> = native.stdout.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
            if lines.len() >= 2 {
                let mut bins = std::collections::HashMap::new();
                bins.insert("jcmd".to_string(), lines[0].to_string());
                bins.insert("jstat".to_string(), lines[1].to_string());
                tracing::info!(env_id = %ctx.env_id, jcmd = %lines[0], "using container-native jcmd/jstat");
                return Ok(ProvisionResult {
                    cached: true,
                    bins,
                    tool_home: dir.clone(),
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    java_version: probe.openjdk_version.clone(),
                    bisheng_version: probe.bisheng_version.clone(),
                    arch: probe.arch.clone(),
                    tool: "jdk".to_string(),
                });
            }
        }

        // ③ Pod 内缓存检查（幂等；Pod 重启内容丢失后自动重装）
        emit_progress(ctx, JDK_TOOL_NAME, "check_cache", &format!("checking {home}/bin/jcmd"));
        let check = run_remote(
            ctx,
            &format!("mkdir -p {dir} && test -x {home}/bin/jcmd"),
            Duration::from_secs(ctx.timeouts.probe),
            "check_cache",
        )
        .await?;
        if check.exit_code == 0 {
            return Ok(ProvisionResult {
                cached: true,
                bins: bins_for(&home),
                tool_home: home,
                elapsed_ms: start.elapsed().as_millis() as u64,
                java_version: probe.openjdk_version.clone(),
                bisheng_version: probe.bisheng_version.clone(),
                arch: probe.arch.clone(),
                tool: "jdk".to_string(),
            });
        }

        // ④ URL + 通道 A（容器内 curl/wget，通常缺失）
        let url = build_download_url(&ctx.artifactory_base_url, &probe)
            .map_err(|e| ProvisionError::new("parse_failed", "resolve_url", e))?;
        emit_progress(ctx, JDK_TOOL_NAME, "download", "channel A: container curl/wget");
        if let Err(a_err) = try_remote_download(ctx, &url, &tarball).await {
            tracing::warn!(session_id = %ctx.session_id, env_id = %ctx.env_id, error = %a_err, "channel A failed, falling back to channel B");
            // ⑤ 通道 B：本地下载 → K8sChannel.upload（两跳 + chgrp，已实现）
            emit_progress(ctx, JDK_TOOL_NAME, "download", "channel B: local download + two-leg upload into pod");
            let local = crate::provision::transfer::download_to_cache(&url, &ctx.cache_dir)
                .map_err(|e| ProvisionError {
                    url: Some(url.clone()),
                    ..ProvisionError::new("provision_failed", "download_local", e)
                })?;
            if let Err(e) = crate::provision::transfer::validate_download(&local, JDK_TARBALL_MIN_BYTES) {
                tracing::warn!(session_id = %ctx.session_id, env_id = %ctx.env_id, path = %local.display(), error = %e, "local cached tarball failed validation, removing");
                let _ = std::fs::remove_file(&local);
                return Err(ProvisionError {
                    url: Some(url.clone()),
                    ..ProvisionError::new("provision_failed", "download_local", e)
                });
            }
            ctx.channel.upload(&local, &tarball).await.map_err(|e| {
                let ch = ctx.channel.clone();
                let cleanup = tarball.clone();
                tokio::spawn(async move {
                    let _ = ch.run(&format!("rm -f {cleanup}")).await;
                });
                ProvisionError {
                    url: Some(url.clone()),
                    ..ProvisionError::new("provision_failed", "upload", e.to_string())
                }
            })?;
        }

        // ⑥ 解压（--no-same-owner）+ 目录规范化 + 清 tar 包 + 属组修正（一条命令原子完成）
        emit_progress(ctx, JDK_TOOL_NAME, "extract", &format!("extracting {tarball}"));
        let extract_cmd = format!(
            "mkdir -p {dir} && cd {dir} && \
             tar --no-same-owner -xzf jdk-{v}.tar.gz && \
             topdir=$(tar -tzf jdk-{v}.tar.gz | head -1 | cut -f1 -d'/') && \
             if [ \"$topdir\" != \"jdk-{v}\" ] && [ -d \"$topdir\" ]; then rm -rf jdk-{v} && mv \"$topdir\" jdk-{v}; fi && \
             rm -f jdk-{v}.tar.gz && \
             chgrp -R {g} jdk-{v} && chmod -R g+rX jdk-{v}",
            g = OSS_GROUP,
        );
        let extract = run_remote(ctx, &extract_cmd, Duration::from_secs(ctx.timeouts.extract), "extract").await?;
        if extract.exit_code != 0 {
            // 失败不留半截（异步清理，与 VM 模式同款）
            let ch = ctx.channel.clone();
            let cleanup_home = home.clone();
            tokio::spawn(async move {
                let _ = ch.run(&format!("rm -rf {cleanup_home}")).await;
            });
            let stage = if extract.stderr.contains("chgrp") || extract.stderr.contains("Operation not permitted") {
                "ownership"
            } else {
                "extract"
            };
            return Err(ProvisionError::new(
                "provision_failed",
                stage,
                format!("tar/chgrp failed (exit {}): {}", extract.exit_code, extract.stderr),
            ));
        }

        // ⑦ 验证
        emit_progress(ctx, JDK_TOOL_NAME, "verify", &format!("verifying {home}/bin/jcmd"));
        let verify = run_remote(
            ctx,
            &format!("test -x {home}/bin/jcmd && test -x {home}/bin/jstat"),
            Duration::from_secs(ctx.timeouts.verify),
            "verify",
        )
        .await?;
        if verify.exit_code != 0 {
            return Err(ProvisionError::new(
                "provision_failed",
                "verify",
                format!("jdk binaries missing after extract; check artifactory base url setting ({})", ctx.artifactory_base_url),
            ));
        }

        Ok(ProvisionResult {
            cached: false,
            bins: bins_for(&home),
            tool_home: home,
            elapsed_ms: start.elapsed().as_millis() as u64,
            java_version: probe.openjdk_version,
            bisheng_version: probe.bisheng_version,
            arch: probe.arch,
            tool: "jdk".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::channel::{ExecChannel, ExecOutput};
    use crate::provision::package::StageTimeouts;
    use async_trait::async_trait;
    use std::path::Path;
    use std::sync::Arc;

    /// 可编排 run 响应 + 记录 run/upload 的通道
    struct ScriptedChannel {
        script: std::sync::Mutex<std::collections::VecDeque<ExecOutput>>,
        runs: tokio::sync::Mutex<Vec<String>>,
        uploads: tokio::sync::Mutex<Vec<(std::path::PathBuf, String)>>,
    }

    impl ScriptedChannel {
        fn new(script: Vec<(&str, i32)>) -> Self {
            Self {
                script: std::sync::Mutex::new(
                    script
                        .into_iter()
                        .map(|(out, code)| ExecOutput { stdout: out.to_string(), stderr: String::new(), exit_code: code })
                        .collect(),
                ),
                runs: tokio::sync::Mutex::new(Vec::new()),
                uploads: tokio::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ExecChannel for ScriptedChannel {
        async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            self.runs.lock().await.push(cmd.to_string());
            Ok(self.script.lock().unwrap().pop_front().unwrap_or(ExecOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 1,
            }))
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

    fn ctx(channel: Arc<ScriptedChannel>) -> ProvisionContext {
        ProvisionContext {
            session_id: "s1".into(),
            env_id: "env-1".into(),
            channel,
            cache_dir: std::path::PathBuf::from("/tmp/unused-cache"),
            artifactory_base_url: "https://artifactory.example.com/artifactory/release".into(),
            arthas_zip: None,
            remote_tools_dir: "/opt/log/dump/heapdump/friday-tools".into(),
            timeouts: StageTimeouts::default(),
            bus: crate::app::events::EventBus::disabled(),
        }
    }

    const PROBE_OUT: &str = "BiSheng_JDK_Enterprise_205.2.0.110.B001\nopenjdk version \"21.0.11\" 2025-04-15\n---\nx86_64\n";

    #[tokio::test]
    async fn test_musl_container_rejected_upfront() {
        // ①probe ok ②musl 探测命中（exit 0）
        let ch = Arc::new(ScriptedChannel::new(vec![(PROBE_OUT, 0), ("", 0)]));
        let err = K8sJdkPackage.ensure(&ctx(ch), "java").await.unwrap_err();
        assert_eq!(err.code, "unsupported_libc");
        assert_eq!(err.stage, "musl_check");
    }

    #[tokio::test]
    async fn test_container_native_jcmd_short_circuits() {
        // ①probe ②musl 无（exit 1） ③自带 jcmd/jstat 命中
        let ch = Arc::new(ScriptedChannel::new(vec![
            (PROBE_OUT, 0),
            ("", 1),
            ("/usr/bin/jcmd\n/usr/bin/jstat\n", 0),
        ]));
        let result = K8sJdkPackage.ensure(&ctx(ch.clone()), "java").await.unwrap();
        assert!(result.cached);
        assert_eq!(result.bins["jcmd"], "/usr/bin/jcmd");
        assert_eq!(result.bins["jstat"], "/usr/bin/jstat");
        // 零上传、零下载
        assert!(ch.uploads.lock().await.is_empty());
        assert!(!ch.runs.lock().await.iter().any(|c| c.contains("curl") || c.contains("wget")));
    }

    #[tokio::test]
    async fn test_full_path_channel_b_upload_and_chgrp_extract() {
        // ①probe ②musl ③native miss ④缓存 miss ⑤无 curl/wget（通道 A 失败）
        // ⑥extract（含 chgrp） ⑦verify
        let ch = Arc::new(ScriptedChannel::new(vec![
            (PROBE_OUT, 0),
            ("", 1),
            ("", 1),
            ("", 1),
            ("", 1),
            ("", 0),
            ("", 0),
        ]));
        // 预置本地缓存 tarball（>50MB）让通道 B 的 download_to_cache 直接命中
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let probe = crate::provision::jdk::parse_probe_output(PROBE_OUT, "").unwrap();
        let url = build_download_url("https://artifactory.example.com/artifactory/release", &probe).unwrap();
        let dest = crate::provision::transfer::cache_path_for(&cache, &url);
        std::fs::write(&dest, vec![0u8; (JDK_TARBALL_MIN_BYTES + 1024) as usize]).unwrap();
        let mut pctx = ctx(ch.clone());
        pctx.cache_dir = cache;

        let result = K8sJdkPackage.ensure(&pctx, "java").await.unwrap();
        assert!(!result.cached);
        assert_eq!(result.tool_home, "/opt/log/dump/heapdump/friday-tools/jdk-21.0.11");
        // 两跳上传发生（tarball 落 Pod 内工具目录）
        let uploads = ch.uploads.lock().await;
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].1, "/opt/log/dump/heapdump/friday-tools/jdk-21.0.11.tar.gz");
        // 解压命令含 --no-same-owner 与 chgrp
        let runs = ch.runs.lock().await;
        let extract = runs.iter().find(|c| c.contains("tar --no-same-owner")).expect("extract cmd");
        assert!(extract.contains("chgrp -R ossgroup"), "extract: {extract}");
        assert!(extract.contains("chmod -R g+rX"), "extract: {extract}");
    }

    #[tokio::test]
    async fn test_extract_failure_cleans_home() {
        // ①probe ②musl ③native ④cache ⑤无下载器 ⑥extract exit 1
        let ch = Arc::new(ScriptedChannel::new(vec![
            (PROBE_OUT, 0),
            ("", 1),
            ("", 1),
            ("", 1),
            ("", 1),
            ("chgrp: Operation not permitted", 1),
        ]));
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let probe = crate::provision::jdk::parse_probe_output(PROBE_OUT, "").unwrap();
        let url = build_download_url("https://artifactory.example.com/artifactory/release", &probe).unwrap();
        let dest = crate::provision::transfer::cache_path_for(&cache, &url);
        std::fs::write(&dest, vec![0u8; (JDK_TARBALL_MIN_BYTES + 1024) as usize]).unwrap();
        let mut pctx = ctx(ch.clone());
        pctx.cache_dir = cache;

        let err = K8sJdkPackage.ensure(&pctx, "java").await.unwrap_err();
        assert_eq!(err.code, "provision_failed");
        // 失败不留半截：清理命令在编排里（异步 spawn，稍等验证）
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let runs = ch.runs.lock().await;
        assert!(
            runs.iter().any(|c| c.contains("rm -rf /opt/log/dump/heapdump/friday-tools/jdk-21.0.11")),
            "cleanup must fire: {runs:?}"
        );
    }
}
