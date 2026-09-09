use crate::provision::package::{
    emit_progress, ProvisionContext, ProvisionError, ProvisionResult, ToolPackage,
};
use crate::provision::jdk::{run_remote, JvmProbe, REMOTE_TOOLS_DIR};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

/// arthas 版本（官方 arthas-bin.zip 对应版本；升级只改这里 + 替换 vendored 包）
pub const ARTHAS_VERSION: &str = "4.3.5";
/// 进度事件携带的工具名：与 MCP 工具名一致（前端按 tool.name 匹配工具卡片）
pub const ARTHAS_TOOL_NAME: &str = "arthas_open";

pub fn arthas_home() -> String {
    format!("{REMOTE_TOOLS_DIR}/arthas-{ARTHAS_VERSION}")
}

pub struct ArthasPackage;

#[async_trait]
impl ToolPackage for ArthasPackage {
    fn name(&self) -> &str {
        "arthas"
    }

    /// arthas 包与目标 JVM 版本无关，无需探测
    async fn probe(
        &self,
        _ctx: &ProvisionContext,
        _java_bin: &str,
    ) -> Result<JvmProbe, ProvisionError> {
        Ok(JvmProbe {
            openjdk_version: String::new(),
            bisheng_version: String::new(),
            arch: String::new(),
        })
    }

    async fn ensure(
        &self,
        ctx: &ProvisionContext,
        _java_bin: &str,
    ) -> Result<ProvisionResult, ProvisionError> {
        let start = std::time::Instant::now();
        let home = arthas_home();

        // 1. 远端缓存检查
        emit_progress(ctx, ARTHAS_TOOL_NAME, "check_cache", &format!("checking {home}/arthas-boot.jar"));
        let check = run_remote(
            ctx,
            &format!("mkdir -p {REMOTE_TOOLS_DIR} && test -f {home}/arthas-boot.jar"),
            Duration::from_secs(ctx.timeouts.probe),
            "check_cache",
        )
        .await?;
        if check.exit_code == 0 {
            return Ok(ProvisionResult {
                tool: "arthas".to_string(),
                cached: true,
                java_version: String::new(),
                bisheng_version: String::new(),
                arch: String::new(),
                tool_home: home,
                bins: HashMap::new(),
                elapsed_ms: start.elapsed().as_millis() as u64,
            });
        }

        // 2. vendored zip：随应用分发的包 SFTP 直传目标机（不再依赖 artifactory）
        let zip = ctx.arthas_zip.as_ref().ok_or_else(|| ProvisionError::new(
            "vendored_package_missing",
            "vendored_package",
            format!("arthas 工具包未随应用分发（resources/arthas/arthas-bin-{ARTHAS_VERSION}.zip），请重新安装 Friday"),
        ))?;
        if let Err(e) = crate::provision::transfer::validate_download(zip, 5 * 1024 * 1024) {
            return Err(ProvisionError::new("vendored_package_corrupt", "vendored_package", e));
        }
        let remote_zip = format!("{REMOTE_TOOLS_DIR}/arthas-bin-{ARTHAS_VERSION}.zip");
        emit_progress(ctx, ARTHAS_TOOL_NAME, "upload", &format!("uploading arthas-bin-{ARTHAS_VERSION}.zip via sftp"));
        ctx.channel.upload(zip, &remote_zip).await.map_err(|e| ProvisionError::new("provision_failed", "upload", e.to_string()))?;

        // 3. 解压（unzip → python3 兜底）+ 顶层目录扁平化 + 清理
        //    find arthas-boot.jar 所在目录作为包根，兼容 zip 内有无顶层目录两种布局
        emit_progress(ctx, ARTHAS_TOOL_NAME, "extract", &format!("extracting arthas-bin-{ARTHAS_VERSION}.zip"));
        let extract_cmd = format!(
            "cd {REMOTE_TOOLS_DIR} && rm -rf arthas-tmp-{ARTHAS_VERSION} arthas-{ARTHAS_VERSION} && \
             mkdir arthas-tmp-{ARTHAS_VERSION} && \
             if command -v unzip >/dev/null 2>&1; then \
               unzip -q -o arthas-bin-{ARTHAS_VERSION}.zip -d arthas-tmp-{ARTHAS_VERSION}/; \
             elif command -v python3 >/dev/null 2>&1; then \
               python3 -m zipfile -e arthas-bin-{ARTHAS_VERSION}.zip arthas-tmp-{ARTHAS_VERSION}/; \
             else \
               echo 'neither unzip nor python3 available' >&2; exit 3; \
             fi && \
             d=$(dirname \"$(find arthas-tmp-{ARTHAS_VERSION} -name arthas-boot.jar | head -1)\") && \
             [ -n \"$d\" ] && mv \"$d\" arthas-{ARTHAS_VERSION} && \
             rm -rf arthas-tmp-{ARTHAS_VERSION} arthas-bin-{ARTHAS_VERSION}.zip && \
             chmod -R 755 arthas-{ARTHAS_VERSION}"
        );
        let extract = run_remote(ctx, &extract_cmd, Duration::from_secs(ctx.timeouts.extract), "extract").await?;
        if extract.exit_code != 0 {
            // 失败清理半成品（后台执行）
            let ch = ctx.channel.clone();
            let cleanup = format!(
                "rm -rf {REMOTE_TOOLS_DIR}/arthas-tmp-{ARTHAS_VERSION} {REMOTE_TOOLS_DIR}/arthas-{ARTHAS_VERSION}"
            );
            tokio::spawn(async move {
                let _ = ch.run(&cleanup).await;
            });
            return Err(ProvisionError::new(
                "provision_failed",
                "extract",
                format!(
                    "unzip failed (exit {}): {} —— 目标机需要 unzip 或 python3 之一",
                    extract.exit_code, extract.stderr
                ),
            ));
        }

        // 4. 验证
        emit_progress(ctx, ARTHAS_TOOL_NAME, "verify", &format!("verifying {home}/arthas-boot.jar"));
        let verify = run_remote(
            ctx,
            &format!("test -f {home}/arthas-boot.jar"),
            Duration::from_secs(ctx.timeouts.verify),
            "verify",
        )
        .await?;
        if verify.exit_code != 0 {
            return Err(ProvisionError::new(
                "provision_failed",
                "verify",
                format!("arthas-boot.jar missing after extract; check vendored arthas-bin-{ARTHAS_VERSION}.zip package layout"),
            ));
        }

        Ok(ProvisionResult {
            tool: "arthas".to_string(),
            cached: false,
            java_version: String::new(),
            bisheng_version: String::new(),
            arch: String::new(),
            tool_home: home,
            bins: HashMap::new(),
            elapsed_ms: start.elapsed().as_millis() as u64,
        })
    }
}

impl ArthasPackage {
    /// 容器环境装备：宿主机中转（SFTP zip → 宿主机 unzip+tar → kubectl exec -i 注入），
    /// 容器内依赖仅 sh + tar（精简镜像无 unzip/python3，VM 模式的"upload zip → 容器内
    /// 解压"链路不可用）。base = 宿主机通道；pod/container 定位目标容器。
    /// 幂等：Pod 内已装备直接 cached 返回（Pod 重启容器层丢失后自动重装）。
    ///
    /// 不进 ToolPackage trait（那是 VM 语义——run/upload 都作用于目标机本身）：
    /// 本流程的解包/打包是宿主机侧活动、注入的 stdin 重定向源是宿主机文件，
    /// 整条流水线以 base 通道为主轴、kubectl exec 命令内联。
    ///
    /// 调用方（arthas attach 的 k8s 分支）在 T5 接入，非测试构建下暂时豁免 dead_code 警告。
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn ensure_k8s(
        &self,
        base: &std::sync::Arc<dyn crate::exec::channel::ExecChannel>,
        pod: &str,
        container: Option<&str>,
        zip: &std::path::Path,
        session_id: &str,
        env_id: &str,
    ) -> Result<ProvisionResult, ProvisionError> {
        let start = std::time::Instant::now();
        let dist = format!("{}/arthas-dist", crate::exec::k8s::POD_TOOLS_DIR);
        let dist_q = crate::exec::ssh::shell_quote_single(&dist);
        let ctr = container
            .map(|c| format!("-c {} ", crate::exec::ssh::shell_quote_single(c)))
            .unwrap_or_default();
        let pod_q = crate::exec::ssh::shell_quote_single(pod);
        let boot = format!("{dist}/arthas-boot.jar");
        let check_cmd = format!(
            "kubectl exec {ctr}{pod_q} -- test -f {}",
            crate::exec::ssh::shell_quote_single(&boot)
        );

        // ① Pod 内缓存检查（幂等；Pod 重启后自动重装）
        let check = base.run(&check_cmd).await.map_err(|e| {
            tracing::warn!(session_id, env_id, pod, error = %e, "k8s arthas cache check exec failed");
            ProvisionError::new(
                "provision_failed",
                "check_cache",
                format!("kubectl exec 缓存检查失败（pod {pod}）: {e}"),
            )
        })?;
        if check.exit_code == 0 {
            tracing::info!(session_id, env_id, pod, home = %dist, "k8s arthas package cached in pod");
            return Ok(ProvisionResult {
                tool: "arthas".to_string(),
                cached: true,
                java_version: String::new(),
                bisheng_version: String::new(),
                arch: String::new(),
                tool_home: dist,
                bins: HashMap::new(),
                elapsed_ms: start.elapsed().as_millis() as u64,
            });
        }

        // vendored zip 完整性守卫（与 VM 模式同款）
        if let Err(e) = crate::provision::transfer::validate_download(zip, 5 * 1024 * 1024) {
            return Err(ProvisionError::new("vendored_package_corrupt", "vendored_package", e));
        }

        // ② SFTP zip → 宿主机 staging（一次性子目录，收尾整目录清理）
        let staging_dir =
            format!("{}/arthas-{}", crate::exec::k8s::STAGING_DIR, uuid::Uuid::new_v4());
        let staging_q = crate::exec::ssh::shell_quote_single(&staging_dir);
        let host_zip = format!("{staging_dir}/arthas.zip");
        let mkdir = base.run(&format!("mkdir -p {staging_q}")).await.map_err(|e| {
            tracing::warn!(session_id, env_id, pod, staging = %staging_dir, error = %e, "k8s arthas staging mkdir failed");
            ProvisionError::new(
                "provision_failed",
                "staging",
                format!("宿主机 staging 目录创建失败（pod {pod}，{staging_dir}）: {e}"),
            )
        })?;
        if mkdir.exit_code != 0 {
            tracing::warn!(session_id, env_id, pod, exit_code = mkdir.exit_code, stderr = %mkdir.stderr, "k8s arthas staging mkdir failed");
            return Err(ProvisionError::new(
                "provision_failed",
                "staging",
                format!(
                    "宿主机 staging 目录创建失败（pod {pod}，exit {}）: {}",
                    mkdir.exit_code, mkdir.stderr
                ),
            ));
        }
        if let Err(e) = base.upload(zip, &host_zip).await {
            tracing::warn!(session_id, env_id, pod, staging = %staging_dir, error = %e, "k8s arthas zip sftp upload failed");
            let _ = base.run(&format!("rm -rf {staging_q}")).await;
            return Err(ProvisionError::new(
                "provision_failed",
                "upload",
                format!("arthas zip SFTP 上传宿主机失败（pod {pod}，{host_zip}）: {e}"),
            ));
        }

        // ③ 宿主机解 zip + 打 tar（容器内无 unzip，解包在宿主机完成）
        let extract_cmd = format!(
            "cd {staging_q} && unzip -oq arthas.zip -d unzipped && tar czf arthas.tar.gz -C unzipped ."
        );
        let extract = base.run(&extract_cmd).await.map_err(|e| {
            tracing::warn!(session_id, env_id, pod, error = %e, "k8s arthas host extract failed");
            ProvisionError::new(
                "provision_failed",
                "extract",
                format!("宿主机解包/打包失败（pod {pod}）: {e}"),
            )
        })?;
        if extract.exit_code != 0 {
            tracing::warn!(session_id, env_id, pod, exit_code = extract.exit_code, stderr = %extract.stderr, "k8s arthas host extract failed");
            let _ = base.run(&format!("rm -rf {staging_q}")).await;
            return Err(ProvisionError::new(
                "provision_failed",
                "extract",
                format!(
                    "宿主机解包/打包失败（pod {pod}，exit {}）: {} —— 宿主机需要 unzip 与 tar（宿主机环境问题，非容器问题）",
                    extract.exit_code, extract.stderr
                ),
            ));
        }

        // ④ 注入容器：kubectl exec -i + 宿主机侧 stdin 重定向（数据不流经 Friday 内存）
        let inject_cmd = format!(
            "kubectl exec -i {ctr}{pod_q} -- sh -c {} < {}",
            crate::exec::ssh::shell_quote_single(&format!("mkdir -p {dist_q} && tar xz -C {dist_q}")),
            crate::exec::ssh::shell_quote_single(&format!("{staging_dir}/arthas.tar.gz")),
        );
        let inject = base.run(&inject_cmd).await.map_err(|e| {
            tracing::warn!(session_id, env_id, pod, error = %e, "k8s arthas tar inject exec failed");
            ProvisionError::new(
                "provision_failed",
                "inject",
                format!("tar 注入容器执行失败（pod {pod}）: {e}"),
            )
        })?;
        if inject.exit_code != 0 {
            tracing::warn!(session_id, env_id, pod, exit_code = inject.exit_code, stderr = %inject.stderr, "k8s arthas tar inject failed");
            Self::rm_pod_dist(base, &ctr, &pod_q, &dist_q).await;
            let _ = base.run(&format!("rm -rf {staging_q}")).await;
            return Err(ProvisionError::new(
                "provision_failed",
                "inject",
                format!(
                    "tar 注入容器失败（pod {pod}，exit {}）: {}",
                    inject.exit_code, inject.stderr
                ),
            ));
        }

        // ⑤ 属组修正（非 ossgroup 无法被目标 JVM 用户使用；容器内 sh -c，无 stdin）
        let fix_cmd = format!(
            "kubectl exec {ctr}{pod_q} -- sh -c {}",
            crate::exec::ssh::shell_quote_single(&format!(
                "chgrp -R {} {dist_q} && chmod -R g+rX {dist_q}",
                crate::exec::k8s::OSS_GROUP
            )),
        );
        let fix = base.run(&fix_cmd).await.map_err(|e| {
            tracing::warn!(session_id, env_id, pod, error = %e, "k8s arthas chgrp exec failed");
            ProvisionError::new(
                "provision_failed",
                "ownership",
                format!("chgrp {} 执行失败（pod {pod}）: {e}", crate::exec::k8s::OSS_GROUP),
            )
        })?;
        if fix.exit_code != 0 {
            tracing::warn!(session_id, env_id, pod, exit_code = fix.exit_code, stderr = %fix.stderr, "k8s arthas chgrp failed");
            Self::rm_pod_dist(base, &ctr, &pod_q, &dist_q).await;
            let _ = base.run(&format!("rm -rf {staging_q}")).await;
            return Err(ProvisionError::new(
                "provision_failed",
                "ownership",
                format!(
                    "chgrp {} 失败（pod {pod}，exit {}）: {} —— exec 用户可能不在 {} 组",
                    crate::exec::k8s::OSS_GROUP,
                    fix.exit_code,
                    fix.stderr,
                    crate::exec::k8s::OSS_GROUP
                ),
            ));
        }

        // ⑥ 验证（同①命令）
        let verify = base.run(&check_cmd).await.map_err(|e| {
            tracing::warn!(session_id, env_id, pod, error = %e, "k8s arthas verify exec failed");
            ProvisionError::new(
                "provision_failed",
                "verify",
                format!("验证命令执行失败（pod {pod}）: {e}"),
            )
        })?;
        if verify.exit_code != 0 {
            tracing::warn!(session_id, env_id, pod, exit_code = verify.exit_code, stderr = %verify.stderr, "k8s arthas verify failed");
            Self::rm_pod_dist(base, &ctr, &pod_q, &dist_q).await;
            let _ = base.run(&format!("rm -rf {staging_q}")).await;
            return Err(ProvisionError::new(
                "provision_failed",
                "verify",
                format!(
                    "注入后 arthas-boot.jar 缺失（pod {pod}，exit {}）: {} —— 检查 vendored arthas zip 包布局",
                    verify.exit_code, verify.stderr
                ),
            ));
        }

        // ⑦ 宿主机 staging 清理（best-effort，成败不阻塞）
        match base.run(&format!("rm -rf {staging_q}")).await {
            Ok(out) if out.exit_code != 0 => {
                tracing::warn!(session_id, env_id, pod, staging = %staging_dir, exit_code = out.exit_code, stderr = %out.stderr, "k8s arthas staging cleanup failed (best-effort)");
            }
            Err(e) => {
                tracing::warn!(session_id, env_id, pod, staging = %staging_dir, error = %e, "k8s arthas staging cleanup exec failed (best-effort)");
            }
            _ => {}
        }

        tracing::info!(session_id, env_id, pod, home = %dist, elapsed_ms = start.elapsed().as_millis() as u64, "k8s arthas package provisioned");
        Ok(ProvisionResult {
            tool: "arthas".to_string(),
            cached: false,
            java_version: String::new(),
            bisheng_version: String::new(),
            arch: String::new(),
            tool_home: dist,
            bins: HashMap::new(),
            elapsed_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// 失败路径容器内半截清理（best-effort）：kubectl exec rm -rf dist
    /// （仅 ensure_k8s 使用，随其一起暂时豁免 dead_code）
    #[cfg_attr(not(test), allow(dead_code))]
    async fn rm_pod_dist(
        base: &std::sync::Arc<dyn crate::exec::channel::ExecChannel>,
        ctr: &str,
        pod_q: &str,
        dist_q: &str,
    ) {
        let _ = base.run(&format!("kubectl exec {ctr}{pod_q} -- rm -rf {dist_q}")).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::channel::{ExecChannel, ExecOutput};
    use crate::provision::package::{ProvisionContext, StageTimeouts};
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    #[test]
    fn test_arthas_home() {
        assert_eq!(arthas_home(), "/tmp/friday-tools/arthas-4.3.5");
    }

    /// 记录 run + upload 调用的 ExecChannel stub
    #[derive(Default)]
    struct RecordingChannel {
        calls: TokioMutex<Vec<String>>,
        uploads: TokioMutex<Vec<(String, String)>>, // (local, remote)
        responses: TokioMutex<VecDeque<ExecOutput>>,
    }

    impl RecordingChannel {
        fn new(responses: Vec<(&str, i32)>) -> Arc<Self> {
            let dq = responses
                .into_iter()
                .map(|(o, c)| ExecOutput { stdout: o.to_string(), stderr: String::new(), exit_code: c })
                .collect();
            Arc::new(Self {
                calls: TokioMutex::new(Vec::new()),
                uploads: TokioMutex::new(Vec::new()),
                responses: TokioMutex::new(dq),
            })
        }
    }

    #[async_trait::async_trait]
    impl ExecChannel for RecordingChannel {
        async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            self.calls.lock().await.push(cmd.to_string());
            Ok(self.responses.lock().await.pop_front().unwrap_or(ExecOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 1,
            }))
        }
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
        async fn disconnect(&self) {}
        async fn is_alive(&self) -> bool { true }
        async fn upload(&self, local: &std::path::Path, remote: &str)
            -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.uploads.lock().await.push((local.display().to_string(), remote.to_string()));
            Ok(())
        }
    }

    fn test_ctx(channel: Arc<RecordingChannel>, arthas_zip: Option<PathBuf>) -> ProvisionContext {
        ProvisionContext {
            session_id: "s1".into(),
            env_id: "env-1".into(),
            channel,
            cache_dir: PathBuf::from("/tmp/unused-cache"),
            artifactory_base_url: "https://artifactory.example.com/artifactory/release".into(),
            arthas_zip,
            remote_tools_dir: "/tmp/friday-tools".into(),
            timeouts: StageTimeouts::default(),
            bus: crate::app::events::EventBus::disabled(),
        }
    }

    fn make_zip(dir: &std::path::Path) -> PathBuf {
        let p = dir.join("arthas-bin-4.3.5.zip");
        std::fs::write(&p, vec![0u8; 6 * 1024 * 1024]).unwrap();
        p
    }

    #[tokio::test]
    async fn test_ensure_cache_hit_skips_upload() {
        let channel = RecordingChannel::new(vec![
            ("", 0), // test -f arthas-boot.jar 缓存命中
        ]);
        let ctx = test_ctx(channel.clone(), None);
        let result = ArthasPackage.ensure(&ctx, "java").await.unwrap();
        assert!(result.cached);
        assert_eq!(result.tool_home, "/tmp/friday-tools/arthas-4.3.5");
        assert!(channel.uploads.lock().await.is_empty(), "cache hit must not upload");
        let calls = channel.calls.lock().await;
        assert!(calls.iter().all(|c| !c.contains("unzip") && !c.contains("python3")), "calls: {calls:?}");
    }

    #[tokio::test]
    async fn test_ensure_missing_zip_reports_structured_error() {
        let channel = RecordingChannel::new(vec![
            ("", 1), // 缓存未命中
        ]);
        let ctx = test_ctx(channel.clone(), None);
        let err = ArthasPackage.ensure(&ctx, "java").await.unwrap_err();
        assert!(err.stage == "vendored_package" || err.message.contains("未随应用分发"), "err: {err:?}");
        assert!(channel.uploads.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_ensure_corrupt_zip_reports_error() {
        let tmp = tempfile::tempdir().unwrap();
        let bad = tmp.path().join("arthas-bin-4.3.5.zip");
        std::fs::write(&bad, vec![0u8; 1024]).unwrap(); // 太小
        let channel = RecordingChannel::new(vec![
            ("", 1), // 缓存未命中
        ]);
        let ctx = test_ctx(channel.clone(), Some(bad));
        let err = ArthasPackage.ensure(&ctx, "java").await.unwrap_err();
        assert!(err.message.contains("arthas") || err.stage == "vendored_package", "err: {err:?}");
        assert!(channel.uploads.lock().await.is_empty(), "corrupt zip must not upload");
    }

    #[tokio::test]
    async fn test_ensure_uploads_and_extracts() {
        let tmp = tempfile::tempdir().unwrap();
        let zip = make_zip(tmp.path());
        let channel = RecordingChannel::new(vec![
            ("", 1), // 缓存未命中
            ("", 0), // 解压成功
            ("", 0), // 验证成功
        ]);
        let ctx = test_ctx(channel.clone(), Some(zip.clone()));
        let result = ArthasPackage.ensure(&ctx, "java").await.unwrap();
        assert!(!result.cached);
        assert_eq!(result.tool_home, "/tmp/friday-tools/arthas-4.3.5");
        let uploads = channel.uploads.lock().await;
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].0, zip.display().to_string());
        assert_eq!(uploads[0].1, "/tmp/friday-tools/arthas-bin-4.3.5.zip");
        let calls = channel.calls.lock().await;
        assert!(calls.iter().any(|c| c.contains("unzip -q -o arthas-bin-4.3.5.zip")), "calls: {calls:?}");
        assert!(calls.iter().any(|c| c.contains("arthas-boot.jar")), "find arthas-boot.jar: {calls:?}");
        // 不再有任何 artifactory 下载
        assert!(calls.iter().all(|c| !c.contains("curl") && !c.contains("wget")), "calls: {calls:?}");
    }

    /// vendoring 一致性守卫：scripts/vendor-versions.json 与 ARTHAS_VERSION 必须一致。
    #[test]
    fn test_vendor_manifest_matches_arthas_version() {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("scripts")
            .join("vendor-versions.json");
        let text = std::fs::read_to_string(&manifest)
            .unwrap_or_else(|e| panic!("read manifest {}: {e}", manifest.display()));
        let v: serde_json::Value =
            serde_json::from_str(&text).expect("vendor-versions.json must be valid JSON");
        let version = v["arthas"]["version"].as_str().expect("arthas.version");
        assert_eq!(
            version, ARTHAS_VERSION,
            "scripts/vendor-versions.json 的 arthas.version 与 ARTHAS_VERSION 漂移，二者必须同步修改"
        );
        let asset = v["arthas"]["asset"].as_str().expect("arthas.asset");
        assert_eq!(asset, format!("arthas-bin-{ARTHAS_VERSION}.zip"));
    }

    // ── ensure_k8s（容器装备：宿主机中转 unzip→tar→kubectl exec -i 注入）──

    const K8S_DIST: &str = "/opt/log/dump/coredump/friday-tools/arthas-dist";
    const K8S_BOOT_CHECK: &str =
        "test -f '/opt/log/dump/coredump/friday-tools/arthas-dist/arthas-boot.jar'";

    async fn ensure_k8s_with(
        ch: &Arc<RecordingChannel>,
        pod: &str,
        container: Option<&str>,
        zip: &std::path::Path,
    ) -> Result<ProvisionResult, ProvisionError> {
        let base: Arc<dyn ExecChannel> = ch.clone();
        ArthasPackage.ensure_k8s(&base, pod, container, zip, "s1", "env-1").await
    }

    #[tokio::test]
    async fn test_ensure_k8s_cache_hit() {
        // kubectl exec test -f 命中（exit 0）→ cached，零上传
        let ch = RecordingChannel::new(vec![("", 0)]);
        let result =
            ensure_k8s_with(&ch, "svc-1", None, std::path::Path::new("/local/arthas.zip"))
                .await
                .unwrap();
        assert!(result.cached);
        assert_eq!(result.tool_home, K8S_DIST);
        assert!(ch.uploads.lock().await.is_empty(), "cache hit must not upload");
        let calls = ch.calls.lock().await;
        assert_eq!(calls.len(), 1, "only the cache check should run: {calls:?}");
        assert!(calls[0].contains("kubectl exec"), "check: {}", calls[0]);
        assert!(calls[0].contains(K8S_BOOT_CHECK), "check: {}", calls[0]);
    }

    #[tokio::test]
    async fn test_ensure_k8s_full_flow() {
        let tmp = tempfile::tempdir().unwrap();
        let zip = make_zip(tmp.path());
        // ①cache miss ②mkdir staging ③host unzip+tar ④inject ⑤chgrp ⑥verify ⑦staging rm
        let ch = RecordingChannel::new(vec![
            ("", 1),
            ("", 0),
            ("", 0),
            ("", 0),
            ("", 0),
            ("", 0),
            ("", 0),
        ]);
        let result = ensure_k8s_with(&ch, "svc-1", None, &zip).await.unwrap();
        assert!(!result.cached);
        assert_eq!(result.tool_home, K8S_DIST);
        assert_eq!(result.tool, "arthas");
        // zip 本地路径透传，远端落宿主机 staging 子目录
        let uploads = ch.uploads.lock().await;
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].0, zip.display().to_string());
        assert!(
            uploads[0].1.starts_with("/tmp/friday-tools/staging/arthas-")
                && uploads[0].1.ends_with("/arthas.zip"),
            "host zip: {}",
            uploads[0].1
        );
        drop(uploads);
        let calls = ch.calls.lock().await;
        // 宿主机侧解 zip + 打 tar
        let extract = calls.iter().find(|c| c.contains("unzip -oq arthas.zip")).expect("extract cmd");
        assert!(extract.contains("tar czf arthas.tar.gz"), "extract: {extract}");
        // 注入：kubectl exec -i + 宿主机 stdin 重定向 + 容器内 tar 解包到 dist
        let inject = calls.iter().find(|c| c.contains("kubectl exec -i")).expect("inject cmd");
        assert!(inject.contains("< '/tmp/friday-tools/staging/arthas-"), "inject: {inject}");
        assert!(inject.contains("/arthas.tar.gz'"), "inject: {inject}");
        assert!(inject.contains("mkdir -p"), "inject: {inject}");
        assert!(inject.contains("tar xz -C"), "inject: {inject}");
        // 属组：容器内 chgrp -R ossgroup + chmod -R g+rX（经 kubectl exec）
        let chgrp = calls.iter().find(|c| c.contains("chgrp -R ossgroup")).expect("chgrp cmd");
        assert!(chgrp.contains("kubectl exec"), "chgrp must run inside pod: {chgrp}");
        assert!(chgrp.contains("chmod -R g+rX"), "chgrp: {chgrp}");
        // 验证：boot.jar test 出现两次（cache check + verify）
        assert_eq!(
            calls.iter().filter(|c| c.contains(K8S_BOOT_CHECK)).count(),
            2,
            "calls: {calls:?}"
        );
        // 宿主机 staging 清理
        assert!(
            calls.iter().any(|c| c.contains("rm -rf '/tmp/friday-tools/staging/arthas-")),
            "staging cleanup: {calls:?}"
        );
    }

    #[tokio::test]
    async fn test_ensure_k8s_inject_failure_cleans_pod_dist() {
        let tmp = tempfile::tempdir().unwrap();
        let zip = make_zip(tmp.path());
        // ①cache miss ②mkdir ③extract ④inject exit 1
        let ch = RecordingChannel::new(vec![("", 1), ("", 0), ("", 0), ("", 1)]);
        let err = ensure_k8s_with(&ch, "svc-1", None, &zip).await.unwrap_err();
        assert_eq!(err.code, "provision_failed");
        assert_eq!(err.stage, "inject");
        assert!(err.message.contains("svc-1"), "err must carry pod: {err:?}");
        let calls = ch.calls.lock().await;
        // 容器内半截清理（kubectl exec rm -rf dist）
        assert!(
            calls
                .iter()
                .any(|c| c.contains("kubectl exec") && c.contains(&format!("rm -rf '{K8S_DIST}'"))),
            "pod dist cleanup must fire: {calls:?}"
        );
        // 宿主机 staging 也清理
        assert!(
            calls.iter().any(|c| c.contains("rm -rf '/tmp/friday-tools/staging/arthas-")),
            "staging cleanup: {calls:?}"
        );
    }

    #[tokio::test]
    async fn test_ensure_k8s_container_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let zip = make_zip(tmp.path());
        let ch = RecordingChannel::new(vec![
            ("", 1),
            ("", 0),
            ("", 0),
            ("", 0),
            ("", 0),
            ("", 0),
            ("", 0),
        ]);
        let result = ensure_k8s_with(&ch, "svc-1", Some("main"), &zip).await.unwrap();
        assert!(!result.cached);
        let calls = ch.calls.lock().await;
        let kubectl_cmds: Vec<&String> = calls.iter().filter(|c| c.contains("kubectl exec")).collect();
        assert!(!kubectl_cmds.is_empty(), "calls: {calls:?}");
        for c in &kubectl_cmds {
            assert!(c.contains("-c 'main' "), "missing container flag: {c}");
        }
    }
}
