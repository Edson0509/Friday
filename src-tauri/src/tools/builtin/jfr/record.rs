//! jfr_record 异步录制管线（issue #23）。
//!
//! 背景：部分 Agent CLI（codeagentcli）的 MCP 客户端存在不可配置的 120s 工具调用
//! 硬超时，旧实现把「等待 duration_secs 落盘」同步阻塞在工具调用内，长录制
//! （>110s）必然被客户端取消，且取消后录制结果悬空、文件无人拉回。
//!
//! 现行契约：jfr_record 执行 JFR.start + JFR.check 校验后**立即返回 recording_id**；
//! 落盘等待由后台任务完成（专用连接轮询 stat，断线自动重建，对齐传输 worker
//! 「后台任务不走池」约定），文件稳定后移交 TransferManager 拉回（沿用既有
//! transfer_status 轮询与 .jfr 下载完成自动预热 JMC 链路）。Agent 轮询
//! jfr_record_status(recording_id) 直到终态。

use crate::app::events::{AppEvent, EventBus};
use crate::exec::channel::ExecChannel;
use crate::tools::builtin::jvm::core::{
    error_output, is_jdk_missing, parse_pid, require_bins, resolve_environment, validate_target,
    JvmExecCore,
};
use crate::tools::builtin::run_command::artifact_dir_for;
use crate::tools::category::ToolCategory;
use crate::tools::registry::{ToolContext, ToolDef, ToolHandler, ToolOutput};
use crate::tools::risk::RiskLevel;
use crate::transfer::state::{Direction, Status as TransferStatus, TransferState};
use crate::transfer::TransferManager;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// 落盘轮询间隔（虚拟时钟友好，测试 start_paused 可瞬时推进）
const RECORD_POLL_INTERVAL_SECS: u64 = 3;
/// JFR.start / JFR.check 同步执行上限：两者必须在 MCP 客户端 120s 硬超时内完成，
/// 工具调用本身不等录制落盘（issue #23 根因）
const RECORD_START_TIMEOUT_SECS: u64 = 60;
const RECORD_CHECK_TIMEOUT_SECS: u64 = 30;
/// 终态记录保留上限（LRU 淘汰防泄漏，对齐 TransferManager）
const MAX_FINISHED_RECORDS: usize = 100;

/// 录制任务阶段（存储态）。Downloading 的拉回终态由状态查询时实时观测并落档。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordingPhase {
    /// JFR.start 已确认运行，等待 duration 到期 + 落盘稳定
    Recording,
    /// 落盘完成，TransferManager 后台拉回中（transfer_id 已就绪）
    Downloading,
    /// 拉回完成（.jfr 已预热 JMC）
    Completed,
    /// 失败（录制未落盘 / 拉回失败，详见 error）
    Failed,
}

impl RecordingPhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, RecordingPhase::Completed | RecordingPhase::Failed)
    }

    fn as_str(self) -> &'static str {
        match self {
            RecordingPhase::Recording => "recording",
            RecordingPhase::Downloading => "downloading",
            RecordingPhase::Completed => "completed",
            RecordingPhase::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct RecordingState {
    pub id: String,
    pub session_id: String,
    pub env_id: String,
    pub pid: u32,
    /// JFR 录制名（friday-<ts>）
    pub name: String,
    /// k8s 目标定位（None = 宿主机 VM 模式；拉回任务专用连接走 K8sChannel 两跳）
    pub pod: Option<String>,
    pub namespace: Option<String>,
    pub container: Option<String>,
    pub remote_path: String,
    pub local_path: PathBuf,
    pub duration_secs: u32,
    pub settings: String,
    pub phase: RecordingPhase,
    pub transfer_id: Option<String>,
    pub remote_size: Option<u64>,
    /// 结构化失败码（issue #23）：pod_failed（目标 Pod 死亡）/ record_not_found
    /// （落盘超预算）/ transfer_failed（拉回失败）/ transfer_cancelled
    pub error_code: Option<String>,
    pub error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// 录制任务注册表：jfr_record 与 jfr_record_status 共享（内存态，随进程销毁）
pub struct RecordingRegistry {
    recordings: Mutex<HashMap<String, RecordingState>>,
}

impl RecordingRegistry {
    pub fn new() -> Self {
        Self { recordings: Mutex::new(HashMap::new()) }
    }

    pub async fn insert(&self, state: RecordingState) {
        self.recordings.lock().await.insert(state.id.clone(), state);
    }

    pub async fn get(&self, id: &str) -> Option<RecordingState> {
        self.recordings.lock().await.get(id).cloned()
    }

    /// 该会话全部录制（按创建时间倒序，对齐 TransferManager::list_for_session）
    pub async fn list_for_session(&self, session_id: &str) -> Vec<RecordingState> {
        let recordings = self.recordings.lock().await;
        let mut list: Vec<RecordingState> = recordings
            .values()
            .filter(|r| r.session_id == session_id)
            .cloned()
            .collect();
        list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        list
    }

    /// 同 session + 目标（env/pod/ns/container 复合键）+ pid 的活跃录制。
    /// pid 仅在目标内唯一（不同 Pod 可撞号），必须整键比较。
    /// 防客户端超时后 Agent 重试造成同 JVM 叠加录制
    pub async fn find_active_for_target(
        &self,
        session_id: &str,
        target: &crate::exec::pool::TargetKey,
        pid: u32,
    ) -> Option<RecordingState> {
        self.recordings
            .lock()
            .await
            .values()
            .find(|r| {
                !r.phase.is_terminal()
                    && r.session_id == session_id
                    && r.pid == pid
                    && crate::exec::pool::TargetKey::from_parts(
                        &r.env_id,
                        r.pod.as_deref(),
                        r.namespace.as_deref(),
                        r.container.as_deref(),
                    ) == *target
            })
            .cloned()
    }

    /// Recording → Downloading（落盘稳定，拉回任务已启动）
    pub async fn mark_downloading(&self, id: &str, transfer_id: String, remote_size: u64) {
        {
            let mut recordings = self.recordings.lock().await;
            if let Some(r) = recordings.get_mut(id) {
                if r.phase == RecordingPhase::Recording {
                    r.phase = RecordingPhase::Downloading;
                    r.transfer_id = Some(transfer_id);
                    r.remote_size = Some(remote_size);
                }
            }
        }
        self.evict_finished().await;
    }

    /// 未终态 → Failed（录制未落盘 / Pod 死亡等；error_code 见 RecordingState 文档）
    pub async fn mark_failed(&self, id: &str, error_code: &str, error: String) {
        {
            let mut recordings = self.recordings.lock().await;
            if let Some(r) = recordings.get_mut(id) {
                if !r.phase.is_terminal() {
                    r.phase = RecordingPhase::Failed;
                    r.error_code = Some(error_code.to_string());
                    r.error = Some(error);
                }
            }
        }
        self.evict_finished().await;
    }

    /// 状态查询观测到拉回终态时落档（仅 Downloading 可流转，幂等）
    pub async fn mark_transfer_outcome(
        &self,
        id: &str,
        phase: RecordingPhase,
        error_code: Option<String>,
        error: Option<String>,
    ) {
        debug_assert!(matches!(phase, RecordingPhase::Completed | RecordingPhase::Failed));
        {
            let mut recordings = self.recordings.lock().await;
            if let Some(r) = recordings.get_mut(id) {
                if r.phase == RecordingPhase::Downloading {
                    r.phase = phase;
                    r.error_code = error_code;
                    r.error = error;
                }
            }
        }
        self.evict_finished().await;
    }

    /// 终态记录超上限时按 created_at 淘汰最旧的（仅终态可淘汰）
    async fn evict_finished(&self) {
        let mut recordings = self.recordings.lock().await;
        let finished: Vec<(String, chrono::DateTime<chrono::Utc>)> = recordings
            .iter()
            .filter(|(_, r)| r.phase.is_terminal())
            .map(|(id, r)| (id.clone(), r.created_at))
            .collect();
        if finished.len() > MAX_FINISHED_RECORDS {
            let mut finished = finished;
            finished.sort_by_key(|(_, created)| *created);
            let to_remove = finished.len() - MAX_FINISHED_RECORDS;
            for (id, _) in finished.into_iter().take(to_remove) {
                recordings.remove(&id);
                tracing::debug!(recording_id = %id, "evicted finished recording record");
            }
        }
    }
}

/// 等待录制落盘的失败分类（错误码 + 上下文由调用方组装文案）
pub(super) enum WaitFailure {
    /// 后台预算（timeout_secs）用尽：文件未出现/未稳定
    Deadline { waited_secs: u64 },
    /// Pod 已死亡（kubectl stderr 死亡信号识别，issue #23）——快速失败，
    /// 不再空转到 deadline。detail = 原始 stderr
    PodGone { detail: String },
}

/// 后台等待录制落盘 → 移交 TransferManager 拉回。
/// 全程不持池连接（专用连接，断线自动重建），等待时长不影响任何 MCP 调用。
pub(super) async fn run_recording_wait(
    registry: Arc<RecordingRegistry>,
    transfer: Arc<TransferManager>,
    bus: EventBus,
    rec: RecordingState,
    deadline: tokio::time::Instant,
) {
    tracing::info!(
        recording_id = %rec.id, session_id = %rec.session_id, env_id = %rec.env_id,
        pid = rec.pid, name = %rec.name, remote_path = %rec.remote_path,
        duration_secs = rec.duration_secs,
        "recording wait: background task started"
    );
    match wait_for_recording(&transfer, &rec, deadline).await {
        Ok(remote_size) => {
            // ① P0 健康检查（issue #23）：文件就绪 ≠ Pod 存活——就绪到下载的窗口内
            //    Pod 可能已崩溃（实测 0.6s），死 Pod 上启动拉回只会得到模糊的
            //    "container not found"。宿主机侧 kubectl get pod 查 phase，
            //    非 Running 直接 pod_failed 终态（带重查指引）
            if let (Some(pod), Some(ns)) = (&rec.pod, &rec.namespace) {
                if let Some(phase) = pod_phase(&transfer, &rec.env_id, pod, ns).await {
                    if phase != "Running" {
                        let error = format!(
                            "目标 Pod 已不处于 Running 状态（phase={phase}），录制文件 {} 不可达。请重新调用 k8s_find_pods 定位新 Pod 后重新 jfr_record；若 dump 目录为共享持久卷，也可用 file_download(新 Pod, 同路径) 尝试抢救。",
                            rec.remote_path
                        );
                        registry.mark_failed(&rec.id, "pod_failed", error.clone()).await;
                        emit_progress(&bus, &rec.session_id, "record", &format!("录制已完成但目标 Pod 已死亡（phase={phase}），无法拉回"));
                        tracing::warn!(
                            recording_id = %rec.id, pod = %pod, phase = %phase,
                            "recording wait: pod not running before transfer, aborting"
                        );
                        return;
                    }
                }
                // phase 无法判定（检查失败/超时）：不阻塞拉回——拉回自身有重试与失败兜底
            }

            // ② 落盘稳定 → 后台拉回（成功后 hook 自动预热 JMC；pod 目标走 K8sChannel 两跳）
            let state = TransferState::new(
                Direction::Download,
                &rec.session_id,
                &rec.env_id,
                &rec.remote_path,
                rec.local_path.clone(),
                true, // 下载成功后清理远端（Friday 自己生成的文件）
                rec.pod.as_deref(),
                rec.namespace.as_deref(),
                rec.container.as_deref(),
            );
            let transfer_id = transfer.start(state).await;
            registry.mark_downloading(&rec.id, transfer_id.clone(), remote_size).await;
            emit_progress(
                &bus,
                &rec.session_id,
                "download",
                "录制完成，后台拉回已启动（jfr_record_status / transfer_status 可查进度）",
            );
            tracing::info!(
                recording_id = %rec.id, transfer_id = %transfer_id, remote_size,
                "recording wait: file ready, background download started"
            );
        }
        Err(WaitFailure::Deadline { waited_secs }) => {
            let error = format!(
                "录制到时后文件未就绪：{}（已等待 {waited_secs}s）。远端文件可能仍在写入，可稍后用 file_download 手动拉回",
                rec.remote_path
            );
            registry.mark_failed(&rec.id, "record_not_found", error.clone()).await;
            emit_progress(&bus, &rec.session_id, "record", &format!("录制文件未就绪：{error}"));
            tracing::error!(
                recording_id = %rec.id, session_id = %rec.session_id,
                remote_path = %rec.remote_path, waited_secs,
                "recording wait: file never materialized"
            );
        }
        Err(WaitFailure::PodGone { detail }) => {
            let error = format!(
                "目标 Pod 已死亡，录制中断（kubectl: {detail}）。请重新调用 k8s_find_pods 定位新 Pod 后重新 jfr_record；若 dump 目录为共享持久卷，也可用 file_download(新 Pod, {}) 尝试抢救。",
                rec.remote_path
            );
            registry.mark_failed(&rec.id, "pod_failed", error.clone()).await;
            emit_progress(&bus, &rec.session_id, "record", "目标 Pod 已死亡，录制中断");
            tracing::error!(
                recording_id = %rec.id, session_id = %rec.session_id,
                pod = rec.pod.as_deref().unwrap_or("-"), detail = %detail,
                "recording wait: pod gone during recording"
            );
        }
    }
}

/// 宿主机侧查询 Pod phase（专用连接 + 15s 超时；连接层面走宿主机而非容器 exec）。
/// None = 无法判定（命令失败/超时/输出异常）——调用方不得据此阻断流程
async fn pod_phase(transfer: &Arc<TransferManager>, env_id: &str, pod: &str, namespace: &str) -> Option<String> {
    const PHASE_CHECK_TIMEOUT_SECS: u64 = 15;
    let check = async {
        let ch = transfer.dedicated_channel(env_id, None, None, None).await.ok()?;
        let cmd = crate::exec::k8s::pod_phase_command(pod, Some(namespace));
        let out = ch.run(&cmd).await.ok();
        ch.disconnect().await;
        let out = out?;
        if out.exit_code != 0 {
            tracing::warn!(pod, exit_code = out.exit_code, stderr = %out.stderr, "pod phase check: kubectl failed");
            return None;
        }
        let phase = out.stdout.trim().to_string();
        if phase.is_empty() {
            None
        } else {
            Some(phase)
        }
    };
    match tokio::time::timeout(std::time::Duration::from_secs(PHASE_CHECK_TIMEOUT_SECS), check).await {
        Ok(phase) => phase,
        Err(_) => {
            tracing::warn!(pod, "pod phase check: timed out");
            None
        }
    }
}

/// 等待录制落盘：duration 到期后文件存在（size > 0）且两次轮询大小相等 → 稳定。
/// Pod 死亡（kubectl stderr 死亡信号）→ PodGone 快速失败；deadline 用尽 → Deadline。
/// stat 走专用连接（懒建 + 断线重建）；全程 tokio 虚拟时钟友好。
async fn wait_for_recording(
    transfer: &Arc<TransferManager>,
    rec: &RecordingState,
    deadline: tokio::time::Instant,
) -> Result<u64, WaitFailure> {
    let start = tokio::time::Instant::now();
    let mut last_size: u64 = 0;
    let mut channel: Option<Arc<dyn ExecChannel>> = None;
    loop {
        if tokio::time::Instant::now() >= deadline {
            if let Some(ch) = channel.take() {
                ch.disconnect().await;
            }
            return Err(WaitFailure::Deadline { waited_secs: start.elapsed().as_secs() });
        }
        tokio::time::sleep(std::time::Duration::from_secs(RECORD_POLL_INTERVAL_SECS)).await;
        // 专用连接懒建：失败不放弃，下轮重试（等待预算内自愈）
        if channel.is_none() {
            match transfer
                .dedicated_channel(
                    &rec.env_id,
                    rec.pod.as_deref(),
                    rec.namespace.as_deref(),
                    rec.container.as_deref(),
                )
                .await
            {
                Ok(ch) => channel = Some(ch),
                Err(e) => {
                    tracing::warn!(recording_id = %rec.id, env_id = %rec.env_id, error = %e, "recording wait: connect failed, retrying next poll");
                    last_size = 0;
                    continue;
                }
            }
        }
        let stat_cmd = format!(
            "stat -c %s {}",
            crate::exec::ssh::shell_quote_single(&rec.remote_path)
        );
        let size: u64 = if let Some(ch) = channel.as_deref() {
            match ch.run(&stat_cmd).await {
                Ok(o) if o.exit_code == 0 => o.stdout.trim().parse().unwrap_or(0),
                Ok(o) => {
                    // Pod 死亡信号：容器不在 / completed pod / Pod 已删（issue #23——
                    // 不再空转到 deadline，Agent 早 10 分钟拿到 pod_failed）
                    if crate::exec::k8s::is_pod_gone_error(&o.stderr) {
                        if let Some(ch) = channel.take() {
                            ch.disconnect().await;
                        }
                        return Err(WaitFailure::PodGone { detail: o.stderr.trim().to_string() });
                    }
                    0
                }
                Err(e) => {
                    tracing::warn!(recording_id = %rec.id, error = %e, "recording wait: stat failed, dropping connection for reconnect");
                    if let Some(ch) = channel.take() {
                        ch.disconnect().await;
                    }
                    0
                }
            }
        } else {
            0
        };
        let elapsed = start.elapsed().as_secs();
        if elapsed >= rec.duration_secs as u64 && size > 0 && size == last_size {
            if let Some(ch) = channel.take() {
                ch.disconnect().await;
            }
            return Ok(size);
        }
        last_size = size;
    }
}

/// jfr_record：JFR.start + JFR.check 校验后立即返回，落盘等待在后台
pub struct JfrRecordHandler {
    pub core: Arc<JvmExecCore>,
    pub bus: EventBus,
    pub transfer: Arc<TransferManager>,
    pub recordings: Arc<RecordingRegistry>,
}

/// jfr_record_status：录制任务状态查询（含拉回实时态观测）
pub struct JfrRecordStatusHandler {
    pub transfer: Arc<TransferManager>,
    pub recordings: Arc<RecordingRegistry>,
}

#[async_trait]
impl ToolHandler for JfrRecordHandler {
    async fn execute(&self, args: serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        self.execute_record(&args, ctx).await
    }
}

#[async_trait]
impl ToolHandler for JfrRecordStatusHandler {
    async fn execute(&self, args: serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        self.execute_status(&args, ctx).await
    }
}

impl JfrRecordHandler {
    async fn execute_record(&self, args: &serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        let Some(environment) = args.get("environment").and_then(|v| v.as_str()) else {
            return error_output("invalid_args", "missing required parameter: environment");
        };
        let Some(pid) = args.get("pid").and_then(|v| parse_pid(v)) else {
            return error_output("invalid_args", "pid 必须是正整数字符串");
        };
        let pod = args.get("pod").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let namespace = args.get("namespace").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let container = args.get("container").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let (duration_secs, settings) = match super::mapping::validate_record_params(args) {
            Ok(v) => v,
            Err(e) => return error_output("invalid_args", &e),
        };
        let timeout_secs = super::mapping::effective_record_timeout(
            args.get("timeout_secs").and_then(|v| v.as_i64()),
            duration_secs,
        );

        let (env, channel) = match resolve_environment(&self.core.db, &self.core.exec_pool, environment, pod, namespace, container).await {
            Ok(Some(pair)) => pair,
            Ok(None) => {
                return error_output(
                    "environment_not_found",
                    &format!(
                        "环境「{environment}」不存在。请先调用 list_environments 查看可用环境；若无匹配，请让用户在右侧「环境」面板添加。"
                    ),
                );
            }
            Err(e) => return error_output("connection_error", &e),
        };

        // 环境类型门禁：vm 拒 pod/ns / container 必填 pod+namespace（引导 k8s_find_pods）+ k8s 名防呆
        if let Err(msg) = validate_target(&env, pod, namespace, container) {
            return error_output("environment_type_mismatch", &msg);
        }

        let target = crate::exec::pool::TargetKey::from_parts(&env.id, pod, namespace, container);

        // JDK 路径：查缓存，miss 引导 ensure_tool
        let Some(layout) = self
            .core
            .jdk_cache
            .get(&crate::tools::builtin::jvm::jdk_cache::cache_key(&target))
            .await
        else {
            tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, "jdk not provisioned (cache miss)");
            return error_output(
                "jdk_not_provisioned",
                "该环境尚未装备 JDK。请先调用 ensure_tool(environment, tool=\"jdk\"；容器内服务需同时传 pod/namespace/container) 装备，然后重试本工具。",
            );
        };
        let bins = match require_bins(&layout, &["jcmd"]) {
            Ok(b) => b,
            Err(e) => return error_output("jdk_not_provisioned", &e),
        };
        let jcmd = &bins[0];

        // 去重：同 session + 目标 + pid 已有活跃录制（MCP 客户端超时后 Agent 重试是
        // issue #23 的典型场景）——直接复用，避免同 JVM 叠加录制开销
        if let Some(existing) = self.recordings.find_active_for_target(&ctx.session_id, &target, pid).await {
            tracing::warn!(
                session_id = %ctx.session_id, env_id = %env.id, pid,
                recording_id = %existing.id, "jfr record: duplicate active recording rejected"
            );
            return ToolOutput {
                success: false,
                data: serde_json::json!({
                    "error": "duplicate_recording",
                    "message": "该 JVM 已有进行中的 JFR 录制任务（长录制耗时数分钟，重复启动会叠加录制开销）。",
                    "recording_id": existing.id,
                    "note": "请轮询 jfr_record_status(recording_id) 获取结果，勿重复启动录制。",
                }),
                raw_stdout: None,
            };
        }

        // ① 一次性定时录制（文件名 Friday 固定构造——不开放自定义，注入面）。
        // 容器目标落 POD_DUMP_DIR（coredump 卷，用户环境实际存在）；VM 保持 /tmp/friday-tools
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let remote_path = if target.pod.is_some() {
            format!("{}/friday-recording-{pid}-{ts}.jfr", crate::exec::k8s::POD_DUMP_DIR)
        } else {
            format!("/tmp/friday-tools/recording-{pid}-{ts}.jfr")
        };
        let name = format!("friday-{ts}");
        let start_cmd = super::mapping::jfr_start_command(jcmd, pid, &name, duration_secs, &settings, &remote_path);
        let local_path = artifact_dir_for(&self.core.artifacts_dir, &ctx.session_id)
            .join(format!("recording-{pid}-{ts}.jfr"));

        tracing::info!(session_id = %ctx.session_id, env_id = %env.id, pid, command = %start_cmd, "jfr record: starting");
        emit_progress(
            &self.bus,
            &ctx.session_id,
            "record",
            &format!("JFR 录制启动中（{duration_secs}s，settings={settings}）…"),
        );

        let start_output = match tokio::time::timeout(
            std::time::Duration::from_secs(RECORD_START_TIMEOUT_SECS),
            channel.run(&start_cmd),
        )
        .await
        {
            Err(_) => {
                tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, timeout_secs = RECORD_START_TIMEOUT_SECS, "JFR.start timed out, dropping connection");
                crate::exec::pool::drop_target_and_kill(&self.core.exec_pool, &self.core.db, &target, &start_cmd).await;
                return error_output(
                    "timeout_error",
                    &format!("JFR.start 超时（{RECORD_START_TIMEOUT_SECS}s）；ssh 连接已断开"),
                );
            }
            Ok(Err(e)) => {
                tracing::error!(session_id = %ctx.session_id, env_id = %env.id, error = %e, "JFR.start exec failed");
                return error_output("connection_error", &e.to_string());
            }
            Ok(Ok(output)) => {
                if is_jdk_missing(output.exit_code, &output.stderr) {
                    tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, "jdk missing on remote, clearing cache");
                    self.core
                        .jdk_cache
                        .clear(&crate::tools::builtin::jvm::jdk_cache::cache_key(&target))
                        .await;
                    return error_output(
                        "jdk_missing_on_remote",
                        "远端 JDK 已不存在（可能 /tmp 被清理）。请重新调用 ensure_tool 装备后重试。",
                    );
                }
                if output.exit_code != 0 {
                    // JFR.start 失败：透传 jcmd 输出 + 兼容性提示（JDK 8 场景）
                    tracing::error!(session_id = %ctx.session_id, env_id = %env.id, exit_code = output.exit_code, "JFR.start command failed");
                    return ToolOutput {
                        success: false,
                        data: serde_json::json!({
                            "error": "record_failed",
                            "message": "JFR.start 失败。目标 JVM 兼容性：JDK 11+ 开箱即用；Oracle JDK 8 需启动参数 -XX:+UnlockCommercialVMOption -XX:+FlightRecorder；OpenJDK 8 无 JFR——此类场景改用 arthas_profiler。",
                            "stdout": output.stdout,
                            "stderr": output.stderr,
                            "exit_code": output.exit_code,
                        }),
                        raw_stdout: Some(output.stdout),
                    };
                }
                output
            }
        };

        // ② JFR.check 校验（issue #23 问题 2：JFR.start exit 0 ≠ 录制在运行）。
        //    通过 = 输出含 running；或录制已瞬间完成且文件已非空（短 duration +
        //    慢 attach 边角）也放行，交给后台等待判定
        let check_cmd = super::mapping::jfr_check_command(jcmd, pid, &name);
        let check_output = match tokio::time::timeout(
            std::time::Duration::from_secs(RECORD_CHECK_TIMEOUT_SECS),
            channel.run(&check_cmd),
        )
        .await
        {
            Err(_) => {
                tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, timeout_secs = RECORD_CHECK_TIMEOUT_SECS, "JFR.check timed out, dropping connection");
                crate::exec::pool::drop_target_and_kill(&self.core.exec_pool, &self.core.db, &target, &check_cmd).await;
                return error_output(
                    "timeout_error",
                    &format!("JFR.check 校验超时（{RECORD_CHECK_TIMEOUT_SECS}s）；ssh 连接已断开，录制状态未知（远端预期路径 {remote_path}）"),
                );
            }
            Ok(Err(e)) => {
                tracing::error!(session_id = %ctx.session_id, env_id = %env.id, error = %e, "JFR.check exec failed");
                return error_output(
                    "connection_error",
                    &format!("JFR.check 校验执行失败：{e}。录制可能已在运行（远端预期路径 {remote_path}），可稍后用 file_download 手动拉回"),
                );
            }
            Ok(Ok(o)) => o,
        };
        if !super::mapping::recording_check_passes(&check_output.stdout, &check_output.stderr)
            && !remote_file_nonzero(&channel, &remote_path).await
        {
            tracing::error!(
                session_id = %ctx.session_id, env_id = %env.id, pid,
                exit_code = check_output.exit_code,
                "JFR.start succeeded but recording not running"
            );
            return ToolOutput {
                success: false,
                data: serde_json::json!({
                    "error": "record_verify_failed",
                    "message": format!("JFR.start 返回成功，但 JFR.check 未确认录制在运行（JVM 侧可能静默失败）。远端预期路径：{remote_path}。"),
                    "stdout": check_output.stdout,
                    "stderr": check_output.stderr,
                    "exit_code": check_output.exit_code,
                }),
                raw_stdout: Some(start_output.stdout),
            };
        }

        // ③ 注册录制任务 + 后台等待落盘（本调用到此返回，等待时长与 MCP 客户端
        //    超时解耦——issue #23 根因修复）
        let state = RecordingState {
            id: uuid::Uuid::new_v4().to_string(),
            session_id: ctx.session_id.clone(),
            env_id: env.id.clone(),
            pid,
            name,
            pod: pod.map(|s| s.to_string()),
            namespace: namespace.map(|s| s.to_string()),
            container: container.map(|s| s.to_string()),
            remote_path: remote_path.clone(),
            local_path: local_path.clone(),
            duration_secs,
            settings: settings.clone(),
            phase: RecordingPhase::Recording,
            transfer_id: None,
            remote_size: None,
            error_code: None,
            error: None,
            created_at: chrono::Utc::now(),
        };
        let recording_id = state.id.clone();
        self.recordings.insert(state.clone()).await;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        let registry = self.recordings.clone();
        let transfer = self.transfer.clone();
        let bus = self.bus.clone();
        let rec = state;
        tokio::spawn(async move {
            run_recording_wait(registry, transfer, bus, rec, deadline).await;
        });

        emit_progress(
            &self.bus,
            &ctx.session_id,
            "record",
            &format!("JFR 录制已启动（{duration_secs}s），后台等待落盘（轮询 jfr_record_status）"),
        );

        tracing::info!(
            session_id = %ctx.session_id, env_id = %env.id, pid,
            recording_id, remote_path, duration_secs, settings, timeout_secs,
            "jfr record: started, background wait spawned"
        );

        ToolOutput {
            success: true,
            data: serde_json::json!({
                "recording_id": recording_id,
                "status": "recording",
                "remote_path": remote_path,
                "local_path": local_path.to_string_lossy(),
                "duration_secs": duration_secs,
                "settings": settings,
                "timeout_secs": timeout_secs,
                "note": "JFR 录制已启动，后台等待落盘（本调用已返回，录制不受影响）。请轮询 jfr_record_status(recording_id)：recording（进行中，稍候再查）→ downloading（带 transfer_id，可轮询 transfer_status 看进度）→ completed（自动预热 JMC，直接用 jfr_quick_analysis(local_path) / jfr_rules(local_path) 分析）/ failed（见 error 字段）。",
            }),
            raw_stdout: Some(start_output.stdout),
        }
    }
}

impl JfrRecordStatusHandler {
    async fn execute_status(&self, args: &serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        tracing::debug!(session_id = %ctx.session_id, "jfr record status: querying");
        match args.get("recording_id").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
            Some(id) => {
                let Some(mut rec) = self.recordings.get(id).await else {
                    return error_output("invalid_args", &format!("recording_id 不存在: {id}"));
                };
                self.observe_transfer_outcome(&mut rec).await;
                ToolOutput {
                    success: true,
                    data: recording_to_json(&rec),
                    raw_stdout: None,
                }
            }
            None => {
                let mut list = self.recordings.list_for_session(&ctx.session_id).await;
                for rec in &mut list {
                    self.observe_transfer_outcome(rec).await;
                }
                ToolOutput {
                    success: true,
                    data: serde_json::json!({
                        "recordings": list.iter().map(recording_to_json).collect::<Vec<_>>(),
                    }),
                    raw_stdout: None,
                }
            }
        }
    }

    /// Downloading 阶段实时观测拉回任务终态并落档（幂等；其余阶段直接返回）。
    /// 拉回失败且目标为 Pod 时，主动查 Pod 存活（issue #23 P2）：死亡 → pod_failed
    /// 结构化错误 + 重查指引；存活 → 保留原传输错误（远端文件在，可断点续传）
    async fn observe_transfer_outcome(&self, rec: &mut RecordingState) {
        if rec.phase != RecordingPhase::Downloading {
            return;
        }
        let Some(tid) = rec.transfer_id.clone() else { return };
        let Some(ts) = self.transfer.get(&tid).await else { return };
        match ts.status {
            TransferStatus::Completed => {
                self.recordings
                    .mark_transfer_outcome(&rec.id, RecordingPhase::Completed, None, None)
                    .await;
                rec.phase = RecordingPhase::Completed;
            }
            TransferStatus::Cancelled => {
                let msg = "录制文件拉回已取消。远端文件保留，可用 file_download 重试（断点续传）。".to_string();
                self.recordings
                    .mark_transfer_outcome(&rec.id, RecordingPhase::Failed, Some("transfer_cancelled".into()), Some(msg.clone()))
                    .await;
                rec.phase = RecordingPhase::Failed;
                rec.error_code = Some("transfer_cancelled".into());
                rec.error = Some(msg);
            }
            TransferStatus::Failed => {
                let orig = ts.error.clone().unwrap_or_else(|| "未知传输错误".to_string());
                let (code, msg) = if let (Some(pod), Some(ns)) = (&rec.pod, &rec.namespace) {
                    match pod_phase(&self.transfer, &rec.env_id, pod, ns).await {
                        Some(phase) if phase != "Running" => (
                            "pod_failed",
                            format!(
                                "录制文件拉回失败：目标 Pod 已不处于 Running 状态（phase={phase}），录制文件不可达。原传输错误：{orig}。请重新调用 k8s_find_pods 定位新 Pod 后重新 jfr_record；若 dump 目录为共享持久卷，也可用 file_download(新 Pod, {}) 尝试抢救。",
                                rec.remote_path
                            ),
                        ),
                        _ => (
                            "transfer_failed",
                            format!("录制文件拉回失败：{orig}。远端文件保留（Pod 存活），可用 file_download 重试（断点续传）"),
                        ),
                    }
                } else {
                    (
                        "transfer_failed",
                        format!("录制文件拉回失败：{orig}。远端文件保留，可用 file_download 重试（断点续传）"),
                    )
                };
                tracing::warn!(recording_id = %rec.id, error_code = code, "recording transfer failed");
                self.recordings
                    .mark_transfer_outcome(&rec.id, RecordingPhase::Failed, Some(code.to_string()), Some(msg.clone()))
                    .await;
                rec.phase = RecordingPhase::Failed;
                rec.error_code = Some(code.to_string());
                rec.error = Some(msg);
            }
            _ => {}
        }
    }
}

/// 远端文件已存在且非空（短 duration + 慢 attach：录制瞬间完成的放行判定）
async fn remote_file_nonzero(channel: &Arc<dyn ExecChannel>, remote_path: &str) -> bool {
    let cmd = format!(
        "stat -c %s {}",
        crate::exec::ssh::shell_quote_single(remote_path)
    );
    match channel.run(&cmd).await {
        Ok(o) if o.exit_code == 0 => o.stdout.trim().parse::<u64>().unwrap_or(0) > 0,
        _ => false,
    }
}

fn emit_progress(bus: &EventBus, session_id: &str, stage: &str, detail: &str) {
    bus.emit(
        session_id,
        AppEvent::ProvisionProgress {
            session_id: session_id.to_string(),
            tool: "jfr_record".to_string(),
            stage: stage.to_string(),
            detail: detail.to_string(),
        },
    );
}

/// RecordingState → Agent 可读 JSON（状态查询单条与列表共用）
fn recording_to_json(rec: &RecordingState) -> serde_json::Value {
    let note = match rec.phase {
        RecordingPhase::Recording => format!(
            "JFR 录制进行中（{}s 档）。请稍候后轮询 jfr_record_status(recording_id)。",
            rec.duration_secs
        ),
        RecordingPhase::Downloading => "录制完成，后台拉回中。可轮询 transfer_status(transfer_id) 查看字节级进度；完成后自动预热 JMC。".to_string(),
        RecordingPhase::Completed => "录制拉回完成并自动预热 JMC。直接用 local_path 调 jfr_quick_analysis / jfr_rules 起步诊断。".to_string(),
        RecordingPhase::Failed => {
            if rec.error_code.as_deref() == Some("pod_failed") {
                "目标 Pod 已死亡，录制文件不可达。请重新调用 k8s_find_pods 定位新 Pod 后重新 jfr_record；若 dump 目录为共享持久卷，也可用 file_download(新 Pod, 同路径) 尝试抢救。".to_string()
            } else if rec.transfer_id.is_some() {
                "拉回失败：远端文件保留，可用 file_download(remote_path) 重试（断点续传）。".to_string()
            } else {
                "录制未正常落盘：远端文件可能仍在写入，可稍后用 file_download(remote_path) 手动拉回；或检查目标 JVM 后重新 jfr_record。".to_string()
            }
        }
    };
    serde_json::json!({
        "recording_id": rec.id,
        "status": rec.phase.as_str(),
        "pid": rec.pid.to_string(),
        "remote_path": rec.remote_path,
        "local_path": rec.local_path.to_string_lossy(),
        "duration_secs": rec.duration_secs,
        "settings": rec.settings,
        "transfer_id": rec.transfer_id.clone(),
        "remote_size": rec.remote_size,
        "error_code": rec.error_code.clone(),
        "error": rec.error.clone(),
        "note": note,
    })
}

pub fn record_tool_def(
    core: &Arc<JvmExecCore>,
    bus: &EventBus,
    transfer: &Arc<TransferManager>,
    recordings: &Arc<RecordingRegistry>,
) -> ToolDef {
    ToolDef {
        name: "jfr_record".to_string(),
        description: "对目标 JVM 热开启 JFR 飞行录制并后台拉回（jcmd JFR.start，目标需 JDK 11+，profile 档开销约 1~3%，不中断服务）。本调用只做 JFR.start + JFR.check 校验，立即返回 recording_id（不等录制完成，长录制不超时）；后台等待 duration_secs（10~600，默认 60）落盘并自动拉回。轮询 jfr_record_status(recording_id)：recording（进行中）→ downloading（带 transfer_id，可轮询 transfer_status）→ completed（自动预热 JMC，直接用 jfr_quick_analysis / jfr_rules 起步诊断）/ failed（见 error，远端文件保留可 file_download 手动拉回）。⚠ 目标 JDK 8 不支持热开启 JFR（Oracle JDK 8 需启动参数，OpenJDK 8 无 JFR），此类场景改用 arthas_profiler。需先 ensure_tool 装备 JDK。".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "environment": { "type": "string", "description": "目标环境名称（list_environments 返回的 name）" },
                "pid": { "type": "string", "description": "目标 Java 进程 PID（list_processes 返回）" },
                "duration_secs": { "type": "number", "description": "录制时长秒数，10~600，默认 60" },
                "settings": { "type": "string", "enum": ["profile", "default"], "description": "事件档位：profile 全维度（开销 1~3%），default 低开销（<1%），默认 profile" },
                "timeout_secs": { "type": "number", "description": "后台等待落盘的总超时秒数（不影响本调用——本调用在 JFR.start 后即返回），默认 600，上限 1800；实际下限为 duration_secs+120" },
                "pod": { "type": "string", "description": "Kubernetes Pod 名（容器环境必填；虚机环境不支持；全小写，须为 k8s_find_pods 返回的准确名，勿用服务名）" },
                "namespace": { "type": "string", "description": "Kubernetes namespace（容器环境必填；与 pod 一起来自 k8s_find_pods 返回；全小写）" },
                "container": { "type": "string", "description": "容器名（多容器 Pod 时指定；缺省用 Pod 默认容器）" }
            },
            "required": ["environment", "pid"]
        }),
        risk_level: RiskLevel::Low,
        category: ToolCategory::Jfr,
        needs_channel: false,
        handler: Arc::new(JfrRecordHandler {
            core: core.clone(),
            bus: bus.clone(),
            transfer: transfer.clone(),
            recordings: recordings.clone(),
        }),
    }
}

pub fn record_status_tool_def(
    transfer: &Arc<TransferManager>,
    recordings: &Arc<RecordingRegistry>,
) -> ToolDef {
    ToolDef {
        name: "jfr_record_status".to_string(),
        description: "查询 jfr_record 后台录制任务状态。传 recording_id 查单条；不传则列出本会话全部录制。状态流转：recording（录制进行中，稍候再查）→ downloading（落盘完成后台拉回中，带 transfer_id，可轮询 transfer_status 看字节级进度）→ completed（拉回完成且自动预热 JMC，local_path 可直接用于 jfr_* 分析工具）/ failed（录制未落盘或拉回失败，详见 error 与 note）。".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "recording_id": { "type": "string", "description": "录制任务 ID（jfr_record 返回，可选，缺省列出全部）" }
            }
        }),
        risk_level: RiskLevel::ReadOnly,
        category: ToolCategory::Jfr,
        needs_channel: false,
        handler: Arc::new(JfrRecordStatusHandler {
            transfer: transfer.clone(),
            recordings: recordings.clone(),
        }),
    }
}
