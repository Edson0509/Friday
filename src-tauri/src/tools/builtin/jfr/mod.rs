pub mod mapping;
pub mod record;

use crate::app::events::EventBus;
use crate::jfr::{JmcError, JmcManager};
use crate::tools::builtin::jvm::core::{clamp_or, error_output, JvmExecCore};
use crate::tools::builtin::run_command::{artifact_dir_for, truncate_output};
use crate::tools::category::ToolCategory;
use crate::tools::registry::{ToolContext, ToolDef, ToolHandler, ToolOutput};
use crate::tools::risk::RiskLevel;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// (default_secs, max_secs)
type Timeouts = (u64, u64);
const QUERY: Timeouts = (60, 300);
const HEAVY: Timeouts = (300, 1800);

/// jfr_compare / 代理分析工具
pub struct JfrProxyHandler {
    pub jmc: Arc<JmcManager>,
    pub artifacts_dir: PathBuf,
    pub kind: JfrToolKind,
    pub timeouts: Timeouts,
}

#[derive(Debug, Clone, Copy)]
pub enum JfrToolKind {
    Compare,
    Proxy(mapping::JfrProxyKind),
}

#[async_trait]
impl ToolHandler for JfrProxyHandler {
    async fn execute(&self, args: serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        match self.kind {
            JfrToolKind::Compare => self.execute_compare(&args, ctx).await,
            JfrToolKind::Proxy(kind) => self.execute_proxy(kind, &args, ctx).await,
        }
    }
}

impl JfrProxyHandler {
    async fn execute_compare(&self, args: &serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        let baseline_raw = args.get("baseline_local_path").and_then(|v| v.as_str());
        let target_raw = args.get("target_local_path").and_then(|v| v.as_str());
        let (Some(baseline_raw), Some(target_raw)) = (baseline_raw, target_raw) else {
            return error_output(
                "invalid_args",
                "missing required parameters: baseline_local_path / target_local_path（两次录制各一份）",
            );
        };
        let baseline = match resolve_existing_file(baseline_raw) {
            Ok(p) => p,
            Err(e) => return error_output("invalid_path", &e),
        };
        let target = match resolve_existing_file(target_raw) {
            Ok(p) => p,
            Err(e) => return error_output("invalid_path", &e),
        };
        let resolved = format!("{} -> {}", baseline.display(), target.display());
        let timeout_secs =
            clamp_or(args.get("timeout_secs").and_then(|v| v.as_i64()), self.timeouts.0, self.timeouts.1);
        let (upstream, upstream_args) = mapping::build_compare(
            &baseline.to_string_lossy(),
            &target.to_string_lossy(),
            args.get("args"),
        );
        self.run_query(&upstream, &upstream_args, &resolved, timeout_secs, ctx).await
    }

    async fn execute_proxy(
        &self,
        kind: mapping::JfrProxyKind,
        args: &serde_json::Value,
        ctx: &ToolContext,
    ) -> ToolOutput {
        let Some(local_path) = args.get("local_path").and_then(|v| v.as_str()) else {
            return error_output("invalid_args", "missing required parameter: local_path");
        };
        let path = match resolve_existing_file(local_path) {
            Ok(p) => p,
            Err(e) => return error_output("invalid_path", &e),
        };
        let resolved = path.display().to_string();
        let timeout_secs =
            clamp_or(args.get("timeout_secs").and_then(|v| v.as_i64()), self.timeouts.0, self.timeouts.1);
        let (upstream, upstream_args) = mapping::build_proxy(kind, &resolved, args.get("args"));
        self.run_query(&upstream, &upstream_args, &resolved, timeout_secs, ctx).await
    }

    async fn run_query(
        &self,
        upstream: &str,
        upstream_args: &serde_json::Value,
        resolved_path: &str,
        timeout_secs: u64,
        ctx: &ToolContext,
    ) -> ToolOutput {
        let start = std::time::Instant::now();
        tracing::info!(session_id = %ctx.session_id, upstream = %upstream, jfr = %resolved_path, timeout_secs, "jfr tool executing");
        match self.jmc.query(upstream, upstream_args, timeout_secs).await {
            Ok(outcome) => {
                render(&ctx.session_id, &self.artifacts_dir, upstream, resolved_path, &outcome.text, start, true)
                    .await
            }
            Err(e) => {
                tracing::warn!(session_id = %ctx.session_id, upstream = %upstream, error = %e, "jfr tool failed");
                self.jmc_error_output(e, &ctx.session_id, upstream, resolved_path, start)
                    .await
            }
        }
    }

    /// JmcError → 结构化错误输出。Upstream（JMC 业务错误）走透传（无 error code，
    /// 对齐 heap_*/jvm_* 惯例），但同样经过 64KB 截断 + 完整结果落盘路径。
    async fn jmc_error_output(
        &self,
        e: JmcError,
        session_id: &str,
        upstream_tool: &str,
        local_path: &str,
        start: std::time::Instant,
    ) -> ToolOutput {
        match e {
            JmcError::JavaMissing(m) => error_output(
                "java_missing",
                &format!("本机 Java 21+ 不可用：{m}。请安装 JDK 21+ 后重试。"),
            ),
            JmcError::Unavailable(m) => error_output(
                "jmc_unavailable",
                &format!("{m}。可重试一次；连续失败请查看 Friday 日志。"),
            ),
            JmcError::Timeout(t) => error_output(
                "jmc_timeout",
                &format!("JMC 分析调用超时（{t}s）。工人进程未受影响，可加大 timeout_secs 或用 start_time/end_time 缩小时间窗后重试。"),
            ),
            JmcError::Upstream(text) => {
                render(session_id, &self.artifacts_dir, upstream_tool, local_path, &text, start, false).await
            }
        }
    }
}

/// 结果组装：64KB 头部截断 + 完整结果落盘 session artifacts（复用 run_command 机制）。
/// success=false 用于上游业务错误透传（upstream_is_error 标记，无 error code）。
async fn render(
    session_id: &str,
    artifacts_dir: &Path,
    upstream_tool: &str,
    local_path: &str,
    text: &str,
    start: std::time::Instant,
    success: bool,
) -> ToolOutput {
    let elapsed_ms = start.elapsed().as_millis() as u64;
    let (body, truncated) = truncate_output(text);
    let session_dir = artifact_dir_for(artifacts_dir, session_id);
    let artifact_path = session_dir.join(format!("jfr-{}.md", uuid::Uuid::new_v4()));
    let full = format!("--- tool: {upstream_tool} ---\n--- local_path: {local_path} ---\n--- full output ---\n{text}\n");
    let mut full_output_path = None;
    match tokio::fs::create_dir_all(&session_dir).await {
        Ok(()) => {
            if tokio::fs::write(&artifact_path, &full).await.is_ok() {
                full_output_path = Some(artifact_path);
            } else {
                tracing::warn!(session_id, tool = upstream_tool, "failed to persist full jfr tool output");
            }
        }
        Err(e) => {
            tracing::warn!(session_id, tool = upstream_tool, error = %e, "failed to create artifacts dir");
        }
    }
    let result_field = if truncated {
        match &full_output_path {
            Some(p) => format!("{body}\n[truncated, full output: {}]", p.display()),
            None => format!("{body}\n[truncated]"),
        }
    } else {
        body
    };
    if success {
        tracing::info!(session_id, tool = upstream_tool, elapsed_ms, truncated, "jfr tool executed");
    } else {
        tracing::warn!(session_id, tool = upstream_tool, elapsed_ms, truncated, "jfr tool upstream error passthrough");
    }
    let mut data = serde_json::json!({
        "tool": upstream_tool,
        "local_path": local_path,
        "result": result_field,
        "elapsed_ms": elapsed_ms,
        "truncated": truncated,
        "full_output_path": full_output_path.as_ref().map(|p| p.display().to_string()),
    });
    if !success {
        data["upstream_is_error"] = serde_json::json!(true);
    }
    ToolOutput {
        success,
        data,
        raw_stdout: Some(text.to_string()),
    }
}

/// local_path 解析：相对路径以 cwd 补全 + 必须是已存在文件。
fn resolve_existing_file(raw: &str) -> Result<PathBuf, String> {
    if raw.trim().is_empty() {
        return Err("local_path 不能为空".into());
    }
    let mut p = PathBuf::from(raw);
    if p.is_relative() {
        let cwd = std::env::current_dir().map_err(|e| e.to_string())?;
        p = cwd.join(p);
    }
    if !p.is_file() {
        return Err(format!("文件不存在: {}", p.display()));
    }
    Ok(p)
}

fn proxy_tool_def(
    name: &str,
    description: &str,
    kind: JfrToolKind,
    timeouts: Timeouts,
    jmc: &Arc<JmcManager>,
    artifacts_dir: &Path,
) -> ToolDef {
    let schema = match kind {
        JfrToolKind::Compare => serde_json::json!({
            "type": "object",
            "properties": {
                "baseline_local_path": { "type": "string", "description": "基准录制（如正常期）的本机路径" },
                "target_local_path": { "type": "string", "description": "对比录制（如故障期）的本机路径" },
                "args": { "type": "object", "description": "上游选项透传（start_time/end_time 等）" },
                "timeout_secs": { "type": "number", "description": format!("超时秒数，默认 {}，上限 {}", timeouts.0, timeouts.1) }
            },
            "required": ["baseline_local_path", "target_local_path"]
        }),
        JfrToolKind::Proxy(_) => serde_json::json!({
            "type": "object",
            "properties": {
                "local_path": { "type": "string", "description": "本机 JFR 录制文件绝对路径（jfr_record 返回的 local_path 或用户已有文件）" },
                "args": { "type": "object", "description": "上游分析选项透传（如 top_n / thread_name / package_prefix / focus / class_pattern / start_time / end_time，见工具描述）" },
                "timeout_secs": { "type": "number", "description": format!("超时秒数，默认 {}，上限 {}", timeouts.0, timeouts.1) }
            },
            "required": ["local_path"]
        }),
    };
    ToolDef {
        name: name.to_string(),
        description: description.to_string(),
        input_schema: schema,
        risk_level: RiskLevel::ReadOnly,
        category: ToolCategory::Jfr,
        needs_channel: false,
        handler: Arc::new(JfrProxyHandler {
            jmc: jmc.clone(),
            artifacts_dir: artifacts_dir.to_path_buf(),
            kind,
            timeouts,
        }),
    }
}

/// 注册全部 jfr_* 工具（lib.rs 调用）：1 录制 + 1 录制状态 + 20 代理 + 1 对比
pub fn register_all(
    registry: &mut crate::tools::registry::ToolRegistry,
    jmc: Arc<JmcManager>,
    core: Arc<JvmExecCore>,
    bus: EventBus,
    transfer: Arc<crate::transfer::TransferManager>,
    artifacts_dir: PathBuf,
) {
    // 录制注册表：jfr_record（写入/流转）与 jfr_record_status（查询/观测）共享
    let recordings = Arc::new(record::RecordingRegistry::new());
    registry.register(record::record_tool_def(&core, &bus, &transfer, &recordings));
    registry.register(record::record_status_tool_def(&transfer, &recordings));

    // (Friday 名, 描述, 代理类型, 超时档)
    let proxies: &[(&str, &str, mapping::JfrProxyKind, Timeouts)] = &[
        ("jfr_overview", "JFR 录制总览：录制时长、事件数、JVM/系统信息。分析起点（jfr_record 完成预热后秒回）。args 可选：start_time/end_time。", mapping::JfrProxyKind::Overview, QUERY),
        ("jfr_rules", "JMC 规则引擎自动瓶颈检测（GC/内存/CPU/锁/IO 规则，带严重度与建议）。录制体检首选。args 可选：min_severity/start_time/end_time。", mapping::JfrProxyKind::Rules, QUERY),
        ("jfr_quick_analysis", "一键宏诊断仪表盘：自动检测主瓶颈并按严重度分类（CPU/内存/锁/IO）。性能问题第一步。args 可选：focus（cpu/memory/locks/io）/start_time/end_time。", mapping::JfrProxyKind::QuickAnalysis, HEAVY),
        ("jfr_gc_detail", "GC 深度分析：分阶段暂停耗时、GC cause 分布、堆趋势、GC 配置。args 可选：detail_level/start_time/end_time。", mapping::JfrProxyKind::GcDetail, QUERY),
        ("jfr_memory_leaks", "老对象采样泄漏分析：按类统计存活老对象（JFR 对象采样），定位疑似泄漏类；与 heap_*（MAT）互补。args 可选：top_n/start_time/end_time。", mapping::JfrProxyKind::MemoryLeaks, HEAVY),
        ("jfr_predictive_leak", "数学检测内存泄漏：对 post-GC 堆使用做线性回归（r_squared 拟合度），泄漏趋势确认。args 可选：r_squared_threshold/start_time/end_time。", mapping::JfrProxyKind::PredictiveLeak, HEAVY),
        ("jfr_allocation_hotspots", "内存分配热点：按类和分配调用点统计分配速率，定位分配风暴。args 可选：top_n/start_time/end_time。", mapping::JfrProxyKind::AllocationHotspots, QUERY),
        ("jfr_hot_methods", "CPU 热点方法 Top N（执行采样）。args 可选：top_n/thread_name/package_prefix/start_time/end_time。", mapping::JfrProxyKind::HotMethods, QUERY),
        ("jfr_thread_cpu", "线程级 CPU 消耗排名（执行采样）。args 可选：top_n/package_prefix/start_time/end_time。", mapping::JfrProxyKind::ThreadCpu, QUERY),
        ("jfr_cpu_flame", "CPU 火焰图数据：热点调用路径 + 线程状态。args 可选：top_n/package_prefix/start_time/end_time。", mapping::JfrProxyKind::CpuFlame, HEAVY),
        ("jfr_thread_contention", "锁竞争分析：monitor 阻塞/挂起/等待统计。args 可选：top_n/start_time/end_time。", mapping::JfrProxyKind::ThreadContention, QUERY),
        ("jfr_deadlock_detection", "死锁环检测：monitor 持有/等待关系分析。args 可选：start_time/end_time。", mapping::JfrProxyKind::DeadlockDetection, QUERY),
        ("jfr_io_hotspots", "IO 热点：慢/高频文件与 socket 操作（按路径/主机），含调用点。args 可选：io_type/top_n/start_time/end_time。", mapping::JfrProxyKind::IoHotspots, QUERY),
        ("jfr_exceptions", "异常抛出统计：按异常类统计次数与栈。args 可选：top_n/start_time/end_time。", mapping::JfrProxyKind::Exceptions, QUERY),
        ("jfr_errors", "严重错误分析：OutOfMemoryError/StackOverflowError 等按严重度分类。args 可选：top_n/start_time/end_time。", mapping::JfrProxyKind::Errors, QUERY),
        ("jfr_safepoints", "safepoint 分析：GC 外 STW 暂停（vm operation 耗时），延迟毛刺定位。args 可选：top_n/start_time/end_time。", mapping::JfrProxyKind::Safepoints, QUERY),
        ("jfr_virtual_threads", "虚拟线程分析（JDK 21+）：虚拟线程存在性与规模（distinct 计数、按事件类型活动分布，基于 Thread.virtual 标志）、pinning 位点/原因/载体线程/总耗时、提交失败。无 pinning 也能判断虚拟线程是否在用。args 可选：top_n/start_time/end_time。", mapping::JfrProxyKind::VirtualThreads, QUERY),
        ("jfr_stack_trace_search", "跨 13 类事件全栈正则搜索（非截断栈）。找人/找路径利器。args 必填：class_pattern；可选 event_type/limit/start_time/end_time。", mapping::JfrProxyKind::StackTraceSearch, HEAVY),
        ("jfr_correlate", "跨维度相关性引擎：锁↔IO↔热点方法关联成瓶颈链。args 可选：dimension/top_n/start_time/end_time。", mapping::JfrProxyKind::Correlate, HEAVY),
        ("jfr_request_waterfall", "线程时序瀑布：按时间顺序串联 锁→IO→CPU→异常 事件。args 必填：thread_name；可选 max_events/start_time/end_time。", mapping::JfrProxyKind::RequestWaterfall, HEAVY),
    ];
    for (name, desc, kind, timeouts) in proxies {
        registry.register(proxy_tool_def(
            name,
            desc,
            JfrToolKind::Proxy(*kind),
            *timeouts,
            &jmc,
            &artifacts_dir,
        ));
    }

    registry.register(proxy_tool_def(
        "jfr_compare",
        "两个 JFR 录制的 A/B 对比（优化前后、故障期 vs 正常期）：事件量/热点/暂停等维度差异汇总。",
        JfrToolKind::Compare,
        HEAVY,
        &jmc,
        &artifacts_dir,
    ));
}

#[cfg(test)]
mod tests {
    use super::register_all;
    use crate::app::events::EventBus;
    use crate::exec::channel::{ExecChannel, ExecOutput};
    use crate::jfr::client::MockJmcClient;
    use crate::jfr::manager::{ClientFactory, JmcConfig, JmcManager};
    use crate::tools::builtin::jvm::core::JvmExecCore;
    use crate::tools::builtin::jvm::jdk_cache::JdkLayout;
    use crate::tools::category::ToolCategory;
    use crate::tools::registry::{ToolContext, ToolRegistry};
    use crate::tools::risk::RiskLevel;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    const SID: &str = "123e4567-e89b-12d3-a456-426614174000";

    /// JFR 感知的可编程 mock channel（对齐 heap_dump.rs 的 DumpChannel 模式）。
    /// JFR.start / JFR.check / stat 按 run 路由；download 落 stat_size 字节的本地
    /// 文件（供拉回 worker 完成大小校验 → rename → completed 全链路）。
    struct JfrChannel {
        start_exit: i32,
        check_stdout: &'static str,
        stat_size: &'static str,
        calls: TokioMutex<Vec<String>>,
    }

    #[async_trait]
    impl ExecChannel for JfrChannel {
        async fn run(
            &self,
            cmd: &str,
        ) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            self.calls.lock().await.push(cmd.to_string());
            if cmd.contains("JFR.start") {
                return Ok(ExecOutput {
                    stdout: String::new(),
                    stderr: String::new(),
                    exit_code: self.start_exit,
                });
            }
            if cmd.contains("JFR.check") {
                return Ok(ExecOutput {
                    stdout: self.check_stdout.to_string(),
                    stderr: String::new(),
                    exit_code: 0,
                });
            }
            if cmd.starts_with("stat -c %s") {
                return Ok(ExecOutput {
                    stdout: self.stat_size.to_string(),
                    stderr: String::new(),
                    exit_code: 0,
                });
            }
            Ok(ExecOutput { stdout: String::new(), stderr: String::new(), exit_code: 0 })
        }
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        async fn disconnect(&self) {}
        async fn is_alive(&self) -> bool {
            true
        }
        async fn download(
            &self,
            _remote_path: &str,
            local: &std::path::Path,
            _offset: u64,
            _progress: &(dyn Fn(u64, u64) + Sync),
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            // 对齐真实 SshTransport::download：落盘前创建父目录
            if let Some(parent) = local.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let n: usize = self.stat_size.trim().parse().unwrap_or(0);
            std::fs::write(local, vec![b'x'; n])?;
            _progress(n as u64, 1024);
            Ok(())
        }
    }

    async fn setup_as(channel: Arc<dyn ExecChannel>, transport: &str) -> (tempfile::TempDir, Arc<JvmExecCore>, Arc<crate::transfer::TransferManager>) {
        // 注意：调用方在 setup 完成后才 pause 时钟（而非 start_paused 全程暂停）——
        // sqlx 建新连接走真实 IO，而 pool acquire_timeout 是 tokio 定时器，全程暂停的
        // auto-advance 会在真实连接完成前把时钟推到超时点（PoolTimedOut，且嵌套
        // runtime 建 pool 会产生随其销毁的僵尸连接）。
        let (tmp, core, env_id) =
            crate::tools::builtin::jvm::core::test_support::setup_env_with_channel(transport, channel.clone()).await;
        let mut bins = HashMap::new();
        bins.insert("jcmd".to_string(), "/tmp/jdk/bin/jcmd".to_string());
        core.jdk_cache.set(&env_id, JdkLayout { tool_home: "/tmp/jdk".into(), bins }).await;
        // 后台等待/拉回 worker 走 TransferManager 专用连接：注入 factory 返回同一 mock，
        // 同时规避 paused 时钟下的真实 SSH 建连
        let mut tm = crate::transfer::TransferManager::new(core.db.clone(), EventBus::disabled());
        let ch = channel.clone();
        tm.set_channel_factory(Arc::new(move || {
            let ch = ch.clone();
            Box::pin(async move { Ok(ch) })
        }));
        let mgr = Arc::new(tm);
        (tmp, core, mgr)
    }

    async fn setup(channel: Arc<dyn ExecChannel>) -> (tempfile::TempDir, Arc<JvmExecCore>, Arc<crate::transfer::TransferManager>) {
        setup_as(channel, "vm").await
    }

    fn jmc_manager(mock: Arc<MockJmcClient>) -> Arc<JmcManager> {
        let factory: ClientFactory = Arc::new(move || {
            let m = mock.clone();
            Box::pin(async move { Ok(m as Arc<dyn crate::jfr::client::JmcClient>) })
        });
        Arc::new(JmcManager::new(factory, EventBus::disabled(), JmcConfig::default()))
    }

    fn ctx() -> ToolContext {
        ToolContext { session_id: SID.into(), channel: None }
    }

    fn def<'a>(reg: &'a ToolRegistry, name: &str) -> &'a crate::tools::registry::ToolDef {
        reg.get(name).unwrap()
    }

    async fn registry(
        channel: Arc<dyn ExecChannel>,
        mock: Arc<MockJmcClient>,
    ) -> (tempfile::TempDir, ToolRegistry) {
        registry_as(channel, mock, "vm").await
    }

    async fn registry_as(
        channel: Arc<dyn ExecChannel>,
        mock: Arc<MockJmcClient>,
        transport: &str,
    ) -> (tempfile::TempDir, ToolRegistry) {
        let (tmp, core, transfer) = setup_as(channel, transport).await;
        let mut reg = ToolRegistry::new();
        register_all(
            &mut reg,
            jmc_manager(mock),
            core,
            EventBus::disabled(),
            transfer,
            tmp.path().join("artifacts"),
        );
        (tmp, reg)
    }

    /// 容器环境 + pod 目标（k8s 复合键通道 + JDK 缓存条目）注册全量 jfr 工具。
    /// 返回 TransferManager 供断言拉回任务 state。
    async fn registry_pod_target(
        channel: Arc<dyn ExecChannel>,
        mock: Arc<MockJmcClient>,
    ) -> (
        tempfile::TempDir,
        ToolRegistry,
        Arc<crate::transfer::TransferManager>,
    ) {
        let (tmp, core, transfer) = setup_as(channel.clone(), "container").await;
        let env_id = crate::app::environments::find_by_name(&core.db, "prod").await.unwrap().unwrap().id;
        let target = crate::exec::pool::TargetKey::k8s(&env_id, "pod-1", Some("ns1"), None);
        core.exec_pool.lock().await.insert_channel(target.clone(), channel).await;
        let mut bins = HashMap::new();
        bins.insert("jcmd".to_string(), "/opt/log/dump/coredump/friday-tools/jdk/bin/jcmd".to_string());
        core.jdk_cache
            .set(
                &crate::tools::builtin::jvm::jdk_cache::cache_key(&target),
                JdkLayout { tool_home: "/opt/log/dump/coredump/friday-tools/jdk".into(), bins },
            )
            .await;
        let mut reg = ToolRegistry::new();
        register_all(
            &mut reg,
            jmc_manager(mock),
            core,
            EventBus::disabled(),
            transfer.clone(),
            tmp.path().join("artifacts"),
        );
        (tmp, reg, transfer)
    }

    fn jfr_file(dir: &std::path::Path) -> std::path::PathBuf {
        let p = dir.join("a.jfr");
        std::fs::write(&p, "fake jfr").unwrap();
        p
    }

    const CHECK_RUNNING: &str = "Recording 1: name=friday duration=10s (running)\n";

    fn std_channel(stat: &'static str) -> Arc<JfrChannel> {
        Arc::new(JfrChannel {
            start_exit: 0,
            check_stdout: CHECK_RUNNING,
            stat_size: stat,
            calls: TokioMutex::new(Vec::new()),
        })
    }

    /// 虚拟时钟起搏器：常驻 1ms 定时任务，把 auto-advance 的推进粒度钳制在 1ms。
    /// 无起搏时，runtime 空闲即跳到下一个 pending 定时器——sqlx 30s acquire 超时
    /// 定时器会在其真实 IO（连接归还/查询往返，µs 级）完成前被瞬间穿透（PoolTimedOut）。
    /// 用后 abort。
    fn spawn_auto_advance_pacer() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
    }

    /// 轮询 jfr_record_status 直到终态（completed/failed）。返回最后一次响应的
    /// data JSON（含 status/transfer_id/local_path 等）。虚拟时钟下由 auto-advance
    /// 推进等待时长；pacer 需保持运行。迭代上限兜底防死循环。
    async fn poll_status_to_terminal(reg: &ToolRegistry, rid: &str) -> serde_json::Value {
        for _ in 0..5000 {
            let out = def(reg, "jfr_record_status")
                .handler
                .execute(serde_json::json!({"recording_id": rid}), &ctx())
                .await;
            assert!(out.success, "status out: {}", out.data);
            let status = out.data["status"].as_str().unwrap().to_string();
            if status == "completed" || status == "failed" {
                return out.data;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("recording {rid} never reached terminal state");
    }

    #[tokio::test]
    async fn test_register_all_twenty_three_tools() {
        let (tmp, reg) = registry(std_channel("1"), Arc::new(MockJmcClient::ok("S"))).await;
        let expected = [
            "jfr_record",
            "jfr_record_status",
            "jfr_overview",
            "jfr_rules",
            "jfr_quick_analysis",
            "jfr_gc_detail",
            "jfr_memory_leaks",
            "jfr_predictive_leak",
            "jfr_allocation_hotspots",
            "jfr_hot_methods",
            "jfr_thread_cpu",
            "jfr_cpu_flame",
            "jfr_thread_contention",
            "jfr_deadlock_detection",
            "jfr_io_hotspots",
            "jfr_exceptions",
            "jfr_errors",
            "jfr_safepoints",
            "jfr_virtual_threads",
            "jfr_stack_trace_search",
            "jfr_correlate",
            "jfr_request_waterfall",
            "jfr_compare",
        ];
        assert_eq!(expected.len(), 23);
        for name in expected {
            let d = def(&reg, name);
            assert_eq!(d.category, ToolCategory::Jfr, "{name}");
            assert!(!d.needs_channel, "{name}");
        }
        assert_eq!(def(&reg, "jfr_record").risk_level, RiskLevel::Low);
        assert_eq!(def(&reg, "jfr_record_status").risk_level, RiskLevel::ReadOnly);
        assert_eq!(def(&reg, "jfr_overview").risk_level, RiskLevel::ReadOnly);
        assert_eq!(def(&reg, "jfr_compare").risk_level, RiskLevel::ReadOnly);
        drop(tmp);
    }

    /// issue #23 核心回归：jfr_record 在 JFR.start + JFR.check 后**立即返回**
    /// recording_id（不等 duration 落盘，不超 MCP 客户端 120s 硬超时）；后台等待
    /// 落盘 → 拉回 → completed 全链路经 jfr_record_status 轮询闭环。
    /// 录制流程开始前手动 pause 时钟 + 起搏器任务（setup 走真实时钟）。
    #[tokio::test]
    async fn test_record_returns_immediately_and_completes_via_status_poll() {
        let ch = std_channel("54321");
        let (tmp, reg) = registry(ch.clone(), Arc::new(MockJmcClient::ok("S"))).await;
        tokio::time::pause();
        let pacer = spawn_auto_advance_pacer();
        let start = tokio::time::Instant::now();
        let out = def(&reg, "jfr_record")
            .handler
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "duration_secs": 10, "timeout_secs": 30}),
                &ctx(),
            )
            .await;
        // 立即返回：工具调用本身不等待 duration（虚拟时钟仅推进了 start/check 的 0s）
        assert!(start.elapsed().as_secs() < 10, "jfr_record must not block for duration");
        assert!(out.success, "out: {}", out.data);
        let rid = out.data["recording_id"].as_str().unwrap();
        assert!(!rid.is_empty());
        assert_eq!(out.data["status"], "recording");
        assert!(out.data["note"].as_str().unwrap().contains("jfr_record_status"));
        assert!(out.data["local_path"].as_str().unwrap().ends_with(".jfr"));

        // 后台链路：等待落盘 → 拉回 → completed
        let done = poll_status_to_terminal(&reg, rid).await;
        assert_eq!(done["status"], "completed", "final: {done}");
        assert_eq!(done["remote_size"], 54321);
        let tid = done["transfer_id"].as_str().unwrap();
        assert!(!tid.is_empty());
        // 本地文件真实落盘（download mock 写入 stat_size 字节）
        let local = done["local_path"].as_str().unwrap();
        assert_eq!(std::fs::metadata(local).map(|m| m.len()).unwrap_or(0), 54321);

        // 命令序列：JFR.start → JFR.check（校验在运行）→ 若干 stat 轮询（后台等待 +
        // 拉回 worker stat）→ rm -f（成功后清理远端）
        let calls = ch.calls.lock().await;
        assert!(calls[0].contains("JFR.start"), "calls[0]: {}", calls[0]);
        assert!(calls[0].contains("duration=10s"));
        assert!(calls[0].contains("settings=profile"));
        assert!(calls[0].contains("filename=/tmp/friday-tools/recording-1234-"));
        assert!(calls[1].contains("JFR.check"), "calls[1]: {}", calls[1]);
        assert!(calls.iter().skip(2).filter(|c| c.starts_with("stat -c %s")).count() >= 2);
        assert!(calls.iter().any(|c| c.starts_with("rm -f")), "remote cleanup expected: {calls:?}");
        pacer.abort();
        drop(tmp);
    }

    #[tokio::test]
    async fn test_record_start_failure_passthrough_with_jdk8_hint() {
        let ch = Arc::new(JfrChannel {
            start_exit: 1,
            check_stdout: CHECK_RUNNING,
            stat_size: "0",
            calls: TokioMutex::new(Vec::new()),
        });
        let (tmp, reg) = registry(ch, Arc::new(MockJmcClient::ok("S"))).await;
        let out = def(&reg, "jfr_record")
            .handler
            .execute(serde_json::json!({"environment": "prod", "pid": "1234"}), &ctx())
            .await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "record_failed");
        assert!(
            out.data["message"].as_str().unwrap().contains("arthas_profiler"),
            "JDK 8 fallback hint required"
        );
        drop(tmp);
    }

    /// issue #23 问题 2 回归：JFR.start exit 0 但 JFR.check 未确认在运行且远端文件
    /// 不存在 → record_verify_failed 快速失败，且不注册录制任务
    #[tokio::test]
    async fn test_record_verify_failed_when_check_not_running() {
        let ch = Arc::new(JfrChannel {
            start_exit: 0,
            check_stdout: "Could not find recording with name friday-777\n",
            stat_size: "0",
            calls: TokioMutex::new(Vec::new()),
        });
        let (tmp, reg) = registry(ch, Arc::new(MockJmcClient::ok("S"))).await;
        let out = def(&reg, "jfr_record")
            .handler
            .execute(serde_json::json!({"environment": "prod", "pid": "1234"}), &ctx())
            .await;
        assert!(!out.success, "out: {}", out.data);
        assert_eq!(out.data["error"], "record_verify_failed");
        assert!(out.data["message"].as_str().unwrap().contains("friday-tools"));
        // 未注册录制任务：会话列表为空
        let list = def(&reg, "jfr_record_status").handler.execute(serde_json::json!({}), &ctx()).await;
        assert!(list.success);
        assert_eq!(list.data["recordings"].as_array().unwrap().len(), 0);
        drop(tmp);
    }

    /// 短 duration + 慢 attach 边角：JFR.check 已查不到（录制瞬间完成）但远端文件
    /// 已非空 → 放行，交给后台等待稳定判定
    #[tokio::test]
    async fn test_record_check_passes_when_file_already_written() {
        let ch = Arc::new(JfrChannel {
            start_exit: 0,
            check_stdout: "Could not find recording with name friday-777\n",
            stat_size: "100",
            calls: TokioMutex::new(Vec::new()),
        });
        let (tmp, reg) = registry(ch, Arc::new(MockJmcClient::ok("S"))).await;
        tokio::time::pause();
        let pacer = spawn_auto_advance_pacer();
        let out = def(&reg, "jfr_record")
            .handler
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "duration_secs": 10, "timeout_secs": 30}),
                &ctx(),
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        let rid = out.data["recording_id"].as_str().unwrap();
        let done = poll_status_to_terminal(&reg, rid).await;
        assert_eq!(done["status"], "completed", "final: {done}");
        pacer.abort();
        drop(tmp);
    }

    /// 落盘等待超预算（后台，不再阻塞工具调用）：jfr_record 本身成功返回，
    /// 轮询到 failed（record_not_found 语义文案 + 远端路径）
    #[tokio::test]
    async fn test_record_file_never_materializes_fails_in_background() {
        let (tmp, reg) = registry(std_channel("0"), Arc::new(MockJmcClient::ok("S"))).await;
        tokio::time::pause();
        let pacer = spawn_auto_advance_pacer();
        let out = def(&reg, "jfr_record")
            .handler
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "duration_secs": 10, "timeout_secs": 30}),
                &ctx(),
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        assert_eq!(out.data["status"], "recording");
        let rid = out.data["recording_id"].as_str().unwrap();
        let done = poll_status_to_terminal(&reg, rid).await;
        assert_eq!(done["status"], "failed", "final: {done}");
        assert!(
            done["error"].as_str().unwrap().contains("friday-tools"),
            "error should mention remote path: {}",
            done["error"]
        );
        pacer.abort();
        drop(tmp);
    }

    /// issue #23 典型场景回归：客户端超时后 Agent 重试 jfr_record —— 同 JVM 已有
    /// 活跃录制时拒绝重复启动并复用 recording_id（不再叠加录制开销）
    #[tokio::test]
    async fn test_record_duplicate_active_recording_reuses_id() {
        let ch = std_channel("54321");
        let (tmp, reg) = registry(ch.clone(), Arc::new(MockJmcClient::ok("S"))).await;
        let first = def(&reg, "jfr_record")
            .handler
            .execute(serde_json::json!({"environment": "prod", "pid": "1234", "duration_secs": 60}), &ctx())
            .await;
        assert!(first.success, "out: {}", first.data);
        let rid = first.data["recording_id"].as_str().unwrap().to_string();
        let second = def(&reg, "jfr_record")
            .handler
            .execute(serde_json::json!({"environment": "prod", "pid": "1234", "duration_secs": 60}), &ctx())
            .await;
        assert!(!second.success, "out: {}", second.data);
        assert_eq!(second.data["error"], "duplicate_recording");
        assert_eq!(second.data["recording_id"].as_str().unwrap(), rid);
        assert!(second.data["note"].as_str().unwrap().contains("jfr_record_status"));
        // 第二次调用未再执行 JFR.start（calls 只有第一次的 start + check）
        let calls = ch.calls.lock().await;
        assert_eq!(calls.iter().filter(|c| c.contains("JFR.start")).count(), 1);
        drop(tmp);
    }

    #[tokio::test]
    async fn test_record_status_unknown_id() {
        let (tmp, reg) = registry(std_channel("1"), Arc::new(MockJmcClient::ok("S"))).await;
        let out = def(&reg, "jfr_record_status")
            .handler
            .execute(serde_json::json!({"recording_id": "nope"}), &ctx())
            .await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "invalid_args");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_record_status_empty_lists_session_recordings() {
        let (tmp, reg) = registry(std_channel("1"), Arc::new(MockJmcClient::ok("S"))).await;
        let out = def(&reg, "jfr_record_status").handler.execute(serde_json::json!({}), &ctx()).await;
        assert!(out.success);
        assert_eq!(out.data["recordings"].as_array().unwrap().len(), 0);
        drop(tmp);
    }

    #[tokio::test]
    async fn test_container_env_requires_pod() {
        // 容器环境 + 缺 pod → environment_type_mismatch（引导 k8s_find_pods）
        let (tmp, reg) = registry_as(std_channel("1"), Arc::new(MockJmcClient::ok("S")), "container").await;
        let out = def(&reg, "jfr_record")
            .handler
            .execute(serde_json::json!({"environment": "prod", "pid": "1234"}), &ctx())
            .await;
        assert!(!out.success, "out: {}", out.data);
        assert_eq!(out.data["error"], "environment_type_mismatch");
        assert!(out.data["message"].as_str().unwrap().contains("k8s_find_pods"));
        drop(tmp);
    }

    /// 容器目标：录制落 POD_DUMP_DIR（coredump 卷），文件名 friday- 前缀；
    /// 拉回任务 state 带 pod/namespace（worker 专用连接走 K8sChannel 两跳）。
    /// 起搏器说明同 test_record_returns_immediately_and_completes_via_status_poll。
    #[tokio::test]
    async fn test_record_pod_target_uses_pod_dump_dir() {
        let ch = std_channel("54321");
        let (tmp, reg, mgr) = registry_pod_target(ch.clone(), Arc::new(MockJmcClient::ok("S"))).await;
        tokio::time::pause();
        let pacer = spawn_auto_advance_pacer();
        let out = def(&reg, "jfr_record")
            .handler
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "duration_secs": 10, "timeout_secs": 30, "pod": "pod-1", "namespace": "ns1"}),
                &ctx(),
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        let calls = ch.calls.lock().await;
        assert!(
            calls[0].contains("filename=/opt/log/dump/coredump/friday-recording-1234-"),
            "start cmd: {}", calls[0]
        );
        drop(calls);
        let rid = out.data["recording_id"].as_str().unwrap();
        let done = poll_status_to_terminal(&reg, rid).await;
        assert_eq!(done["status"], "completed", "final: {done}");
        // 拉回任务带 pod/namespace 定位
        let tid = done["transfer_id"].as_str().unwrap();
        let st = mgr.get(tid).await.unwrap();
        assert_eq!(st.pod.as_deref(), Some("pod-1"));
        assert_eq!(st.namespace.as_deref(), Some("ns1"));
        assert!(st.container.is_none());
        assert!(st.cleanup_remote_on_success, "Friday 生成的录制文件成功后清理远端");
        pacer.abort();
        drop(tmp);
    }

    #[tokio::test]
    async fn test_record_invalid_args() {
        let (tmp, reg) = registry(std_channel("1"), Arc::new(MockJmcClient::ok("S"))).await;
        for args in [
            serde_json::json!({"environment": "prod"}),
            serde_json::json!({"pid": "1234"}),
            serde_json::json!({"environment": "prod", "pid": "1234", "duration_secs": 5}),
            serde_json::json!({"environment": "prod", "pid": "1234", "settings": "boot"}),
            serde_json::json!({"environment": "prod", "pid": "1; rm -rf /"}),
        ] {
            let out = def(&reg, "jfr_record").handler.execute(args, &ctx()).await;
            assert!(!out.success, "args should be rejected");
            assert_eq!(out.data["error"], "invalid_args");
        }
        drop(tmp);
    }

    #[tokio::test]
    async fn test_record_environment_not_found() {
        let (tmp, reg) = registry(std_channel("1"), Arc::new(MockJmcClient::ok("S"))).await;
        let out = def(&reg, "jfr_record")
            .handler
            .execute(serde_json::json!({"environment": "nope", "pid": "1234"}), &ctx())
            .await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "environment_not_found");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_proxy_routes_to_upstream_with_path_and_sync() {
        let mock = Arc::new(MockJmcClient::ok("OVERVIEW"));
        let (tmp, reg) = registry(std_channel("1"), mock.clone()).await;
        let p = jfr_file(tmp.path());
        let out = def(&reg, "jfr_overview")
            .handler
            .execute(
                serde_json::json!({"local_path": p.to_string_lossy(), "args": {"start_time": "2026-09-03T10:00:00Z"}}),
                &ctx(),
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        assert_eq!(out.data["tool"], "jfrOverview");
        let calls = mock.calls.lock().await;
        let (name, args) = calls.last().unwrap();
        assert_eq!(name, "jfrOverview");
        assert_eq!(args["jfr_file_path"].as_str().unwrap(), p.to_string_lossy());
        assert_eq!(args["start_time"], "2026-09-03T10:00:00Z");
        assert_eq!(args["async"], false);
        drop(tmp);
    }

    #[tokio::test]
    async fn test_proxy_missing_params_and_file() {
        let (tmp, reg) = registry(std_channel("1"), Arc::new(MockJmcClient::ok("S"))).await;
        // 缺 local_path
        let out = def(&reg, "jfr_hot_methods").handler.execute(serde_json::json!({}), &ctx()).await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "invalid_args");
        // 文件不存在
        let out = def(&reg, "jfr_hot_methods")
            .handler
            .execute(serde_json::json!({"local_path": "C:/definitely/nope.jfr"}), &ctx())
            .await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "invalid_path");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_compare_maps_two_paths() {
        let mock = Arc::new(MockJmcClient::ok("DIFF"));
        let (tmp, reg) = registry(std_channel("1"), mock.clone()).await;
        let base = jfr_file(tmp.path());
        let target = {
            let p = tmp.path().join("b.jfr");
            std::fs::write(&p, "fake").unwrap();
            p
        };
        let out = def(&reg, "jfr_compare")
            .handler
            .execute(
                serde_json::json!({
                    "baseline_local_path": base.to_string_lossy(),
                    "target_local_path": target.to_string_lossy()
                }),
                &ctx(),
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        assert_eq!(out.data["tool"], "compareRecordings");
        let calls = mock.calls.lock().await;
        let (name, args) = calls.last().unwrap();
        assert_eq!(name, "compareRecordings");
        assert_eq!(args["baseline_jfr_path"].as_str().unwrap(), base.to_string_lossy());
        assert_eq!(args["target_jfr_path"].as_str().unwrap(), target.to_string_lossy());
        assert_eq!(args["async"], false);
        drop(tmp);
    }

    #[tokio::test]
    async fn test_compare_requires_both_paths() {
        let (tmp, reg) = registry(std_channel("1"), Arc::new(MockJmcClient::ok("S"))).await;
        let p = jfr_file(tmp.path());
        let out = def(&reg, "jfr_compare")
            .handler
            .execute(serde_json::json!({"baseline_local_path": p.to_string_lossy()}), &ctx())
            .await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "invalid_args");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_jmc_unavailable_error_code() {
        let mock = Arc::new(MockJmcClient::with_fn(|_name, _args| async {
            Err("transport closed".to_string())
        }));
        let (tmp, reg) = registry(std_channel("1"), mock).await;
        let p = jfr_file(tmp.path());
        let out = def(&reg, "jfr_overview")
            .handler
            .execute(serde_json::json!({"local_path": p.to_string_lossy()}), &ctx())
            .await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "jmc_unavailable");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_upstream_error_passthrough_and_truncation() {
        let big = format!("JMC boom\n{}", "x".repeat(70 * 1024));
        let mock = Arc::new(MockJmcClient::with_fn(move |_name, _args| {
            let big = big.clone();
            async move { Ok(crate::analyzer::client::CallOutcome { text: big, is_error: true }) }
        }));
        let (tmp, reg) = registry(std_channel("1"), mock).await;
        let p = jfr_file(tmp.path());
        let out = def(&reg, "jfr_rules")
            .handler
            .execute(serde_json::json!({"local_path": p.to_string_lossy()}), &ctx())
            .await;
        assert!(!out.success);
        // 业务错误透传：无 error code，result 携带上游文本 + upstream_is_error
        assert_eq!(out.data["error"], serde_json::Value::Null);
        assert_eq!(out.data["upstream_is_error"], true);
        assert!(out.data["result"].as_str().unwrap().contains("JMC boom"));
        assert_eq!(out.data["truncated"], true);
        assert!(out.data["result"].as_str().unwrap().contains("[truncated"));
        let full = out.data["full_output_path"].as_str().unwrap();
        assert!(std::fs::metadata(full).map(|m| m.len() as usize > 70 * 1024).unwrap_or(false));
        drop(tmp);
    }
}
