pub mod mapping;

use crate::arthas::manager::{ArthasManager, ManagerError};
use crate::tools::builtin::jvm::core::{clamp_or, error_output, parse_pid, validate_target};
use crate::tools::builtin::run_command::{artifact_dir_for, truncate_output};
use crate::tools::category::ToolCategory;
use crate::tools::registry::{ToolContext, ToolDef, ToolHandler, ToolOutput};
use crate::tools::risk::RiskLevel;
use async_trait::async_trait;
use mapping::{ArthasToolKind, build_args, upstream_name};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// (default_secs, max_secs)
type Timeouts = (u64, u64);
const OPEN: Timeouts = (120, 300);
const CLOSE: Timeouts = (30, 60);
const FAST: Timeouts = (30, 60);
const STREAM: Timeouts = (120, 600);
const PROFILER: Timeouts = (300, 1800);

pub struct ArthasToolHandler {
    pub manager: Arc<ArthasManager>,
    pub db: sqlx::SqlitePool,
    pub artifacts_dir: PathBuf,
    pub kind: ArthasToolKind,
    pub timeouts: Timeouts,
}

#[async_trait]
impl ToolHandler for ArthasToolHandler {
    async fn execute(&self, args: serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        let Some(environment) = args.get("environment").and_then(|v| v.as_str()) else {
            return error_output("invalid_params", "missing required parameter: environment");
        };
        let Some(pid) = args
            .get("pid")
            .and_then(|v| v.as_str())
            .and_then(|s| parse_pid(&serde_json::json!(s)))
        else {
            return error_output("invalid_params", "missing required parameter: pid（正整数字符串）");
        };
        // 按名称查环境
        let env = match crate::app::environments::find_by_name(&self.db, environment).await {
            Ok(Some(env)) => env,
            Ok(None) => {
                return error_output(
                    "environment_not_found",
                    &format!(
                        "环境「{environment}」不存在。请先调用 list_environments 查看可用环境；\
                         若无匹配，请让用户在右侧「环境」面板添加。"
                    ),
                );
            }
            Err(e) => return error_output("lookup_failed", &format!("查询环境失败: {e}")),
        };

        // pod/namespace/container 提取（空串归一为 None，与 SessionKey::from_parts 一致）
        let pod = args.get("pod").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let namespace = args.get("namespace").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let container = args.get("container").and_then(|v| v.as_str()).filter(|s| !s.is_empty());

        // 环境类型门禁：container 必填 pod+namespace（引导 k8s_find_pods）/ vm 拒 pod + k8s 名 DNS-1123 防呆。
        // namespace 目前仅门禁校验——arthas 会话/attach 链路的 ns 贯通见 NS-T3。
        if let Err(msg) = validate_target(&env, pod, namespace, container) {
            tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, kind = ?self.kind, pod = ?pod, error = %msg, "arthas target validation failed");
            return error_output("environment_type_mismatch", &msg);
        }

        let timeout_secs = clamp_or(
            args.get("timeout_secs").and_then(|v| v.as_i64()),
            self.timeouts.0,
            self.timeouts.1,
        );
        let start = Instant::now();
        let label = format!("{}/{}", environment, pid);
        tracing::info!(session_id = %ctx.session_id, kind = ?self.kind, env_id = %env.id, pid, "arthas tool executing");

        match self.kind {
            ArthasToolKind::Open => {
                let java_bin = args.get("java_bin").and_then(|v| v.as_str()).unwrap_or("java");
                match self.manager.open(&ctx.session_id, &env.id, pod, container, pid as i64, java_bin, timeout_secs).await {
                    Ok(outcome) => render(&ctx.session_id, &self.artifacts_dir, "arthas_open", &label, &outcome.summary, start, true).await,
                    Err(e) => self.manager_error_output(e, &ctx.session_id, "arthas_open", &label, start).await,
                }
            }
            ArthasToolKind::Close => {
                let was_open = self.manager.close(&env.id, pod, container, pid as i64).await;
                ToolOutput {
                    success: true,
                    data: serde_json::json!({
                        "tool": "arthas_close",
                        "environment": environment,
                        "pid": pid,
                        "was_open": was_open,
                    }),
                    raw_stdout: None,
                }
            }
            kind => {
                let upstream = upstream_name(kind);
                let upstream_args = match build_args(kind, &args) {
                    Ok(v) => v,
                    Err(e) => return error_output("invalid_params", &e),
                };
                match self.manager.query(&env.id, pod, container, pid as i64, upstream, &upstream_args, timeout_secs).await {
                    Ok(outcome) => {
                        render(&ctx.session_id, &self.artifacts_dir, upstream, &label, &outcome.text, start, !outcome.is_error).await
                    }
                    Err(e) => self.manager_error_output(e, &ctx.session_id, upstream, &label, start).await,
                }
            }
        }
    }
}

impl ArthasToolHandler {
    /// ManagerError → 结构化错误输出（对齐 heap 工具的 manager_error_output 模式）
    async fn manager_error_output(
        &self,
        e: ManagerError,
        session_id: &str,
        upstream_tool: &str,
        label: &str,
        start: Instant,
    ) -> ToolOutput {
        match e {
            ManagerError::Attach(m) => error_output("arthas_attach_failed", &m),
            ManagerError::NotOpen { attaching } => {
                if attaching {
                    error_output("arthas_not_open", "该 JVM 正在 attach 中（首次需下发工具包/建隧道，约 10-60s）。请稍候后重试。")
                } else {
                    error_output("arthas_not_open", "该 JVM 尚未 attach arthas。请先调用 arthas_open(environment, pid)。")
                }
            }
            ManagerError::Timeout(t) => error_output(
                "arthas_timeout",
                &format!("arthas 调用超时（{t}s）。会话未受影响，可加大 timeout_secs 重试。"),
            ),
            ManagerError::Transport(m) => error_output(
                "arthas_transport",
                &format!("arthas 通道传输错误：{m}。会话已失效，请重新调用 arthas_open。"),
            ),
            ManagerError::Upstream(text) => {
                render(session_id, &self.artifacts_dir, upstream_tool, label, &text, start, false).await
            }
        }
    }
}

/// 结果组装：64KB 头部截断 + 完整结果落盘 session artifacts（复用 run_command 机制）
async fn render(
    session_id: &str,
    artifacts_dir: &Path,
    upstream_tool: &str,
    label: &str,
    text: &str,
    start: Instant,
    success: bool,
) -> ToolOutput {
    let elapsed_ms = start.elapsed().as_millis() as u64;
    let (body, truncated) = truncate_output(text);
    let session_dir = artifact_dir_for(artifacts_dir, session_id);
    let artifact_path = session_dir.join(format!("arthas-{}.md", uuid::Uuid::new_v4()));
    let full = format!(
        "--- tool: {upstream_tool} ---\n--- target: {label} ---\n--- full output ---\n{text}\n"
    );
    let mut full_output_path = None;
    match tokio::fs::create_dir_all(&session_dir).await {
        Ok(()) => {
            if tokio::fs::write(&artifact_path, &full).await.is_ok() {
                full_output_path = Some(artifact_path);
            } else {
                tracing::warn!(session_id, tool = upstream_tool, "failed to persist full arthas tool output");
            }
        }
        Err(e) => {
            tracing::warn!(session_id, tool = upstream_tool, error = %e, "failed to create artifacts dir");
        }
    }
    if success {
        tracing::info!(session_id, tool = upstream_tool, elapsed_ms, truncated, "arthas tool executed");
    } else {
        tracing::warn!(session_id, tool = upstream_tool, elapsed_ms, truncated, "arthas tool upstream error passthrough");
    }
    let mut data = serde_json::json!({
        "tool": upstream_tool,
        "target": label,
        "elapsed_ms": elapsed_ms,
        "output": body,
        "truncated": truncated,
    });
    if let Some(p) = full_output_path {
        data["full_output_path"] = serde_json::json!(p.display().to_string());
    }
    ToolOutput { success, data, raw_stdout: None }
}

/// 注册全部 27 个 arthas 工具
pub fn register_all(
    registry: &mut crate::tools::registry::ToolRegistry,
    manager: Arc<ArthasManager>,
    db: sqlx::SqlitePool,
    artifacts_dir: PathBuf,
) {
    // (name, description, risk, timeouts, kind)
    let defs: Vec<(&str, &str, RiskLevel, Timeouts, ArthasToolKind)> = vec![
        ("arthas_open",
         "attach arthas 到目标 JVM 并建立诊断通道（幂等，已 attach 秒回）。首次自动下发 arthas 工具包（内置随应用分发，无需 Artifactory；仅目标机无 java 需补装 JDK 时才依赖 Artifactory）；SSH 用户与 JVM 用户不一致时需要已录入对应用户凭证。加载 agent 侵入目标 JVM，需确认。容器环境：先 k8s_find_pods 定位 Pod，再带 pod + namespace 参数调用（首次自动装备 arthas 到 Pod）。",
         RiskLevel::Low, OPEN, ArthasToolKind::Open),
        ("arthas_close",
         "停止目标 JVM 上的 arthas agent 并释放通道（卸载字节码增强与 agent，幂等）。诊断完成后调用，或留给空闲自动回收。",
         RiskLevel::ReadOnly, CLOSE, ArthasToolKind::Close),
        ("arthas_dashboard",
         "实时 JVM 面板：线程/内存/GC/运行环境概览。args: {interval?, num?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Dashboard),
        ("arthas_jvm",
         "JVM 详细运行时信息（类加载/编译器/GC/线程/系统属性概览）。args: {}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Jvm),
        ("arthas_memory",
         "JVM 内存使用：各分代/元空间/堆外。args: {}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Memory),
        ("arthas_sysenv",
         "查看目标 JVM 进程环境变量。args: {variable?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Sysenv),
        ("arthas_perfcounter",
         "JVM Perf Counter 性能计数器信息。args: {}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Perfcounter),
        ("arthas_sc",
         "搜索 JVM 已加载类，可看类详情（类加载器/父类/接口/字段）。args: {classPattern, details?, fields?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Sc),
        ("arthas_sm",
         "搜索已加载类的方法信息（签名/参数/注解）。args: {classPattern, methodPattern?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Sm),
        ("arthas_jad",
         "反编译指定已加载类（JVM 实际运行的字节码 → Java 源码）。args: {classPattern, methodName?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Jad),
        ("arthas_classloader",
         "ClassLoader 诊断：统计/继承树/加载的 URL。args: {}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Classloader),
        ("arthas_getstatic",
         "查看类的静态字段值。args: {className, field?, classloader?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Getstatic),
        ("arthas_mbean",
         "查看/监控 MBean 属性信息。args: {name, attribute?, interval?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Mbean),
        ("arthas_dump",
         "导出指定类（已加载字节码）到目标机 arthas-output 目录，配合 arthas_viewfile/文件传输查看。args: {classPattern}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Dump),
        ("arthas_thread",
         "线程信息与堆栈：定位 BLOCKED/死锁/最忙线程。不支持 interrupt 子操作。args: {id?, state?, topN?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Thread),
        ("arthas_viewfile",
         "查看目标机 arthas-output 目录内文件（profiler 火焰图等）。args: {file, cursor?, offset?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Viewfile),
        ("arthas_options",
         "查看 arthas 全局开关选项。args: {option?, value?}",
         RiskLevel::ReadOnly, FAST, ArthasToolKind::Options),
        ("arthas_watch",
         "观察方法执行的入参/返回值/异常（实时，字节码增强）。args: {classPattern, methodPattern, express?, condition?}",
         RiskLevel::Low, STREAM, ArthasToolKind::Watch),
        ("arthas_trace",
         "追踪方法内部调用链与各级耗时，定位慢调用。args: {classPattern, methodPattern, condition?}",
         RiskLevel::Low, STREAM, ArthasToolKind::Trace),
        ("arthas_stack",
         "输出方法被调用的调用路径（谁调用了它）。args: {classPattern, methodPattern}",
         RiskLevel::Low, STREAM, ArthasToolKind::Stack),
        ("arthas_monitor",
         "监控方法调用统计：次数/成功率/平均 RT（周期采样）。args: {classPattern, methodPattern, interval?}",
         RiskLevel::Low, STREAM, ArthasToolKind::Monitor),
        ("arthas_tt",
         "方法执行数据时空隧道：记录每次调用的入参/返回，可事后查看/重放。args: {classPattern, methodPattern, ...}",
         RiskLevel::Low, STREAM, ArthasToolKind::Tt),
        ("arthas_ognl",
         "执行 OGNL 表达式（可调用方法/读写字段，能力很强，需确认）。args: {express, classloader?}",
         RiskLevel::Low, FAST, ArthasToolKind::Ognl),
        ("arthas_vmtool",
         "VM 工具集：forceGc（强制 GC）/ getInstances（获取类实例）。不支持 interrupt。args: {action, className?, limit?}",
         RiskLevel::Low, FAST, ArthasToolKind::Vmtool),
        ("arthas_sysprop",
         "查看/修改目标 JVM 系统属性（可写，需确认）。args: {name?, value?}",
         RiskLevel::Low, FAST, ArthasToolKind::Sysprop),
        ("arthas_vmoption",
         "查看/更新目标 JVM VM 选项（可写，需确认）。args: {name?, value?}",
         RiskLevel::Low, FAST, ArthasToolKind::Vmoption),
        ("arthas_profiler",
         "async-profiler 采样：CPU/alloc/lock，输出火焰图（到 arthas-output 目录，用 arthas_viewfile 或文件传输查看）。采样周期长，注意 timeout_secs。args: {action, event?, duration?}",
         RiskLevel::Low, PROFILER, ArthasToolKind::Profiler),
    ];
    for (name, desc, risk, timeouts, kind) in defs {
        registry.register(arthas_tool_def(name, desc, risk, timeouts, kind, manager.clone(), db.clone(), artifacts_dir.clone()));
    }
}

fn arthas_tool_def(
    name: &str,
    description: &str,
    risk: RiskLevel,
    timeouts: Timeouts,
    kind: ArthasToolKind,
    manager: Arc<ArthasManager>,
    db: sqlx::SqlitePool,
    artifacts_dir: PathBuf,
) -> ToolDef {
    let mut props = serde_json::json!({
        "environment": { "type": "string", "description": "目标环境名（来自 list_environments）" },
        "pid": { "type": "string", "description": "目标 JVM 进程号（来自 list_processes）" },
        "pod": { "type": "string", "description": "Kubernetes Pod 名（容器环境必填；虚机环境不支持；全小写，须为 k8s_find_pods 返回的准确名，勿用服务名）" },
        "namespace": { "type": "string", "description": "Kubernetes namespace（容器环境必填；与 pod 一起来自 k8s_find_pods 返回；全小写）" },
        "container": { "type": "string", "description": "容器名（多容器 Pod 时指定；缺省用 Pod 默认容器）" },
        "timeout_secs": { "type": "integer", "description": format!("超时秒数，默认 {}，最大 {}", timeouts.0, timeouts.1) },
    });
    if !matches!(kind, ArthasToolKind::Open | ArthasToolKind::Close) {
        props["args"] = serde_json::json!({
            "type": "object",
            "description": "arthas 命令参数对象（字段与 arthas 命令选项一致，原样透传给 arthas）"
        });
    }
    if matches!(kind, ArthasToolKind::Open) {
        props["java_bin"] = serde_json::json!({
            "type": "string",
            "description": "目标机 java 可执行文件路径（默认 java；目标机 PATH 无 java 时需指定）"
        });
    }
    // close + 25 个代理工具：会话查找 key 含 pod，容器环境须带与 arthas_open 相同的 pod/namespace 参数
    let description = if matches!(kind, ArthasToolKind::Open) {
        description.to_string()
    } else {
        format!("{description}容器环境需带与 arthas_open 相同的 pod/namespace 参数。")
    };
    ToolDef {
        name: name.to_string(),
        description: description.to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": props,
            "required": ["environment", "pid"],
        }),
        risk_level: risk,
        category: ToolCategory::Arthas,
        needs_channel: false,
        handler: Arc::new(ArthasToolHandler {
            manager,
            db,
            artifacts_dir,
            kind,
            timeouts,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arthas::manager::{
        ArthasClient, ArthasConfig, ArthasStopHandle, AttachRequest, AttachedSession, CallOutcome,
        AttachFactory,
    };
    use crate::tools::category::ToolCategory;

    struct MockClient;

    #[async_trait]
    impl ArthasClient for MockClient {
        async fn call_tool(&self, _name: &str, _args: &serde_json::Value) -> Result<CallOutcome, String> {
            Ok(CallOutcome { text: "ok".to_string(), is_error: false })
        }
        async fn shutdown(&self) {}
    }

    struct MockStop;

    #[async_trait]
    impl ArthasStopHandle for MockStop {
        async fn stop(&self) {}
    }

    fn ok_session() -> AttachedSession {
        AttachedSession {
            client: Arc::new(MockClient),
            stop_handle: Arc::new(MockStop),
            remote_port: 18563,
        }
    }

    /// 建库 + 单环境（transport = vm | container）
    async fn db_with_env(transport: &str) -> (tempfile::TempDir, sqlx::SqlitePool) {
        let tmp = tempfile::tempdir().unwrap();
        let db = crate::infra::db::init(tmp.path().join("friday.db")).await.unwrap();
        crate::app::env_save::save_environment_with_transport(
            &db, None, "prod", "10.0.0.1", 22, transport,
            vec![crate::app::env_save::CredentialInput {
                id: None,
                username: "root".to_string(),
                auth_type: "password".to_string(),
                private_key_path: None,
                secret: None,
                is_default: true,
            }],
        )
        .await
        .unwrap();
        (tmp, db)
    }

    /// 记录型工厂：捕获 AttachRequest 并返回就绪会话
    fn recording_factory(
        captured: Arc<std::sync::Mutex<Vec<AttachRequest>>>,
    ) -> AttachFactory {
        Arc::new(move |req| {
            let captured = captured.clone();
            Box::pin(async move {
                captured.lock().unwrap().push(req);
                Ok(ok_session())
            })
        })
    }

    fn handler(
        manager: Arc<ArthasManager>,
        db: sqlx::SqlitePool,
        kind: ArthasToolKind,
    ) -> ArthasToolHandler {
        ArthasToolHandler {
            manager,
            db,
            artifacts_dir: std::path::PathBuf::from("/tmp/x"),
            kind,
            timeouts: FAST,
        }
    }

    fn ctx() -> ToolContext {
        ToolContext { session_id: "s1".into(), channel: None }
    }

    #[tokio::test]
    async fn test_arthas_tool_def_metadata() {
        // dummy attach factory：metadata 测试不触发 attach，构造一个必然失败的闭包即可
        let factory: AttachFactory =
            Arc::new(|_req| Box::pin(async { Err(ManagerError::Attach("dummy".to_string())) }));
        let manager = Arc::new(ArthasManager::new(factory, ArthasConfig::default()));
        let def = arthas_tool_def(
            "arthas_open",
            "test",
            RiskLevel::Low,
            OPEN,
            ArthasToolKind::Open,
            manager,
            sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            std::path::PathBuf::from("/tmp/x"),
        );
        assert_eq!(def.name, "arthas_open");
        assert_eq!(def.category, ToolCategory::Arthas);
        assert_eq!(def.risk_level, RiskLevel::Low);
        assert!(!def.needs_channel);
    }

    #[tokio::test]
    async fn test_open_container_env_passes_pod_to_manager() {
        // 门禁撤除后：容器环境 + pod+ns 的 arthas_open 走到 manager（attach 收到 pod/container）
        let (_tmp, db) = db_with_env("container").await;
        let captured: Arc<std::sync::Mutex<Vec<AttachRequest>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let manager = Arc::new(ArthasManager::new(
            recording_factory(captured.clone()),
            ArthasConfig::default(),
        ));
        let out = handler(manager, db, ArthasToolKind::Open)
            .execute(
                serde_json::json!({
                    "environment": "prod", "pid": "1234",
                    "pod": "oom-service-7d9b-x2vkl", "namespace": "ns1", "container": "main"
                }),
                &ctx(),
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        let reqs = captured.lock().unwrap();
        assert_eq!(reqs.len(), 1, "attach factory must be called exactly once");
        assert_eq!(reqs[0].pod.as_deref(), Some("oom-service-7d9b-x2vkl"));
        assert_eq!(reqs[0].container.as_deref(), Some("main"));
        assert_eq!(reqs[0].pid, 1234);
    }

    #[tokio::test]
    async fn test_open_vm_env_normalizes_empty_pod() {
        // 虚机环境不带 pod（空串归一为 None）正常走 manager
        let (_tmp, db) = db_with_env("vm").await;
        let captured: Arc<std::sync::Mutex<Vec<AttachRequest>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let manager = Arc::new(ArthasManager::new(
            recording_factory(captured.clone()),
            ArthasConfig::default(),
        ));
        let out = handler(manager, db, ArthasToolKind::Open)
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "pod": "", "container": ""}),
                &ctx(),
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        let reqs = captured.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].pod, None, "empty string must normalize to None");
        assert_eq!(reqs[0].container, None);
    }

    #[tokio::test]
    async fn test_container_env_missing_pod_rejected() {
        // 容器环境缺 pod → environment_type_mismatch（不触发 attach）
        let (_tmp, db) = db_with_env("container").await;
        let factory: AttachFactory =
            Arc::new(|_req| Box::pin(async { Err(ManagerError::Attach("must not reach attach".to_string())) }));
        let manager = Arc::new(ArthasManager::new(factory, ArthasConfig::default()));
        let out = handler(manager, db, ArthasToolKind::Open)
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234"}),
                &ctx(),
            )
            .await;
        assert!(!out.success, "out: {}", out.data);
        assert_eq!(out.data["error"], "environment_type_mismatch");
        assert!(
            out.data["message"].as_str().unwrap().contains("k8s_find_pods"),
            "message must guide to k8s_find_pods: {}",
            out.data["message"]
        );
    }

    #[tokio::test]
    async fn test_vm_env_with_pod_rejected() {
        // 虚机环境带 pod → environment_type_mismatch（不触发 attach）
        let (_tmp, db) = db_with_env("vm").await;
        let factory: AttachFactory =
            Arc::new(|_req| Box::pin(async { Err(ManagerError::Attach("must not reach attach".to_string())) }));
        let manager = Arc::new(ArthasManager::new(factory, ArthasConfig::default()));
        let out = handler(manager, db, ArthasToolKind::Open)
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "pod": "some-pod"}),
                &ctx(),
            )
            .await;
        assert!(!out.success, "out: {}", out.data);
        assert_eq!(out.data["error"], "environment_type_mismatch");
    }

    #[tokio::test]
    async fn test_proxy_pod_participates_in_session_key() {
        // 容器环境：open 建会话（pod-a）→ 同 pod 代理工具命中会话；不同 pod 报 not_open
        let (_tmp, db) = db_with_env("container").await;
        let manager = Arc::new(ArthasManager::new(
            Arc::new(|_req| Box::pin(async { Ok(ok_session()) })),
            ArthasConfig::default(),
        ));

        let out = handler(manager.clone(), db.clone(), ArthasToolKind::Open)
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "pod": "pod-a", "namespace": "ns1"}),
                &ctx(),
            )
            .await;
        assert!(out.success, "open: {}", out.data);

        let dash = handler(manager, db, ArthasToolKind::Dashboard);
        let out = dash
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "pod": "pod-a", "namespace": "ns1"}),
                &ctx(),
            )
            .await;
        assert!(out.success, "same-pod query: {}", out.data);
        assert_eq!(out.data["output"], "ok");

        let out = dash
            .execute(
                serde_json::json!({"environment": "prod", "pid": "1234", "pod": "pod-b", "namespace": "ns1"}),
                &ctx(),
            )
            .await;
        assert!(!out.success, "different pod must not hit pod-a session");
        assert_eq!(out.data["error"], "arthas_not_open");
    }

    #[tokio::test]
    async fn test_all_tool_schemas_have_pod_and_container() {
        // 工厂集中生成 schema：27 个工具全部带 pod/namespace/container 参数
        let factory: AttachFactory =
            Arc::new(|_req| Box::pin(async { Err(ManagerError::Attach("dummy".to_string())) }));
        let manager = Arc::new(ArthasManager::new(factory, ArthasConfig::default()));
        let mut registry = crate::tools::registry::ToolRegistry::new();
        register_all(
            &mut registry,
            manager,
            sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            std::path::PathBuf::from("/tmp/x"),
        );
        let defs = registry.list();
        assert_eq!(defs.len(), 27, "arthas tool count");
        for def in defs {
            let props = &def.input_schema["properties"];
            assert!(props["pod"]["type"] == "string", "{} missing pod", def.name);
            assert!(props["namespace"]["type"] == "string", "{} missing namespace", def.name);
            assert!(props["container"]["type"] == "string", "{} missing container", def.name);
            assert!(
                props["pod"]["description"].as_str().unwrap().contains("k8s_find_pods"),
                "{} pod description must guide to k8s_find_pods",
                def.name
            );
            if def.name == "arthas_open" {
                assert!(def.description.contains("k8s_find_pods"), "open desc must cover container flow");
            } else {
                assert!(
                    def.description.contains("相同的 pod/namespace 参数"),
                    "{} desc must mention same-pod requirement",
                    def.name
                );
            }
        }
    }
}
