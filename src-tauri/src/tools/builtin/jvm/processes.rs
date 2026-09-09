use crate::tools::builtin::jvm::core::{
    clamp_or, error_output, resolve_environment, validate_target, JvmExecCore,
};
use crate::tools::category::ToolCategory;
use crate::tools::registry::{ToolContext, ToolDef, ToolHandler, ToolOutput};
use crate::tools::risk::RiskLevel;
use async_trait::async_trait;
use std::sync::Arc;

const DEFAULT_TIMEOUT_SECS: u64 = 30;
const MAX_TIMEOUT_SECS: u64 = 120;

pub struct ListProcessesHandler {
    pub core: Arc<JvmExecCore>,
}

#[async_trait]
impl ToolHandler for ListProcessesHandler {
    async fn execute(&self, args: serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        let Some(environment) = args.get("environment").and_then(|v| v.as_str()) else {
            return error_output("invalid_params", "missing required parameter: environment");
        };
        let timeout_secs = clamp_or(
            args.get("timeout_secs").and_then(|v| v.as_i64()),
            DEFAULT_TIMEOUT_SECS,
            MAX_TIMEOUT_SECS,
        );
        // keyword 可选：空串视为未传（返回全部进程）
        let keyword = args
            .get("keyword")
            .and_then(|v| v.as_str())
            .filter(|kw| !kw.is_empty());
        let pod = args.get("pod").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let namespace = args.get("namespace").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let container = args.get("container").and_then(|v| v.as_str()).filter(|s| !s.is_empty());

        let (env, channel) =
            match resolve_environment(&self.core.db, &self.core.exec_pool, environment, pod, namespace, container).await {
            Ok(Some(pair)) => pair,
            Ok(None) => {
                return error_output(
                    "environment_not_found",
                    &format!("环境「{environment}」不存在。请先调用 list_environments 查看可用环境；若无匹配，请让用户在右侧「环境」面板添加。"),
                );
            }
            Err(e) => return error_output("connection_error", &e),
        };

        // 环境类型门禁：vm 拒 pod/ns / container 必填 pod+namespace（引导 k8s_find_pods）+ k8s 名防呆
        if let Err(msg) = validate_target(&env, pod, namespace, container) {
            return error_output("environment_type_mismatch", &msg);
        }

        // keyword 插值进远端 shell 命令（注入面），必须单引号转义；无 keyword 时纯 ps 返回全部进程
        let command = match keyword {
            Some(kw) => format!(
                "ps -eo pid=,user=,args= | grep -i {} | grep -v grep",
                crate::exec::ssh::shell_quote_single(kw)
            ),
            None => "ps -eo pid=,user=,args=".to_string(),
        };
        tracing::info!(session_id = %ctx.session_id, env_id = %env.id, command = %command, "list_processes executing");

        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            channel.run(&command),
        )
        .await;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        match result {
            Err(_) => {
                tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, timeout_secs, "list_processes timed out, dropping connection");
                let target = crate::exec::pool::TargetKey::from_parts(&env.id, pod, namespace, container);
                crate::exec::pool::drop_target_and_kill(&self.core.exec_pool, &self.core.db, &target, &command).await;
                error_output("timeout_error", &format!("command timed out after {timeout_secs}s"))
            }
            Ok(Err(e)) => {
                tracing::error!(session_id = %ctx.session_id, env_id = %env.id, error = %e, "list_processes exec failed");
                error_output("connection_error", &e.to_string())
            }
            Ok(Ok(output)) => {
                // Rust 侧再过滤一次（防御远端 shell 差异）；无 keyword 时保留全部行
                let kw_lower = keyword.map(|kw| kw.to_lowercase());
                let lines: Vec<&str> = output
                    .stdout
                    .lines()
                    .filter(|l| match &kw_lower {
                        Some(kw) => l.to_lowercase().contains(kw),
                        None => true,
                    })
                    .collect();
                let processes = lines.join("\n");
                tracing::info!(session_id = %ctx.session_id, env_id = %env.id, found = lines.len(), elapsed_ms, "list_processes done");
                ToolOutput {
                    success: true,
                    data: serde_json::json!({
                        "command": command,
                        "processes": processes,
                        "count": lines.len(),
                        "note": "每行格式: PID USER 命令行。从命令行中识别目标服务并取 PID。",
                        "exit_code": output.exit_code,
                        "elapsed_ms": elapsed_ms,
                    }),
                    raw_stdout: Some(output.stdout),
                }
            }
        }
    }
}

pub fn list_processes_tool_def(core: Arc<JvmExecCore>) -> ToolDef {
    ToolDef {
        name: "list_processes".to_string(),
        description: "列出目标环境上的进程（PID、用户、完整命令行），按 keyword（服务名/关键字，大小写不敏感）过滤。虚机环境：诊断第一步，用服务名作 keyword 查 PID，再配合 jvm_* 等工具。容器环境：先用 k8s_find_pods 定位 Pod，再带 pod + namespace 列出容器内进程（PID 为容器内 PID，后续 jvm_* 工具需带相同 pod + namespace）。不依赖 JDK 装备。".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "environment": { "type": "string", "description": "目标环境名称（list_environments 返回的 name）" },
                "keyword": { "type": "string", "description": "过滤关键字（服务名等，大小写不敏感；缺省返回全部进程）" },
                "timeout_secs": { "type": "number", "description": "超时秒数，默认 30，上限 120" },
                "pod": { "type": "string", "description": "Kubernetes Pod 名（容器环境必填；虚机环境不支持；全小写，须为 k8s_find_pods 返回的准确名，勿用服务名）" },
                "namespace": { "type": "string", "description": "Kubernetes namespace（容器环境必填；与 pod 一起来自 k8s_find_pods 返回；全小写）" },
                "container": { "type": "string", "description": "容器名（多容器 Pod 时指定；缺省用 Pod 默认容器）" }
            },
            "required": ["environment"]
        }),
        risk_level: RiskLevel::ReadOnly,
        category: ToolCategory::Environment,
        needs_channel: false,
        handler: Arc::new(ListProcessesHandler { core }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::channel::{ExecChannel, ExecOutput};
    use async_trait::async_trait;

    struct PsChannel {
        stdout: &'static str,
        calls: tokio::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ExecChannel for PsChannel {
        async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            self.calls.lock().await.push(cmd.to_string());
            Ok(ExecOutput { stdout: self.stdout.to_string(), stderr: String::new(), exit_code: 0 })
        }
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
        async fn disconnect(&self) {}
        async fn is_alive(&self) -> bool { true }
    }

    async fn setup_as(channel: Arc<dyn ExecChannel>, transport: &str) -> (tempfile::TempDir, Arc<JvmExecCore>) {
        let (tmp, core, _env_id) =
            crate::tools::builtin::jvm::core::test_support::setup_env_with_channel(transport, channel).await;
        (tmp, core)
    }

    async fn setup(channel: Arc<dyn ExecChannel>) -> (tempfile::TempDir, Arc<JvmExecCore>) {
        setup_as(channel, "vm").await
    }

    const PS_OUTPUT: &str = "  1234 root /opt/jdk/bin/java -Xmx4g -jar oomservice.jar\n  5678 root /usr/bin/python3 script.py\n  9999 app nginx: worker process\n";

    #[tokio::test]
    async fn test_keyword_filters_and_quotes_command() {
        let ch = Arc::new(PsChannel { stdout: PS_OUTPUT, calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch.clone()).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler
            .execute(serde_json::json!({"environment": "prod", "keyword": "OOMService"}), &ctx)
            .await;
        assert!(out.success);
        // Rust 侧大小写不敏感过滤
        assert_eq!(out.data["count"], 1);
        assert!(out.data["processes"].as_str().unwrap().contains("1234"));
        assert!(!out.data["processes"].as_str().unwrap().contains("python"));
        // 命令构造：keyword 单引号包裹（注入面）
        let calls = ch.calls.lock().await;
        assert!(calls[0].contains("grep -i 'OOMService'"), "cmd: {}", calls[0]);
        assert!(calls[0].contains("grep -v grep"));
        drop(tmp);
    }

    #[tokio::test]
    async fn test_keyword_injection_quoted() {
        let ch = Arc::new(PsChannel { stdout: "", calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch.clone()).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let _ = handler
            .execute(serde_json::json!({"environment": "prod", "keyword": "x'; rm -rf /; echo '"}), &ctx)
            .await;
        let calls = ch.calls.lock().await;
        // 注入内容被单引号转义，命令仍是单条 grep
        assert!(calls[0].contains(r"grep -i 'x'\''; rm -rf /; echo '\''"), "cmd: {}", calls[0]);
        drop(tmp);
    }

    #[tokio::test]
    async fn test_no_keyword_returns_all_processes() {
        let ch = Arc::new(PsChannel { stdout: PS_OUTPUT, calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch.clone()).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler.execute(serde_json::json!({"environment": "prod"}), &ctx).await;
        assert!(out.success);
        assert_eq!(out.data["count"], 3);
        // 无 keyword：纯 ps，无 grep 管道
        let calls = ch.calls.lock().await;
        assert_eq!(calls[0], "ps -eo pid=,user=,args=");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_empty_keyword_returns_all_processes() {
        let ch = Arc::new(PsChannel { stdout: PS_OUTPUT, calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch.clone()).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler.execute(serde_json::json!({"environment": "prod", "keyword": ""}), &ctx).await;
        assert!(out.success);
        // 空串视为未传：返回全部进程，纯 ps 命令
        assert_eq!(out.data["count"], 3);
        let calls = ch.calls.lock().await;
        assert_eq!(calls[0], "ps -eo pid=,user=,args=");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_no_match_returns_empty() {
        let ch = Arc::new(PsChannel { stdout: PS_OUTPUT, calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler
            .execute(serde_json::json!({"environment": "prod", "keyword": "nonexistent-svc"}), &ctx)
            .await;
        assert!(out.success);
        assert_eq!(out.data["count"], 0);
        assert_eq!(out.data["processes"], "");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_missing_environment_param() {
        let ch = Arc::new(PsChannel { stdout: PS_OUTPUT, calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler.execute(serde_json::json!({}), &ctx).await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "invalid_params");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_unknown_environment_guides_agent() {
        let ch = Arc::new(PsChannel { stdout: PS_OUTPUT, calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler.execute(serde_json::json!({"environment": "nope"}), &ctx).await;
        assert!(!out.success);
        assert_eq!(out.data["error"], "environment_not_found");
        assert!(out.data["message"].as_str().unwrap().contains("list_environments"));
        drop(tmp);
    }

    #[tokio::test]
    async fn test_vm_env_rejects_pod_param() {
        // 虚机环境 + pod 参数 → environment_type_mismatch（类型门禁）
        let ch = Arc::new(PsChannel { stdout: PS_OUTPUT, calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch.clone()).await;
        let env_id = crate::app::environments::find_by_name(&core.db, "prod").await.unwrap().unwrap().id;
        // 预注入 k8s 目标通道：让 resolve 成功，证明拦截来自门禁而非连接层
        core.exec_pool.lock().await.insert_channel(
            crate::exec::pool::TargetKey::k8s(&env_id, "pod-1", None, None),
            ch,
        ).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler
            .execute(serde_json::json!({"environment": "prod", "pod": "pod-1"}), &ctx)
            .await;
        assert!(!out.success, "out: {}", out.data);
        assert_eq!(out.data["error"], "environment_type_mismatch");
        drop(tmp);
    }

    #[tokio::test]
    async fn test_pod_param_routes_through_k8s_channel() {
        // pod+namespace 参数贯通：注入 TargetKey::k8s 的 K8sChannel（包记录型 base），
        // ps 命令必须经 kubectl exec -n 'ns1' 包装进容器（而非走宿主机 base key）
        let ch = Arc::new(PsChannel { stdout: PS_OUTPUT, calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup_as(ch.clone(), "container").await;
        let env_id = crate::app::environments::find_by_name(&core.db, "prod").await.unwrap().unwrap().id;
        core.exec_pool.lock().await.insert_channel(
            crate::exec::pool::TargetKey::k8s(&env_id, "pod-1", Some("ns1"), None),
            Arc::new(crate::exec::k8s::K8sChannel {
                base: ch.clone(),
                pod: "pod-1".to_string(),
                namespace: Some("ns1".to_string()),
                container: None,
            }),
        ).await;
        let handler = ListProcessesHandler { core };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler
            .execute(
                serde_json::json!({"environment": "prod", "pod": "pod-1", "namespace": "ns1"}),
                &ctx,
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        let calls = ch.calls.lock().await;
        assert!(calls[0].contains("kubectl exec"), "ps must be wrapped for k8s target: {}", calls[0]);
        assert!(calls[0].contains("-n 'ns1'"), "ns must pass through to kubectl: {}", calls[0]);
        assert!(calls[0].contains("ps -eo pid=,user=,args="), "cmd: {}", calls[0]);
        drop(tmp);
    }

    #[tokio::test]
    async fn test_tool_def_metadata() {
        let ch = Arc::new(PsChannel { stdout: "", calls: tokio::sync::Mutex::new(Vec::new()) });
        let (tmp, core) = setup(ch).await;
        let def = list_processes_tool_def(core);
        assert_eq!(def.name, "list_processes");
        assert_eq!(def.risk_level, RiskLevel::ReadOnly);
        assert_eq!(def.category, ToolCategory::Environment);
        assert!(!def.needs_channel);
        assert!(def.input_schema["properties"]["keyword"].is_object());
        drop(tmp);
    }
}
