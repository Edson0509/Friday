//! K8s 服务发现：宿主机 kubectl get pods → Pod 列表（pattern 过滤）。
//! 判定模型（spec）：不猜环境类型——服务在哪，通道走哪。
//! kubectl 不存在的环境明确报错，Agent 自然切换 list_processes 路径。

use crate::tools::builtin::jvm::core::{clamp_or, error_output, resolve_environment, JvmExecCore};
use crate::tools::category::ToolCategory;
use crate::tools::registry::{ToolContext, ToolDef, ToolHandler, ToolOutput};
use crate::tools::risk::RiskLevel;
use async_trait::async_trait;
use serde::Serialize;
use std::sync::Arc;

const DEFAULT_TIMEOUT_SECS: u64 = 30;
const MAX_TIMEOUT_SECS: u64 = 120;

/// custom-columns 输出格式稳定（列名固定 5 列，CONTAINERS 逗号分隔）
pub const KUBECTL_GET_PODS: &str = "kubectl get pods -A -o custom-columns=NAME:.metadata.name,NS:.metadata.namespace,STATUS:.status.phase,CONTAINERS:.spec.containers[*].name,NODE:.spec.nodeName";

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PodInfo {
    pub name: String,
    pub namespace: String,
    pub status: String,
    pub containers: Vec<String>,
    pub node: String,
}

/// 解析 custom-columns 输出：跳过表头；5 列空白切分；异常行（列数≠5）跳过，宁缺勿错
pub fn parse_pods_output(stdout: &str) -> Vec<PodInfo> {
    let mut pods = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() || line == "NAME" || line.starts_with("NAME ") {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != 5 {
            continue;
        }
        pods.push(PodInfo {
            name: fields[0].to_string(),
            namespace: fields[1].to_string(),
            status: fields[2].to_string(),
            containers: fields[3]
                .split(',')
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            node: fields[4].to_string(),
        });
    }
    pods
}

/// pattern 过滤：Pod 名大小写不敏感包含匹配；None/空串 = 全部
pub fn filter_pods(pods: Vec<PodInfo>, pattern: Option<&str>) -> Vec<PodInfo> {
    match pattern.map(str::trim).filter(|p| !p.is_empty()) {
        None => pods,
        Some(p) => {
            let p = p.to_lowercase();
            pods.into_iter().filter(|pod| pod.name.to_lowercase().contains(&p)).collect()
        }
    }
}

pub struct FindPodsHandler {
    pub core: Arc<JvmExecCore>,
}

#[async_trait]
impl ToolHandler for FindPodsHandler {
    async fn execute(&self, args: serde_json::Value, ctx: &ToolContext) -> ToolOutput {
        let Some(environment) = args.get("environment").and_then(|v| v.as_str()) else {
            return error_output("invalid_params", "missing required parameter: environment");
        };
        let pattern = args.get("pattern").and_then(|v| v.as_str());
        let timeout_secs = clamp_or(
            args.get("timeout_secs").and_then(|v| v.as_i64()),
            DEFAULT_TIMEOUT_SECS,
            MAX_TIMEOUT_SECS,
        );

        // 发现走宿主机 base 通道（不传 pod）
        let (env, channel) = match resolve_environment(&self.core.db, &self.core.exec_pool, environment, None, None).await {
            Ok(Some(pair)) => pair,
            Ok(None) => {
                return error_output(
                    "environment_not_found",
                    &format!("环境「{environment}」不存在。请先调用 list_environments 查看可用环境；若无匹配，请让用户在右侧「环境」面板添加。"),
                );
            }
            Err(e) => return error_output("connection_error", &e),
        };

        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            channel.run(KUBECTL_GET_PODS),
        )
        .await;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        match result {
            Err(_) => {
                tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, timeout_secs, "k8s_find_pods timed out, dropping connection");
                {
                    let mut pool = self.core.exec_pool.lock().await;
                    pool.disconnect(&env.id).await;
                }
                error_output("timeout_error", &format!("command timed out after {timeout_secs}s"))
            }
            Ok(Err(e)) => {
                tracing::error!(session_id = %ctx.session_id, env_id = %env.id, error = %e, "k8s_find_pods exec failed");
                error_output("connection_error", &e.to_string())
            }
            Ok(Ok(output)) => {
                // kubectl 不存在 → 明确引导（Agent 切换 list_processes 路径）
                if output.exit_code == 127
                    || output.stderr.contains("command not found")
                    || output.stderr.contains("executable file not found")
                {
                    return error_output(
                        "not_k8s_environment",
                        "该环境没有 kubectl（非 Kubernetes 宿主机）。请用 list_processes 在宿主机上定位服务进程。",
                    );
                }
                if output.exit_code != 0 {
                    return error_output(
                        "kubectl_error",
                        &format!("kubectl get pods failed (exit {}): {}", output.exit_code, output.stderr),
                    );
                }
                let pods = filter_pods(parse_pods_output(&output.stdout), pattern);
                tracing::info!(session_id = %ctx.session_id, env_id = %env.id, found = pods.len(), elapsed_ms, "k8s_find_pods done");
                ToolOutput {
                    success: true,
                    data: serde_json::json!({
                        "pods": pods,
                        "count": pods.len(),
                        "note": "多实例命中时请让用户选择目标 Pod；后续 jvm_* 工具传该 Pod 名作为 pod 参数（PID 为容器内 PID，用带 pod 的 list_processes 获取）。",
                        "elapsed_ms": elapsed_ms,
                    }),
                    raw_stdout: Some(output.stdout),
                }
            }
        }
    }
}

pub fn k8s_find_pods_tool_def(core: Arc<JvmExecCore>) -> ToolDef {
    ToolDef {
        name: "k8s_find_pods".to_string(),
        description: "在 Kubernetes 宿主机上按服务名发现 Pod（kubectl get pods -A + 名称过滤），返回 Pod/命名空间/容器列表/状态/节点。诊断入口：用户说「检查 xx 服务的内存」而环境是 K8s 宿主机时，先用本工具定位 Pod；多实例时向用户确认选哪个。之后的诊断工具传 pod（+container）参数。宿主机与 Pod 内进程可分别用 list_processes（不传/传 pod）排查。".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "environment": { "type": "string", "description": "目标环境名称（list_environments 返回的 name）" },
                "pattern": { "type": "string", "description": "Pod 名过滤关键字（大小写不敏感包含匹配；缺省返回全部）" },
                "timeout_secs": { "type": "number", "description": "超时秒数，默认 30，上限 120" }
            },
            "required": ["environment"]
        }),
        risk_level: RiskLevel::ReadOnly,
        category: ToolCategory::K8s,
        needs_channel: false,
        handler: Arc::new(FindPodsHandler { core }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "NAME                    NS        STATUS    CONTAINERS      NODE\n\
                          snmpagent-7d9b-x2vkl    default   Running   app,sidecar     node-1\n\
                          oomservice-5c8d-p9qrs   oms       Running   oomservice      node-2\n\
                          bad-line                only-three-columns\n";

    #[test]
    fn test_parse_skips_header_and_malformed() {
        let pods = parse_pods_output(SAMPLE);
        assert_eq!(pods.len(), 2);
        assert_eq!(pods[0].name, "snmpagent-7d9b-x2vkl");
        assert_eq!(pods[0].namespace, "default");
        assert_eq!(pods[0].status, "Running");
        assert_eq!(pods[0].containers, vec!["app".to_string(), "sidecar".to_string()]);
        assert_eq!(pods[0].node, "node-1");
        assert_eq!(pods[1].name, "oomservice-5c8d-p9qrs");
        assert_eq!(pods[1].node, "node-2");
    }

    #[test]
    fn test_filter_case_insensitive_contains() {
        let pods = parse_pods_output(SAMPLE);
        let hit = filter_pods(pods.clone(), Some("SNMPAgent"));
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].name, "snmpagent-7d9b-x2vkl");
        // None / 空串 = 全部
        assert_eq!(filter_pods(pods.clone(), None).len(), 2);
        assert_eq!(filter_pods(pods, Some("  ")).len(), 2);
    }

    mod handler_tests {
        use super::super::*;
        use super::SAMPLE;
        use crate::exec::channel::{ExecChannel, ExecOutput};
        use crate::tools::registry::{ToolContext, ToolHandler};
        use async_trait::async_trait;

        const SAMPLE_OUTPUT: &str = SAMPLE;

        struct KubectlChannel {
            exit_code: i32,
            stderr: &'static str,
        }

        #[async_trait]
        impl ExecChannel for KubectlChannel {
            async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
                assert!(cmd.starts_with("kubectl get pods"), "must run kubectl on host: {cmd}");
                Ok(ExecOutput { stdout: SAMPLE_OUTPUT.to_string(), stderr: self.stderr.to_string(), exit_code: self.exit_code })
            }
            async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
            async fn disconnect(&self) {}
            async fn is_alive(&self) -> bool { true }
        }

        async fn setup(channel: Arc<dyn ExecChannel>) -> (tempfile::TempDir, Arc<JvmExecCore>) {
            let tmp = tempfile::tempdir().unwrap();
            let db = crate::infra::db::init(tmp.path().join("friday.db")).await.unwrap();
            let env_id = crate::app::env_save::save_environment(
                &db, None, "prod", "10.0.0.1", 22,
                vec![crate::app::env_save::CredentialInput {
                    id: None,
                    username: "root".to_string(),
                    auth_type: "password".to_string(),
                    private_key_path: None,
                    secret: None,
                    is_default: true,
                }],
            ).await.unwrap().environment.id;
            let exec_pool = Arc::new(tokio::sync::Mutex::new(crate::exec::pool::ExecChannelPool::new()));
            exec_pool.lock().await.insert_channel(env_id, channel).await;
            let artifacts = tmp.path().join("artifacts");
            std::fs::create_dir_all(&artifacts).unwrap();
            let core = Arc::new(JvmExecCore {
                db,
                exec_pool,
                jdk_cache: Arc::new(crate::tools::builtin::jvm::jdk_cache::JdkCache::new()),
                artifacts_dir: artifacts,
            });
            (tmp, core)
        }

        #[tokio::test]
        async fn test_find_pods_filters_and_structures() {
            let ch = Arc::new(KubectlChannel { exit_code: 0, stderr: "" });
            let (tmp, core) = setup(ch).await;
            let handler = FindPodsHandler { core };
            let ctx = ToolContext { session_id: "s1".into(), channel: None };
            let out = handler
                .execute(serde_json::json!({"environment": "prod", "pattern": "snmpagent"}), &ctx)
                .await;
            assert!(out.success, "out: {}", out.data);
            assert_eq!(out.data["count"], 1);
            let pod = &out.data["pods"][0];
            assert_eq!(pod["name"], "snmpagent-7d9b-x2vkl");
            assert_eq!(pod["namespace"], "default");
            assert_eq!(pod["status"], "Running");
            assert_eq!(pod["containers"], serde_json::json!(["app", "sidecar"]));
            assert_eq!(pod["node"], "node-1");
            // 原始 stdout 透传（供 artifacts/调试）
            assert!(out.raw_stdout.unwrap().contains("oomservice-5c8d-p9qrs"));
            drop(tmp);
        }

        #[tokio::test]
        async fn test_no_kubectl_reports_not_k8s_environment() {
            let ch = Arc::new(KubectlChannel { exit_code: 127, stderr: "bash: kubectl: command not found" });
            let (tmp, core) = setup(ch).await;
            let handler = FindPodsHandler { core };
            let ctx = ToolContext { session_id: "s1".into(), channel: None };
            let out = handler.execute(serde_json::json!({"environment": "prod"}), &ctx).await;
            assert!(!out.success);
            assert_eq!(out.data["error"], "not_k8s_environment");
            assert!(out.data["message"].as_str().unwrap().contains("list_processes"));
            drop(tmp);
        }

        #[tokio::test]
        async fn test_kubectl_error_passthrough() {
            let ch = Arc::new(KubectlChannel { exit_code: 1, stderr: "error: server unauthorized" });
            let (tmp, core) = setup(ch).await;
            let handler = FindPodsHandler { core };
            let ctx = ToolContext { session_id: "s1".into(), channel: None };
            let out = handler.execute(serde_json::json!({"environment": "prod"}), &ctx).await;
            assert!(!out.success);
            assert_eq!(out.data["error"], "kubectl_error");
            assert!(out.data["message"].as_str().unwrap().contains("unauthorized"));
            drop(tmp);
        }

        #[tokio::test]
        async fn test_tool_def_metadata() {
            let tmp = tempfile::tempdir().unwrap();
            let artifacts = tmp.path().join("artifacts");
            std::fs::create_dir_all(&artifacts).unwrap();
            let core = Arc::new(JvmExecCore {
                db: sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
                exec_pool: Arc::new(tokio::sync::Mutex::new(crate::exec::pool::ExecChannelPool::new())),
                jdk_cache: Arc::new(crate::tools::builtin::jvm::jdk_cache::JdkCache::new()),
                artifacts_dir: artifacts,
            });
            let def = k8s_find_pods_tool_def(core);
            assert_eq!(def.name, "k8s_find_pods");
            assert_eq!(def.risk_level, RiskLevel::ReadOnly);
            assert_eq!(def.category, ToolCategory::K8s);
            assert!(!def.needs_channel);
            assert!(def.input_schema["properties"]["pattern"].is_object());
        }
    }
}
