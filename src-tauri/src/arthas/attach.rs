use crate::app::events::{AppEvent, EventBus};
use crate::exec::channel::ExecChannel;
use crate::exec::pool::ExecChannelPool;
use crate::exec::ssh::shell_quote_single;
use crate::provision::package::ToolPackage;
use async_trait::async_trait;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::manager::{
    ActivePortsFn, ArthasClient, ArthasStopHandle, AttachFactory, AttachRequest, AttachedSession, ManagerError,
};
use super::tunnel::{establish_pf_tunnel, teardown_pf, ArthasTunnels, PfLease, PfTunnelParams};

/// 远端 arthas HTTP 端口分配起点（顺序向上探测）
pub const ARTHAS_PORT_START: u16 = 18563;
pub const ARTHAS_PORT_CANDIDATES: u16 = 10;

/// arthas.properties 内容。overrideAll=true 使 properties 覆盖 CLI（agent 侧 telnetPort=-1
/// 真正禁用 telnet，CLI 传的 telnet 端口仅用于骗过 boot 的预检）；localConnectionNonAuth +
/// ip=127.0.0.1 为官方包默认（stop 走 /api 本地免密、只绑回环），此前覆盖时丢失。
/// 内容不含单引号/美元符，可安全嵌入 shell 单引号（见测试）。
pub fn arthas_properties_content(http_port: u16, token: &str) -> String {
    arthas_properties_content_with_bind(http_port, token, "127.0.0.1")
}

/// 容器分支 properties：额外把绑定地址换成 0.0.0.0——宿主机侧探活（/dev/tcp 打
/// podIP）与 T6 正向隧道都从 Pod 网络进入，127.0.0.1 绑定不可达；MCP 桥的 curl
/// 127.0.0.1 在容器内本地执行，回环仍通。VM 模式维持官方默认 127.0.0.1 不变。
pub fn arthas_properties_content_pod(http_port: u16, token: &str) -> String {
    arthas_properties_content_with_bind(http_port, token, "0.0.0.0")
}

fn arthas_properties_content_with_bind(http_port: u16, token: &str, bind_ip: &str) -> String {
    format!(
        "arthas.config.overrideAll=true\narthas.mcpEndpoint=/mcp\narthas.telnetPort=-1\narthas.httpPort={http_port}\narthas.password={token}\narthas.localConnectionNonAuth=true\narthas.ip={bind_ip}\n"
    )
}

/// 用户对齐 pre-flight：目标进程属主 + 当前 SSH 用户
pub fn check_user_command(pid: i64) -> String {
    format!("ps -o user= -p {pid} 2>/dev/null; echo '---'; id -un")
}

/// 解析 check_user_command 输出 → (jvm_user, ssh_user)。
/// jvm_user 为空 = 进程不存在（或已被回收）。
pub fn parse_user_check(stdout: &str) -> Result<(String, String), String> {
    let mut parts = stdout.splitn(2, "---");
    let jvm_raw = parts.next().unwrap_or_default();
    let ssh_raw = parts.next().unwrap_or_default();
    let jvm_user = jvm_raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .last()
        .unwrap_or_default();
    let ssh_user = ssh_raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .last()
        .unwrap_or_default();
    if jvm_user.is_empty() {
        return Err("目标进程不存在或已退出（ps 无属主输出）".to_string());
    }
    if ssh_user.is_empty() {
        return Err("无法确定当前 SSH 用户（id -un 无输出）".to_string());
    }
    Ok((jvm_user.to_string(), ssh_user.to_string()))
}

/// 目标机端口占用探测（bash /dev/tcp）：busy = 可连（占用）；free = 连不上
pub fn port_probe_command(port: u16) -> String {
    format!(
        "if (exec 3<>/dev/tcp/127.0.0.1/{port}) 2>/dev/null; then exec 3>&- 3<&-; echo busy; else echo free; fi"
    )
}

/// 宿主机侧查询 Pod IP（k8s 探活用）。kubectl 不带 -n——依赖 kubeconfig context 的
/// 当前 namespace（见 attach_arthas_in_pod 的已知假设注释）。
pub fn pod_ip_command(pod: &str) -> String {
    format!(
        "kubectl get pod {} -o jsonpath='{{.status.podIP}}'",
        shell_quote_single(pod)
    )
}

/// 宿主机侧 Pod 端口探测（bash /dev/tcp 打 podIP）：busy = 可连。timeout 1 兜底
/// 网络黑洞（无 timeout 时连接挂起会拖到内核 TCP 超时，拖爆探活循环）。
/// pod_ip 必须先过 validate_pod_ip（防注入）。
pub fn pod_ip_probe_command(pod_ip: &str, port: u16) -> String {
    format!(
        "timeout 1 bash -c 'exec 3<>/dev/tcp/{pod_ip}/{port}' 2>/dev/null && echo busy || echo free"
    )
}

/// kubectl 输出的 PodIP 校验：只接受 IPv4/IPv6 字面量（防命令注入）。
pub fn validate_pod_ip(ip: &str) -> Result<(), String> {
    ip.parse::<std::net::IpAddr>()
        .map(|_| ())
        .map_err(|_| format!("kubectl 返回的 PodIP 不是合法 IP: {ip:?}"))
}

/// 从 start 起找两个空闲端口（http + telnet 预检用）：探测候选 count 个，输出两行。
/// VM 分支用（目标机本地 bash /dev/tcp 探 127.0.0.1）；pod 分支用
/// find_free_port_pod_command（宿主机侧探 podIP——容器 sh 无 /dev/tcp）。
/// 遍历全部候选输出空闲端口再 `head -2`（管道下循环 SIGPIPE 提前退出，行为正确）；
/// 末尾 `; true` 保证整体 exit 0（探活命令的退出码不被 head 影响）。
pub fn find_free_port_command(start: u16, count: u16) -> String {
    let end = start + count - 1;
    format!(
        "for p in $(seq {start} {end}); do \
         if (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null; then exec 3>&- 3<&-; else echo $p; fi; \
         done | head -2; true"
    )
}

/// 宿主机侧 Pod 端口段分配（pod 模式）：bash /dev/tcp 打 podIP 逐候选探测，
/// 连不上（含 timeout 1 网络黑洞兜底）= 空闲 → 输出端口。容器 sh（busybox）
/// 无 /dev/tcp，容器内探测恒 free（同 Pod 并发会话恒选 18563 撞 already-bind），
/// 故 pod 模式探测必须经 base 通道在宿主机执行。遍历全部候选输出空闲端口再
/// `head -2`（管道下循环 SIGPIPE 提前退出）；末尾 `; true` 保证整体 exit 0。
/// `$p` 用双引号由宿主机 shell 展开后传入内层 bash——单引号内变量不透传，
/// 空展开会让所有端口恒报空闲。pod_ip 必须先过 validate_pod_ip（防注入）。
pub fn find_free_port_pod_command(pod_ip: &str, start: u16, count: u16) -> String {
    let end = start + count - 1;
    format!(
        "for p in $(seq {start} {end}); do \
         timeout 1 bash -c \"exec 3<>/dev/tcp/{pod_ip}/$p\" 2>/dev/null || echo $p; \
         done | head -2; true"
    )
}

/// 解析 find_free_port_command 输出 → (http_port, telnet_det_port)
pub fn parse_free_port(stdout: &str) -> Result<(u16, u16), String> {
    let ports: Vec<u16> = stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .filter_map(|l| l.parse::<u16>().ok())
        .collect();
    if ports.len() >= 2 {
        return Ok((ports[0], ports[1]));
    }
    if ports.len() == 1 {
        return Err(format!(
            "端口 {ARTHAS_PORT_START}~{} 中只有 1 个空闲（需要 2 个：HTTP + telnet 预检），请稍后重试",
            ARTHAS_PORT_START + ARTHAS_PORT_CANDIDATES - 1
        ));
    }
    Err(format!(
        "端口 {ARTHAS_PORT_START}~{} 均被占用，请减少同机并发 attach 的 JVM 数或稍后重试",
        ARTHAS_PORT_START + ARTHAS_PORT_CANDIDATES - 1
    ))
}

/// 写 arthas.properties（内容经单引号转义；chmod 644 保证 jvm_user 可读）
pub fn write_properties_command(home: &str, content: &str) -> String {
    format!(
        "printf '%s' {} > {home}/arthas.properties && chmod 644 {home}/arthas.properties",
        shell_quote_single(content)
    )
}

/// attach 命令：pid 是位置参数（arthas-boot 无 --pid 选项）；--attach-only 使 boot 进程
/// attach 后即退出（telnet 已禁用，不启动交互 client）；--telnet-port 传有效空闲端口
/// 骗过 boot 的预检（对 -1 会抛 port out of range 退出），agent 侧实际不绑（overrideAll）。
/// nohup 后台驻留，stdin 接 /dev/null 防交互等待。java 为可执行文件完整路径（已做字符集校验）。
/// 日志重定向到 /tmp（跨用户 attach 时 home 目录对 jvm_user 不可写，/tmp 才能保证可写）。
pub fn attach_command(java: &str, home: &str, http_port: u16, telnet_det_port: u16, pid: i64) -> String {
    format!(
        "cd {home} && nohup {java} -jar arthas-boot.jar --attach-only --http-port {http_port} --telnet-port {telnet_det_port} {pid} < /dev/null >> /tmp/arthas-friday-{pid}.log 2>&1 & echo attach-started"
    )
}

/// HTTP stop（best-effort）：arthas HTTP API 执行 stop 命令，卸载 agent。
/// curl 缺失时 wget 兜底，再失败吞掉（stop 尽力而为）。
pub fn stop_command(port: u16, token: &str) -> String {
    format!(
        "curl -s -m 10 -X POST -H 'Authorization: Bearer {token}' -H 'Content-Type: application/json' \
         -d '{{\"action\":\"exec\",\"command\":\"stop\"}}' http://127.0.0.1:{port}/api \
         || wget -q -O /dev/null --header='Authorization: Bearer {token}' \
         --post-data='{{\"action\":\"exec\",\"command\":\"stop\"}}' http://127.0.0.1:{port}/api \
         || true"
    )
}

/// 生成 Bearer token（32 位十六进制，无 shell 特殊字符）
pub fn generate_token() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

// ─────────────────────────── 生产编排 ───────────────────────────

/// 生产 attach 依赖集
#[derive(Clone)]
pub struct AttachDeps {
    pub db: sqlx::SqlitePool,
    pub exec_pool: Arc<Mutex<ExecChannelPool>>,
    pub jdk_cache: Arc<crate::tools::builtin::jvm::jdk_cache::JdkCache>,
    pub cache_dir: PathBuf,
    pub arthas_zip: Option<PathBuf>,
    pub bus: EventBus,
    /// 活跃会话端口查询（manager 共享；残留清理排除活跃会话）
    pub active_ports_fn: ActivePortsFn,
    /// SSH 隧道管理器（T6 pf 隧道主路径；测试注入 mock）
    pub tunnels: Arc<dyn ArthasTunnels>,
}

pub fn production_attach_factory(deps: AttachDeps) -> AttachFactory {
    Arc::new(move |req| {
        let deps = deps.clone();
        Box::pin(attach_arthas(deps, req))
    })
}

/// attach 命令执行通道。关键正确性约束：Shared 是连接池里的连接，用后**绝不能**
/// disconnect（会拆掉池连接）；Temp 是 jvm_user 临时连接，attach 完即断开。
enum AttachExecKind {
    Shared(Arc<dyn ExecChannel>),
    Temp(TempAttachTransport),
}

async fn attach_arthas(deps: AttachDeps, req: AttachRequest) -> Result<AttachedSession, ManagerError> {
    // 0. 宿主机默认连接（连接池）：VM 分支的命令执行主通道；k8s 分支的
    //    装备中转（staging unzip/tar）、podIP 查询与宿主机侧端口探测/探活通道
    let base = get_default_channel(&deps, &req.env_id).await?;
    match req.pod.clone() {
        Some(pod) => attach_arthas_in_pod(deps, req, base, &pod).await,
        None => attach_arthas_on_vm(deps, req, base).await,
    }
}

/// VM 分支（原单通道流程）：所有命令在宿主机上执行。
async fn attach_arthas_on_vm(
    deps: AttachDeps,
    req: AttachRequest,
    channel: Arc<dyn ExecChannel>,
) -> Result<AttachedSession, ManagerError> {
    let progress = |stage: &str, detail: String| {
        tracing::info!(session_id = %req.session_id, env_id = %req.env_id, stage, detail = %detail, "arthas attach progress (vm)");
        deps.bus.emit(
            &req.session_id,
            AppEvent::ProvisionProgress {
                session_id: req.session_id.clone(),
                tool: "arthas_open".to_string(),
                stage: stage.to_string(),
                detail,
            },
        );
    };

    // 1. 确保 arthas 工具包（幂等，cached 快路径）
    progress("ensure_package", "确保 arthas 工具包".to_string());
    let pctx = provision_context(&deps, &req, channel.clone(), crate::provision::jdk::REMOTE_TOOLS_DIR)
        .await?;
    let arthas_pkg = crate::provision::arthas::ArthasPackage;
    let arthas_result = arthas_pkg
        .ensure(&pctx, "java")
        .await
        .map_err(|e| ManagerError::Attach(format!("arthas 工具包下发失败: {}", e.message)))?;
    let arthas_home = arthas_result.tool_home;

    // 2. 解析 attach 用 java（JdkCache → PATH java → ensure JDK），返回可执行文件完整路径
    let java = resolve_attach_java(&deps, &req, &channel).await?;

    // 3. 用户对齐 pre-flight
    progress("check_user", "检查目标 JVM 运行用户".to_string());
    let (jvm_user, ssh_user) = check_users(channel.as_ref(), req.pid).await?;
    let attach_exec_kind = if jvm_user == ssh_user || ssh_user == "root" {
        AttachExecKind::Shared(channel.clone())
    } else {
        progress(
            "check_user",
            format!("SSH 用户 {ssh_user} ≠ JVM 用户 {jvm_user}，使用 {jvm_user} 凭证临时连接"),
        );
        match crate::app::env_credentials::find_credential_by_username(&deps.db, &req.env_id, &jvm_user).await {
            Ok(Some(cred)) => AttachExecKind::Temp(build_temp_transport(&deps, &req.env_id, &cred).await?),
            _ => {
                return Err(ManagerError::Attach(format!(
                    "目标 JVM 运行用户为 {jvm_user}，当前 SSH 用户为 {ssh_user} 且未录入 {jvm_user} 的凭证。\
                     请让用户在环境管理中为该环境添加用户 {jvm_user} 的凭证后重试"
                )))
            }
        }
    };

    // 3.5 残留实例清理：上次失败 attach 留下的 arthas 会占住端口并因 already-bind 守卫挡死重试
    progress("cleanup", "清理残留 arthas 实例".to_string());
    let active_ports = (deps.active_ports_fn)(&req.env_id).await;
    cleanup_stale_instances(channel.as_ref(), &active_ports).await?;

    // 4. 分配远端端口（http + telnet 预检各一）+ 写 arthas.properties
    progress("allocate_port", "分配 arthas 端口".to_string());
    let (port, telnet_det_port) = find_free_remote_port(channel.as_ref()).await?;
    let token = generate_token();
    progress("write_config", format!("写入 arthas.properties（httpPort={port}）"));
    write_properties(channel.as_ref(), &arthas_home, &arthas_properties_content(port, &token)).await?;

    // 5. attach（nohup 后台驻留；临时连接场景执行完即断开）
    progress("attach", format!("attach arthas 到 PID {}（java={java}）", req.pid));
    let temp_disconnect: Option<TempAttachTransport> = match attach_exec_kind {
        AttachExecKind::Shared(shared) => {
            run_attach_command(shared.as_ref(), &java, &arthas_home, port, telnet_det_port, req.pid).await?;
            None
        }
        AttachExecKind::Temp(t) => {
            run_attach_command(&t, &java, &arthas_home, port, telnet_det_port, req.pid).await?;
            Some(t)
        }
    };
    if let Some(t) = temp_disconnect {
        tokio::spawn(async move { t.disconnect().await; });
    }

    // 6. 探活（端口可连 = arthas HTTP server 就绪）。失败先停掉可能已起的 arthas
    //    再报错（防 already-bind 残留挡死后续重试）
    progress("probe", "等待 arthas HTTP 服务就绪".to_string());
    if let Err(e) = wait_http_ready(channel.as_ref(), port, std::time::Duration::from_secs(60)).await {
        cleanup_partial_attach(channel.as_ref(), port, &token).await;
        return Err(e);
    }

    // 7. MCP 通路（exec HTTP 桥：每请求经 exec 通道在目标机本地 curl，不依赖
    //    sshd TCP 转发）+ MCP 握手。失败先停 arthas 再报错（防 already-bind
    //    残留挡死后续重试）
    progress("bridge", "建立 MCP 通路（exec HTTP 桥）".to_string());
    let url = format!("http://127.0.0.1:{port}/mcp");
    let bridge = crate::arthas::bridge::ExecHttpBridge::new(channel.clone(), 60);
    progress("handshake", format!("MCP 握手（{url}）"));
    let client: Arc<dyn ArthasClient> = match crate::arthas::client::connect_arthas_client(bridge, &url, &token).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            cleanup_partial_attach(channel.as_ref(), port, &token).await;
            return Err(ManagerError::Attach(format!("arthas MCP 握手失败: {e}")));
        }
    };

    progress("ready", format!("arthas 就绪（远端端口 {port}，exec HTTP 桥）"));
    let stop_handle: Arc<dyn ArthasStopHandle> = Arc::new(ProductionStopHandle {
        db: deps.db.clone(),
        exec_pool: deps.exec_pool.clone(),
        env_id: req.env_id.clone(),
        pod: None,
        container: None,
        remote_port: port,
        token,
        client: client.clone(),
        tunnels: deps.tunnels.clone(),
        pf: None,
    });
    Ok(AttachedSession { client, stop_handle, remote_port: port })
}

/// 容器分支（k8s）：双通道编排——base = 宿主机（装备中转、podIP 查询、残留
/// 清理/端口分配的 podIP 探测、探活），k8s_ch = 容器内执行（java 解析/
/// properties/attach/残留清理 stop/MCP curl 桥）。
async fn attach_arthas_in_pod(
    deps: AttachDeps,
    req: AttachRequest,
    base: Arc<dyn ExecChannel>,
    pod: &str,
) -> Result<AttachedSession, ManagerError> {
    let progress = |stage: &str, detail: String| {
        tracing::info!(session_id = %req.session_id, env_id = %req.env_id, pod, stage, detail = %detail, "arthas attach progress (pod)");
        deps.bus.emit(
            &req.session_id,
            AppEvent::ProvisionProgress {
                session_id: req.session_id.clone(),
                tool: "arthas_open".to_string(),
                stage: stage.to_string(),
                detail,
            },
        );
    };

    // 已知假设（记录，暂不修）：kubectl exec / kubectl get pod 均不带 -n，依赖
    // kubeconfig context 的当前 namespace（与 Phase 1 kubectl exec 行为一致；
    // k8s_find_pods 用 -A 全局发现）。若未来跨 namespace 环境出问题，需把
    // namespace 从 find_pods 贯通到工具参数。

    // 0. 容器执行通道（kubectl exec 包装；池内独立键，不复用 base 连接）
    progress("channel", format!("建立容器执行通道（pod {pod}）"));
    let k8s_ch = get_target_channel(&deps, &req.env_id, Some(pod), req.container.as_deref()).await?;

    // 1~6 前半程：装备 → java 解析 → 残留清理 → 端口/properties → attach → 探活
    let (port, token) = pod_attach_prepare(
        &deps,
        &req,
        &base,
        &k8s_ch,
        pod,
        std::time::Duration::from_secs(POD_PROBE_BUDGET_SECS),
        &progress,
    )
    .await?;

    // 7. MCP 通路（T6 主路径）：port-forward 隧道（宿主机 nohup kubectl
    //    port-forward → Pod mcp_port，TunnelManager direct-tcpip 到宿主机
    //    127.0.0.1:P）+ rmcp 原生 reqwest transport 握手 http://127.0.0.1:{L}/mcp；
    //    任一步失败 → 拆隧道 → 降级 exec HTTP 桥（T5 路径，容器需 curl）。
    //    隧道模式的 pf/隧道生命周期挂 stop handle（所有会话销毁路径统一经
    //    stop() 释放：close / LRU 逐出 / reaper / invalidate / close_for_environment）。
    let connector = ProductionPodMcpConnector {
        token: token.clone(),
        bridge_budget: std::time::Duration::from_secs(POD_HANDSHAKE_RETRY_BUDGET_SECS),
    };
    let (client, pf_lease) = establish_pod_mcp(
        &base,
        &k8s_ch,
        deps.tunnels.as_ref(),
        &connector,
        &req.env_id,
        pod,
        req.container.as_deref(),
        port,
        &token,
        &PfTunnelParams::default(),
        &progress,
    )
    .await?;

    let transport_desc = if pf_lease.is_some() { "port-forward 隧道" } else { "exec HTTP 桥" };
    progress(
        "ready",
        format!("arthas 就绪（pod {pod} 容器内端口 {port}，{transport_desc}）"),
    );
    let stop_handle: Arc<dyn ArthasStopHandle> = Arc::new(ProductionStopHandle {
        db: deps.db.clone(),
        exec_pool: deps.exec_pool.clone(),
        env_id: req.env_id.clone(),
        pod: Some(pod.to_string()),
        container: req.container.clone(),
        remote_port: port,
        token,
        client: client.clone(),
        tunnels: deps.tunnels.clone(),
        pf: pf_lease,
    });
    Ok(AttachedSession { client, stop_handle, remote_port: port })
}

/// 容器分支探活预算 / 握手重试预算 / 轮询间隔（秒）
const POD_PROBE_BUDGET_SECS: u64 = 60;
const POD_HANDSHAKE_RETRY_BUDGET_SECS: u64 = 60;
const POD_POLL_INTERVAL_SECS: u64 = 3;

/// 容器分支 MCP 客户端连接器（注入 seam：生产 = 原生 reqwest transport /
/// exec HTTP 桥；测试 = scripted mock）
#[async_trait]
trait PodMcpConnector: Sync {
    /// 原生 HTTP 握手（rmcp reqwest transport 打本地隧道端口）
    async fn connect_native(&self, url: &str) -> Result<Arc<dyn ArthasClient>, String>;
    /// exec HTTP 桥握手（容器内 curl；带重试预算——探活失败兜底路径下
    /// arthas 可能仍在启动）
    async fn connect_bridge(&self, k8s_ch: &Arc<dyn ExecChannel>, url: &str) -> Result<Arc<dyn ArthasClient>, String>;
}

/// 生产连接器：token / 桥重试预算随 attach 会话固定
struct ProductionPodMcpConnector {
    token: String,
    bridge_budget: std::time::Duration,
}

#[async_trait]
impl PodMcpConnector for ProductionPodMcpConnector {
    async fn connect_native(&self, url: &str) -> Result<Arc<dyn ArthasClient>, String> {
        crate::arthas::client::connect_arthas_client_native(url, &self.token)
            .await
            .map(|c| Arc::new(c) as Arc<dyn ArthasClient>)
    }

    async fn connect_bridge(&self, k8s_ch: &Arc<dyn ExecChannel>, url: &str) -> Result<Arc<dyn ArthasClient>, String> {
        connect_with_retry(k8s_ch, url, &self.token, self.bridge_budget)
            .await
            .map(|c| Arc::new(c) as Arc<dyn ArthasClient>)
    }
}

/// 容器分支 MCP 通路编排（T6 主路径）：① port-forward 隧道（宿主机 nohup pf
/// + TunnelManager direct-tcpip）+ rmcp 原生 reqwest transport 握手；
/// ② 任一步失败 → 拆隧道（kill pf + close）→ 降级 exec HTTP 桥（T5 路径，
/// 容器需 curl）。返回 (client, pf lease)——lease=Some 表示隧道模式
/// （stop 走隧道原生 HTTP，见 run_production_stop）。
async fn establish_pod_mcp(
    base: &Arc<dyn ExecChannel>,
    k8s_ch: &Arc<dyn ExecChannel>,
    tunnels: &dyn ArthasTunnels,
    connector: &dyn PodMcpConnector,
    env_id: &str,
    pod: &str,
    container: Option<&str>,
    mcp_port: u16,
    token: &str,
    pf_params: &PfTunnelParams,
    progress: &(dyn Fn(&str, String) + Sync),
) -> Result<(Arc<dyn ArthasClient>, Option<PfLease>), ManagerError> {
    progress("tunnel", format!("建立 MCP 通路（port-forward 隧道，pod {pod}）"));
    match establish_pf_tunnel(base.as_ref(), tunnels, env_id, pod, container, mcp_port, pf_params).await {
        Ok(lease) => {
            let url = format!("http://127.0.0.1:{}/mcp", lease.local_port);
            progress("handshake", format!("MCP 握手（原生 HTTP 隧道 {url}）"));
            match connector.connect_native(&url).await {
                Ok(client) => {
                    tracing::info!(env_id, pod, mcp_port, pf_pid = lease.pf_pid,
                        host_port = lease.host_port, local_port = lease.local_port,
                        "pod arthas mcp established via pf tunnel");
                    Ok((client, Some(lease)))
                }
                Err(e) => {
                    tracing::warn!(env_id, pod, url = %url, error = %e,
                        "原生 MCP 握手失败，拆除隧道并降级 exec HTTP 桥（容器内 curl）");
                    progress("bridge", "原生握手失败，拆除隧道并降级 exec HTTP 桥（容器内 curl）".to_string());
                    teardown_pf(base.as_ref(), tunnels, env_id, lease.pf_pid, Some(lease.host_port)).await;
                    pod_bridge_fallback(k8s_ch, connector, mcp_port, token, progress)
                        .await
                        .map(|c| (c, None))
                }
            }
        }
        Err(e) => {
            tracing::warn!(env_id, pod, mcp_port, error = %e,
                "port-forward 隧道建立失败，降级 exec HTTP 桥（容器需 curl）");
            progress("bridge", format!("隧道建立失败（{e}），降级 exec HTTP 桥（容器内 curl）"));
            pod_bridge_fallback(k8s_ch, connector, mcp_port, token, progress)
                .await
                .map(|c| (c, None))
        }
    }
}

/// MCP 兜底路径：exec HTTP 桥（T5 主路径，T6 起降级兜底；容器需 curl）。
/// 桥也失败 → cleanup_partial_attach（best-effort 停 arthas）后报错。
async fn pod_bridge_fallback(
    k8s_ch: &Arc<dyn ExecChannel>,
    connector: &dyn PodMcpConnector,
    mcp_port: u16,
    token: &str,
    progress: &(dyn Fn(&str, String) + Sync),
) -> Result<Arc<dyn ArthasClient>, ManagerError> {
    let url = format!("http://127.0.0.1:{mcp_port}/mcp");
    progress("handshake", format!("MCP 握手（exec HTTP 桥 {url}）"));
    match connector.connect_bridge(k8s_ch, &url).await {
        Ok(client) => Ok(client),
        Err(e) => {
            cleanup_partial_attach(k8s_ch.as_ref(), mcp_port, token).await;
            Err(ManagerError::Attach(format!("arthas MCP 握手失败: {e}")))
        }
    }
}

/// 容器 attach 前半程编排（装备 → java 解析 → podIP 查询 → 残留清理 → 端口/
/// properties → attach → 探活），返回 (http_port, token) 供 MCP 建桥握手。
/// base = 宿主机通道（ensure_k8s 中转 + podIP 查询/残留清理与端口分配的 podIP
/// 探测/探活）；k8s_ch = 容器内通道（java 解析/properties/attach/残留清理 stop）。
/// podIP 查询失败不硬失败：残留清理/端口分配降级容器内探测（busybox sh 无
/// /dev/tcp 时失明，同旧路径），探活跳过，最终可达性由 MCP 握手判定。
async fn pod_attach_prepare(
    deps: &AttachDeps,
    req: &AttachRequest,
    base: &Arc<dyn ExecChannel>,
    k8s_ch: &Arc<dyn ExecChannel>,
    pod: &str,
    probe_budget: std::time::Duration,
    progress: &(dyn Fn(&str, String) + Sync),
) -> Result<(u16, String), ManagerError> {
    // 1. 装备（宿主机中转 unzip→tar→kubectl exec -i 注入；幂等，cached 快路径；
    //    zip 缺失只在缓存未命中且需要下发时报，对齐 VM ensure 语义）
    progress("ensure_package", format!("确保容器内 arthas 工具包（pod {pod}）"));
    let arthas_pkg = crate::provision::arthas::ArthasPackage;
    arthas_pkg
        .ensure_k8s(
            base,
            pod,
            req.container.as_deref(),
            deps.arthas_zip.as_deref(),
            &req.session_id,
            &req.env_id,
        )
        .await
        .map_err(|e| ManagerError::Attach(format!("arthas 工具包下发失败: {}", e.message)))?;
    // 容器内 arthas 安装目录 = ensure_k8s 的注入目标（POD_TOOLS_DIR/arthas-dist）
    let arthas_home = format!("{}/arthas-dist", crate::exec::k8s::POD_TOOLS_DIR);

    // 2. 解析 attach 用 java（容器内语义：JdkCache 复合键 → 容器 PATH java → ensure K8s JDK）
    progress("resolve_java", "解析容器内 attach 用 java".to_string());
    let java = resolve_attach_java(deps, req, k8s_ch).await?;

    // 3. 用户对齐：跳过——容器 exec 用户 = ossadm = JVM 用户（设计决策），
    //    VM 分支的跨用户临时连接流程不适用。

    // 3.4 查询 Pod IP（宿主机侧 kubectl，输出经 IpAddr 校验防注入）：残留清理/
    //     端口分配的宿主机侧探测与探活共用（只查一次，探活复用；早取失败时
    //     探活前幂等重取一次）。容器 sh（busybox）无 bash /dev/tcp，容器内探测
    //     恒 free——端口分配失明会让同 Pod 并发会话恒选 18563 撞 already-bind，
    //     故 pod 模式端口探测一律走宿主机侧打 podIP。
    let pod_ip = match get_pod_ip(base.as_ref(), pod).await {
        Ok(ip) => Some(ip),
        Err(e) => {
            tracing::warn!(session_id = %req.session_id, env_id = %req.env_id, pod, error = %e,
                "podIP 查询失败：残留清理/端口分配降级容器内探测（busybox 下失明），探活跳过，MCP 握手兜底");
            None
        }
    };

    // 3.5 残留实例清理：探测走宿主机侧打 podIP（容器 sh 无 /dev/tcp，容器内探测
    //     恒 free 失明），stop 走容器内 curl（容器内 127.0.0.1 回环可达）。podIP
    //     拿不到时降级容器内探测（busybox 下恒 free，清理退化为 no-op——
    //     already-bind 由探活/握手超时兜底报错）
    progress("cleanup", "清理残留 arthas 实例".to_string());
    let active_ports = (deps.active_ports_fn)(&req.env_id).await;
    match &pod_ip {
        Some(ip) => {
            cleanup_stale_instances_with(
                base.as_ref(),
                k8s_ch.as_ref(),
                &|port| pod_ip_probe_command(ip, port),
                &active_ports,
                std::time::Duration::from_secs(15),
                std::time::Duration::from_millis(500),
            )
            .await?;
        }
        None => {
            cleanup_stale_instances(k8s_ch.as_ref(), &active_ports).await?;
        }
    }

    // 4. 端口分配 + properties 写入（容器内 dist 目录；绑 0.0.0.0 供宿主侧探测/T6 隧道接入）。
    //    pod 模式端口探测走宿主机侧 podIP:port（容器内探测失明）；podIP 拿不到时
    //    降级容器内探测（旧路径，busybox 下失明，MCP 握手兜底）
    progress("allocate_port", "分配 arthas 端口".to_string());
    let (port, telnet_det_port) = match &pod_ip {
        Some(ip) => find_free_remote_port_pod(base.as_ref(), ip).await?,
        None => find_free_remote_port(k8s_ch.as_ref()).await?,
    };
    let token = generate_token();
    progress("write_config", format!("写入 arthas.properties（httpPort={port}，绑定 0.0.0.0）"));
    write_properties(k8s_ch.as_ref(), &arthas_home, &arthas_properties_content_pod(port, &token)).await?;

    // 5. attach（容器内 nohup；日志 /tmp/arthas-friday-{pid}.log 为 Pod 内路径，语义正确）
    progress("attach", format!("attach arthas 到 PID {}（java={java}）", req.pid));
    run_attach_command(k8s_ch.as_ref(), &java, &arthas_home, port, telnet_det_port, req.pid).await?;

    // 6. 探活：宿主机侧 /dev/tcp 打 podIP（容器 sh 无 /dev/tcp，容器内探测不可行）。
    //    podIP 复用 3.4 的查询结果（早取失败的幂等重取一次——容忍瞬时 kubectl 故障）。
    //    失败兜底：不硬失败——宿主→Pod 网络可能被拦截，交由 MCP 握手（容器内
    //    curl 127.0.0.1）+ 重试做最终判定。
    progress("probe", "等待 arthas HTTP 服务就绪（宿主机侧探 podIP）".to_string());
    let pod_ip = match pod_ip {
        Some(ip) => Ok(ip),
        None => get_pod_ip(base.as_ref(), pod).await,
    };
    match pod_ip {
        Ok(pod_ip) => {
            if let Err(e) = wait_pod_port_ready(
                base.as_ref(),
                &pod_ip,
                port,
                probe_budget,
                std::time::Duration::from_secs(POD_POLL_INTERVAL_SECS),
            )
            .await
            {
                tracing::warn!(session_id = %req.session_id, env_id = %req.env_id, pod, port, error = %e, "podIP 探活未通过，交由 MCP 握手兜底判定");
            }
        }
        Err(e) => {
            tracing::warn!(session_id = %req.session_id, env_id = %req.env_id, pod, error = %e, "podIP 查询失败，跳过探活，交由 MCP 握手兜底判定");
        }
    }
    Ok((port, token))
}

/// best-effort stop：HTTP stop arthas（卸载 agent）+ 关 MCP client。
/// pf=Some（T6 隧道模式）时 stop 编排见 run_production_stop：先经隧道原生 HTTP
/// 停 arthas（TunnelManager 专属连接，不依赖宿主机 exec 通道——base 拿不到也
/// 先试）→ kill 宿主机 pf → 关 TunnelManager 隧道（base 拿不到仍关隧道，kill pf
/// 交由下次 attach 的 pkill 兜底）；隧道 stop 失败回落 exec 通道 curl
/// （k8s = kubectl exec 容器内；VM = 宿主机本地）。
/// pf=None（VM / 桥降级模式）：exec 通道 curl stop（原 T5 行为）。
struct ProductionStopHandle {
    db: sqlx::SqlitePool,
    exec_pool: Arc<Mutex<ExecChannelPool>>,
    env_id: String,
    pod: Option<String>,
    container: Option<String>,
    remote_port: u16,
    token: String,
    client: Arc<dyn ArthasClient>,
    /// SSH 隧道管理器（pf 隧道关闭）
    tunnels: Arc<dyn ArthasTunnels>,
    /// T6 隧道模式的 pf/隧道租约（桥降级 / VM 模式为 None）
    pf: Option<PfLease>,
}

#[async_trait::async_trait]
impl ArthasStopHandle for ProductionStopHandle {
    async fn stop(&self) {
        // 通道懒解析（stop 可能发生在 attach 很久之后，池连接可能已换/重建）
        let get_base_channel = {
            let db = self.db.clone();
            let exec_pool = self.exec_pool.clone();
            let env_id = self.env_id.clone();
            move || {
                let db = db.clone();
                let exec_pool = exec_pool.clone();
                let env_id = env_id.clone();
                Box::pin(async move {
                    get_default_channel_raw(&db, &exec_pool, &env_id)
                        .await
                        .map_err(|e| e.to_string())
                }) as Pin<Box<dyn Future<Output = Result<Arc<dyn ExecChannel>, String>> + Send>>
            }
        };
        let get_target_channel = {
            let db = self.db.clone();
            let exec_pool = self.exec_pool.clone();
            let env_id = self.env_id.clone();
            let pod = self.pod.clone();
            let container = self.container.clone();
            move || {
                let db = db.clone();
                let exec_pool = exec_pool.clone();
                let env_id = env_id.clone();
                let pod = pod.clone();
                let container = container.clone();
                Box::pin(async move {
                    get_target_channel_raw(&db, &exec_pool, &env_id, pod.as_deref(), container.as_deref())
                        .await
                        .map_err(|e| e.to_string())
                }) as Pin<Box<dyn Future<Output = Result<Arc<dyn ExecChannel>, String>> + Send>>
            }
        };
        let tunnel_stop = {
            let token = self.token.clone();
            move |port: u16| {
                let token = token.clone();
                Box::pin(async move { super::tunnel::http_stop_via_tunnel(port, &token).await })
                    as Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
            }
        };
        run_production_stop(
            self.client.as_ref(),
            self.tunnels.as_ref(),
            self.pf.as_ref(),
            &self.env_id,
            self.pod.as_deref(),
            self.remote_port,
            &self.token,
            &tunnel_stop,
            &get_base_channel,
            &get_target_channel,
        )
        .await;
    }
}

/// stop 编排核心（参数全注入，测试 seam）。固定顺序：
/// ① client.shutdown()（DELETE MCP 会话——隧道模式经原生 transport 走隧道，
///    此时 arthas 与隧道都还活着，语义与 T5 一致：先清会话再停 arthas）
/// ② pf 存在（隧道模式）：先经隧道原生 HTTP stop arthas（tunnel_stop 走
///    TunnelManager 专属连接，**不依赖宿主机 exec 通道**——拿不到 base 时也
///    必须先试）→ kill pf + 关隧道（teardown_pf；kill pf 固定先于 tunnels.close
///    ——反序时 close 失败会泄漏宿主机 pf 进程）。宿主机通道拿不到：仍关隧道，
///    kill pf 交由下次 attach 的 pkill 清残留兜底
/// ③ arthas 尚未停掉（无 pf / 隧道 stop 失败）：exec 通道 curl stop 兜底
async fn run_production_stop(
    client: &dyn ArthasClient,
    tunnels: &dyn ArthasTunnels,
    pf: Option<&PfLease>,
    env_id: &str,
    pod: Option<&str>,
    remote_port: u16,
    token: &str,
    tunnel_stop: &(dyn Fn(u16) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Sync),
    get_base_channel: &(dyn Fn() -> Pin<Box<dyn Future<Output = Result<Arc<dyn ExecChannel>, String>> + Send>> + Sync),
    get_target_channel: &(dyn Fn() -> Pin<Box<dyn Future<Output = Result<Arc<dyn ExecChannel>, String>> + Send>> + Sync),
) {
    // ① DELETE MCP 会话
    client.shutdown().await;

    // ② 隧道段
    let mut agent_stopped = false;
    if let Some(pf) = pf {
        // ②a 隧道原生 stop：TunnelManager 专属连接（本地端口 L），不依赖宿主机
        //     exec 通道——base 拿不到也先试（旧序在 base 失败时整段跳过，
        //     白白放弃不依赖 base 的停 arthas 通路）
        agent_stopped = match tunnel_stop(pf.local_port).await {
            Ok(()) => {
                tracing::info!(env_id, local_port = pf.local_port, "arthas stopped via tunnel-native http");
                true
            }
            Err(e) => {
                tracing::warn!(
                    env_id, local_port = pf.local_port, error = %e,
                    "tunnel-native arthas stop 失败（将回落 exec 通道 stop）"
                );
                false
            }
        };
        // ②b kill pf + 关隧道（需宿主机通道；拿不到 → 仍关隧道条目，kill pf
        //     交由下次 attach 的 pkill 清残留兜底）
        match get_base_channel().await {
            Ok(base) => {
                super::tunnel::teardown_pf(base.as_ref(), tunnels, env_id, pf.pf_pid, Some(pf.host_port)).await;
            }
            Err(e) => {
                tracing::warn!(
                    env_id, pod = ?pod, error = %e,
                    "拿不到宿主机通道，kill pf 失败（宿主机残留 pf 由下次 attach 的 pkill 清理兜底）；隧道条目仍关闭"
                );
                tunnels.close(env_id, super::tunnel::TUNNEL_REMOTE_HOST, pf.host_port).await;
            }
        }
    }

    // ③ exec 通道 stop 兜底（VM / 桥降级 / 隧道 stop 失败）
    if !agent_stopped {
        match get_target_channel().await {
            Ok(channel) => match run_with_timeout(channel.as_ref(), &stop_command(remote_port, token), 15).await {
                Ok(_) => tracing::info!(env_id, pod = ?pod, port = remote_port, "arthas stopped via http api (exec channel)"),
                Err(e) => tracing::warn!(env_id, pod = ?pod, port = remote_port, error = %e, "arthas http stop failed (best-effort)"),
            },
            Err(e) => tracing::warn!(env_id, pod = ?pod, port = remote_port, error = %e, "failed to get exec channel for arthas http stop (best-effort skip)"),
        }
    }
}

// ── 编排子步骤 ──

async fn get_default_channel(deps: &AttachDeps, env_id: &str) -> Result<Arc<dyn ExecChannel>, ManagerError> {
    get_default_channel_raw(&deps.db, &deps.exec_pool, env_id).await
}

/// 按目标取通道（pod=None = 宿主机 VM 模式；k8s 分支容器执行通道同源）
async fn get_target_channel(
    deps: &AttachDeps,
    env_id: &str,
    pod: Option<&str>,
    container: Option<&str>,
) -> Result<Arc<dyn ExecChannel>, ManagerError> {
    get_target_channel_raw(&deps.db, &deps.exec_pool, env_id, pod, container).await
}

async fn get_default_channel_raw(
    db: &sqlx::SqlitePool,
    exec_pool: &Arc<Mutex<ExecChannelPool>>,
    env_id: &str,
) -> Result<Arc<dyn ExecChannel>, ManagerError> {
    get_target_channel_raw(db, exec_pool, env_id, None, None).await
}

async fn get_target_channel_raw(
    db: &sqlx::SqlitePool,
    exec_pool: &Arc<Mutex<ExecChannelPool>>,
    env_id: &str,
    pod: Option<&str>,
    container: Option<&str>,
) -> Result<Arc<dyn ExecChannel>, ManagerError> {
    let mut pool = exec_pool.lock().await;
    pool.get_or_create(env_id, pod, container, db)
        .await
        .map_err(|e| ManagerError::Attach(format!("SSH 连接失败: {e}")))
}

/// remote_tools_dir：目标机工具根目录（VM = REMOTE_TOOLS_DIR；k8s 场景由调用方传
/// POD_TOOLS_DIR——T5 接入 k8s 分支）。
async fn provision_context(
    deps: &AttachDeps,
    req: &AttachRequest,
    channel: Arc<dyn ExecChannel>,
    remote_tools_dir: &str,
) -> Result<crate::provision::package::ProvisionContext, ManagerError> {
    let base = crate::app::settings::artifactory_base_url(&deps.db)
        .await
        .map_err(|e| ManagerError::Attach(format!("读取 Artifactory 设置失败: {e}")))?;
    if base.trim().is_empty() {
        return Err(ManagerError::Attach(
            "Artifactory 地址未配置，请在设置中配置后重试".to_string(),
        ));
    }
    Ok(crate::provision::package::ProvisionContext {
        session_id: req.session_id.clone(),
        env_id: req.env_id.clone(),
        channel,
        cache_dir: deps.cache_dir.clone(),
        artifactory_base_url: base,
        arthas_zip: deps.arthas_zip.clone(),
        remote_tools_dir: remote_tools_dir.to_string(),
        timeouts: crate::provision::package::StageTimeouts::default(),
        bus: deps.bus.clone(),
    })
}

/// attach 用 java 可执行文件解析：JdkCache（目标复合键：VM = env_id，k8s =
/// env|pod=..|ctr=..）→ PATH java（channel 上执行：VM = 宿主机 / k8s = 容器内）
/// → ensure JDK（VM = JdkPackage，k8s = K8sJdkPackage 装进 POD_TOOLS_DIR；
/// 结果回写 JdkCache）。返回可执行文件完整路径（已做字符集校验）。
/// ProvisionContext 只在 ensure 兜底路径构建——容器有 PATH java 时不强制 Artifactory 配置。
async fn resolve_attach_java(
    deps: &AttachDeps,
    req: &AttachRequest,
    channel: &Arc<dyn ExecChannel>,
) -> Result<String, ManagerError> {
    let target = crate::exec::pool::TargetKey::from_parts(
        &req.env_id,
        req.pod.as_deref(),
        req.container.as_deref(),
    );
    let cache_key = crate::tools::builtin::jvm::jdk_cache::cache_key(&target);
    if let Some(layout) = deps.jdk_cache.get(&cache_key).await {
        return Ok(format!("{}/bin/java", layout.tool_home));
    }
    // PATH 上有 java：直接用（JRE 也够跑 arthas-boot）
    if let Ok(out) = run_with_timeout(channel.as_ref(), "command -v java", 15).await {
        let java = out.stdout.trim().to_string();
        if out.exit_code == 0 && !java.is_empty() {
            // 字符集校验（防 shell 注入，与 ensure_tool 的 java_bin 同款规则）
            if crate::provision::jdk::validate_java_bin(&java).is_ok() {
                return Ok(java);
            }
            tracing::warn!(java = %java, "PATH java path failed charset validation, ignoring");
        }
    }
    // 兜底：ensure JDK（依赖 java_bin 参数指向可用 java；目标机无 java 时给 agent 可行动的错误）
    let remote_tools_dir = if req.pod.is_some() {
        crate::exec::k8s::POD_TOOLS_DIR
    } else {
        crate::provision::jdk::REMOTE_TOOLS_DIR
    };
    let pctx = provision_context(deps, req, channel.clone(), remote_tools_dir).await?;
    let jdk: Box<dyn crate::provision::package::ToolPackage> = if req.pod.is_some() {
        Box::new(crate::provision::k8s::K8sJdkPackage)
    } else {
        Box::new(crate::provision::jdk::JdkPackage)
    };
    match jdk.ensure(&pctx, &req.java_bin).await {
        Ok(result) => {
            deps.jdk_cache
                .set(
                    &cache_key,
                    crate::tools::builtin::jvm::jdk_cache::JdkLayout {
                        tool_home: result.tool_home.clone(),
                        bins: result.bins.clone(),
                    },
                )
                .await;
            Ok(format!("{}/bin/java", result.tool_home))
        }
        Err(e) => Err(ManagerError::Attach(format!(
            "目标机找不到可用的 java（{}）。可用 run_command 确认目标服务的 java 路径后，\
             用 java_bin 参数重试 arthas_open",
            e.message
        ))),
    }
}

/// 用户对齐检查（jvm_user, ssh_user）
async fn check_users(
    channel: &dyn ExecChannel,
    pid: i64,
) -> Result<(String, String), ManagerError> {
    let out = run_with_timeout(channel, &check_user_command(pid), 15).await?;
    parse_user_check(&out.stdout).map_err(|e| ManagerError::Attach(format!("用户对齐检查失败: {e}; stderr: {}", out.stderr)))
}

/// 分配远端空闲端口（VM 分支：18563 起顺序探测目标机本地回环，取两个：
/// http + telnet 预检）
async fn find_free_remote_port(channel: &dyn ExecChannel) -> Result<(u16, u16), ManagerError> {
    let cmd = find_free_port_command(ARTHAS_PORT_START, ARTHAS_PORT_CANDIDATES);
    let out = run_with_timeout(channel, &cmd, 20).await?;
    parse_free_port(&out.stdout).map_err(ManagerError::Attach)
}

/// 分配 Pod 内空闲端口（pod 分支：宿主机侧 /dev/tcp 打 podIP 逐候选探测——
/// 容器 sh 无 /dev/tcp，容器内探测恒 free 失明。timeout 1 兜底网络黑洞，
/// 10 候选最坏 10s，整体预算 30s）
async fn find_free_remote_port_pod(
    base: &dyn ExecChannel,
    pod_ip: &str,
) -> Result<(u16, u16), ManagerError> {
    let cmd = find_free_port_pod_command(pod_ip, ARTHAS_PORT_START, ARTHAS_PORT_CANDIDATES);
    let out = run_with_timeout(base, &cmd, 30).await?;
    parse_free_port(&out.stdout).map_err(ManagerError::Attach)
}

/// 写 arthas.properties（经默认连接执行；chmod 644 保证 jvm_user 可读）
async fn write_properties(
    channel: &dyn ExecChannel,
    home: &str,
    content: &str,
) -> Result<(), ManagerError> {
    let out = run_with_timeout(channel, &write_properties_command(home, content), 15).await?;
    if out.exit_code != 0 {
        return Err(ManagerError::Attach(format!(
            "写入 arthas.properties 失败（exit {}）: {}",
            out.exit_code, out.stderr
        )));
    }
    Ok(())
}

/// 执行 attach 命令（临时连接场景用后即断）
async fn run_attach_command(
    exec: &dyn ExecChannel,
    java: &str,
    arthas_home: &str,
    http_port: u16,
    telnet_det_port: u16,
    pid: i64,
) -> Result<(), ManagerError> {
    let out = run_with_timeout(exec, &attach_command(java, arthas_home, http_port, telnet_det_port, pid), 30).await?;
    if out.exit_code != 0 {
        return Err(ManagerError::Attach(format!(
            "arthas attach 命令失败（exit {}）: {}",
            out.exit_code, out.stderr
        )));
    }
    Ok(())
}

/// 探活循环：端口可连即认为 arthas HTTP server 就绪（bash /dev/tcp，无 curl 依赖）
async fn wait_http_ready(
    channel: &dyn ExecChannel,
    port: u16,
    budget: std::time::Duration,
) -> Result<(), ManagerError> {
    let deadline = tokio::time::Instant::now() + budget;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let out = run_with_timeout(channel, &port_probe_command(port), 15)
            .await
            .map_err(|e| ManagerError::Attach(format!("arthas 探活失败: {e}")))?;
        if out.stdout.trim() == "busy" {
            tracing::info!(port, attempt, "arthas http server ready");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ManagerError::Attach(format!(
                "arthas HTTP 服务在 {}s 内未就绪（端口 {port}）。\
                 可能原因：attach 失败（用户权限/attach 机制被禁用）、目标 JVM 拒绝 attach。\
                 可用 run_command 查看 {ARTHAS_LOG_HINT} 日志",
                budget.as_secs()
            )));
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

/// attach 日志位置提示（错误消息用）
const ARTHAS_LOG_HINT: &str = "/tmp/arthas-friday-<pid>.log";

/// 查询 Pod IP（宿主机侧 kubectl，输出经 IpAddr 校验防注入）
async fn get_pod_ip(base: &dyn ExecChannel, pod: &str) -> Result<String, ManagerError> {
    let out = run_with_timeout(base, &pod_ip_command(pod), 20).await?;
    let ip = out.stdout.trim().to_string();
    if out.exit_code != 0 || ip.is_empty() {
        return Err(ManagerError::Attach(format!(
            "查询 Pod {pod} 的 IP 失败（kubectl get pod，exit {}）: {}",
            out.exit_code,
            out.stderr.trim()
        )));
    }
    validate_pod_ip(&ip).map_err(ManagerError::Attach)?;
    Ok(ip)
}

/// 容器分支探活循环（宿主机侧 /dev/tcp 打 podIP）：可连即认为 arthas HTTP
/// server 就绪。budget/interval 参数化（测试用快参数，生产 60s/3s）。
async fn wait_pod_port_ready(
    base: &dyn ExecChannel,
    pod_ip: &str,
    port: u16,
    budget: std::time::Duration,
    poll_interval: std::time::Duration,
) -> Result<(), ManagerError> {
    let deadline = tokio::time::Instant::now() + budget;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let out = run_with_timeout(base, &pod_ip_probe_command(pod_ip, port), 15)
            .await
            .map_err(|e| ManagerError::Attach(format!("podIP 探活失败: {e}")))?;
        if out.stdout.trim() == "busy" {
            tracing::info!(pod_ip, port, attempt, "arthas http server ready (pod ip probe)");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ManagerError::Attach(format!(
                "arthas HTTP 服务在 {}s 内未就绪（podIP {pod_ip}:{port} 不可达）。\
                 可能原因：attach 失败、目标 JVM 拒绝 attach、宿主机到 Pod 网络不通。\
                 可用 k8s_find_pods / run_command 查看 Pod 内 {ARTHAS_LOG_HINT} 日志",
                budget.as_secs()
            )));
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// MCP 握手（带重试预算）：容器分支探活兜底路径下 arthas 可能仍在启动，
/// 连接拒绝类失败按间隔重试直至预算耗尽（每次尝试独立建桥）。
async fn connect_with_retry(
    channel: &Arc<dyn ExecChannel>,
    url: &str,
    token: &str,
    budget: std::time::Duration,
) -> Result<crate::arthas::client::McpArthasClient, String> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let bridge = crate::arthas::bridge::ExecHttpBridge::new(channel.clone(), 60);
        match crate::arthas::client::connect_arthas_client(bridge, url, token).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tracing::warn!(url, error = %e, "arthas MCP 握手失败，重试中");
                tokio::time::sleep(std::time::Duration::from_secs(POD_POLL_INTERVAL_SECS)).await;
            }
        }
    }
}

/// 残留 arthas 实例清理（VM 模式：探测与 stop 都在目标机本地 127.0.0.1）。
/// v0.11.2+ 实例（localConnectionNonAuth）本地 stop 免密；更早残留会 401 → 报错指路重启目标服务。
async fn cleanup_stale_instances(
    channel: &dyn ExecChannel,
    active_ports: &[u16],
) -> Result<(), ManagerError> {
    cleanup_stale_instances_with(
        channel,
        channel,
        &port_probe_command,
        active_ports,
        std::time::Duration::from_secs(15),
        std::time::Duration::from_millis(500),
    )
    .await
}

/// cleanup_stale_instances 的参数化内核：探测通道 / stop 通道 / 探测命令注入。
/// VM = 宿主机本地 port_probe_command（探 127.0.0.1）；pod = 宿主机侧
/// pod_ip_probe_command 打 podIP + 容器内 stop（容器 sh 无 /dev/tcp，容器内
/// 探测恒 free 失明）。stop 恒为执行侧本地 curl（127.0.0.1 回环：VM = 宿主机 /
/// pod = 容器内）。等待预算/轮询间隔可调（测试用快参数）。
async fn cleanup_stale_instances_with(
    probe_channel: &dyn ExecChannel,
    stop_channel: &dyn ExecChannel,
    probe_command: &(dyn Fn(u16) -> String + Sync),
    active_ports: &[u16],
    wait_budget: std::time::Duration,
    poll_interval: std::time::Duration,
) -> Result<(), ManagerError> {
    for port in ARTHAS_PORT_START..ARTHAS_PORT_START + ARTHAS_PORT_CANDIDATES {
        if active_ports.contains(&port) {
            continue;
        }
        let probe = run_with_timeout(probe_channel, &probe_command(port), 15)
            .await
            .map_err(|e| ManagerError::Attach(format!("残留端口探测失败: {e}")))?;
        if probe.stdout.trim() != "busy" {
            continue;
        }
        tracing::info!(port, "stale arthas instance detected, stopping");
        run_with_timeout(stop_channel, &stop_command(port, ""), 15).await?;
        let deadline = tokio::time::Instant::now() + wait_budget;
        loop {
            tokio::time::sleep(poll_interval).await;
            let check = run_with_timeout(probe_channel, &probe_command(port), 15).await?;
            if check.stdout.trim() != "busy" {
                tracing::info!(port, "stale arthas instance stopped");
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ManagerError::Attach(format!(
                    "端口 {port} 被残留 arthas 占用且无法停止（可能是旧版本实例，stop 被拒绝）。请重启目标服务后重试"
                )));
            }
        }
    }
    Ok(())
}

/// attach 中途失败的收尾：best-effort 停掉可能已启动的 arthas（stop 失败不掩盖原错误）
async fn cleanup_partial_attach(channel: &dyn ExecChannel, port: u16, token: &str) {
    match run_with_timeout(channel, &stop_command(port, token), 15).await {
        Ok(_) => tracing::info!(port, "partial attach cleaned up (arthas stopped)"),
        Err(e) => tracing::warn!(port, error = %e, "cleanup stop failed, arthas agent may remain on target"),
    }
}

/// 临时 attach 连接：jvm_user 凭证 → 独立 SshTransport（用后由调用方 disconnect）
async fn build_temp_transport(
    deps: &AttachDeps,
    env_id: &str,
    cred: &crate::app::env_credentials::EnvCredentialRow,
) -> Result<TempAttachTransport, ManagerError> {
    let env = crate::exec::pool::fetch_environment(&deps.db, env_id)
        .await
        .map_err(|e| ManagerError::Attach(format!("环境查询失败: {e}")))?;
    let auth = crate::exec::ssh::SshAuth::from_row(&cred.auth_type, cred.private_key_path.as_deref())
        .ok_or_else(|| ManagerError::Attach(format!("用户 {} 的认证配置无效", cred.username)))?;
    let secret = crate::app::credentials::load_cred_secret(env_id, &cred.id)
        .await
        .map_err(|e| ManagerError::Attach(format!("读取用户 {} 密钥失败: {e}", cred.username)))?;
    let secret = match (&auth, secret) {
        // 密码认证但未存储密码（None/空串）→ 明确报错（不能回落到默认用户的旧密钥）；
        // 私钥认证的 None 合法（无口令私钥），原样透传
        (crate::exec::ssh::SshAuth::Password, s) if s.as_deref().map_or(true, |v| v.trim().is_empty()) => {
            return Err(ManagerError::Attach(format!(
                "用户 {} 的凭证未存储密码，请在环境管理中补录该用户的密码后重试",
                cred.username
            )));
        }
        (_, s) => s,
    };
    let transport = crate::exec::ssh::SshTransport::with_secret(
        env_id,
        env.host.as_deref().unwrap_or_default(),
        env.port.unwrap_or(22),
        &cred.username,
        auth,
        secret,
    );
    transport
        .connect()
        .await
        .map_err(|e| ManagerError::Attach(format!("以用户 {} 建立连接失败: {e}", cred.username)))?;
    Ok(TempAttachTransport { inner: transport })
}

/// 临时连接包装：断开由调用方处理（russh 无 async Drop，用后台 spawn）
struct TempAttachTransport {
    inner: crate::exec::ssh::SshTransport,
}

#[async_trait::async_trait]
impl ExecChannel for TempAttachTransport {
    async fn run(&self, cmd: &str) -> Result<crate::exec::channel::ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.run(cmd).await
    }
    async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.connect().await
    }
    async fn disconnect(&self) {
        self.inner.disconnect().await;
    }
    async fn is_alive(&self) -> bool {
        self.inner.is_alive().await
    }
}

/// 统一的带超时远端执行（命令本身都应秒级返回）
async fn run_with_timeout(
    channel: &dyn ExecChannel,
    cmd: &str,
    secs: u64,
) -> Result<crate::exec::channel::ExecOutput, ManagerError> {
    match tokio::time::timeout(std::time::Duration::from_secs(secs), channel.run(cmd)).await {
        Err(_) => Err(ManagerError::Attach(format!("远端命令执行超时（{secs}s）: {cmd}"))),
        Ok(Err(e)) => Err(ManagerError::Attach(format!("远端命令执行失败: {e}（命令: {cmd}）"))),
        Ok(Ok(out)) => {
            if !out.stderr.trim().is_empty() {
                tracing::debug!(cmd, stderr = %out.stderr, "remote command stderr");
            }
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arthas_properties_content() {
        let content = arthas_properties_content(18563, "abc123");
        assert!(content.contains("arthas.config.overrideAll=true\n"));
        assert!(content.contains("arthas.mcpEndpoint=/mcp\n"));
        assert!(content.contains("arthas.telnetPort=-1\n"));
        assert!(content.contains("arthas.httpPort=18563\n"));
        assert!(content.contains("arthas.password=abc123\n"));
        assert!(content.contains("arthas.localConnectionNonAuth=true\n"));
        assert!(content.contains("arthas.ip=127.0.0.1\n"));
        // 无单引号/美元符（安全嵌入 shell 单引号）
        assert!(!content.contains('\''));
        assert!(!content.contains('$'));
    }

    #[test]
    fn test_check_user_command() {
        assert_eq!(check_user_command(123), "ps -o user= -p 123 2>/dev/null; echo '---'; id -un");
    }

    #[test]
    fn test_parse_user_check() {
        let (jvm, ssh) = parse_user_check("svcapp\n---\nopc\n").unwrap();
        assert_eq!(jvm, "svcapp");
        assert_eq!(ssh, "opc");
        // ps 输出带空白
        let (jvm, ssh) = parse_user_check("  svcapp \n---\n opc \n").unwrap();
        assert_eq!(jvm, "svcapp");
        assert_eq!(ssh, "opc");
    }

    #[test]
    fn test_parse_user_check_pid_gone() {
        assert!(parse_user_check("\n---\nopc\n").is_err());
    }

    #[test]
    fn test_find_free_port_command_and_parse() {
        let cmd = find_free_port_command(18563, 10);
        assert!(cmd.contains("seq 18563 18572"));
        // 返回两个空闲端口：第一行 http，第二行 telnet 预检
        assert_eq!(parse_free_port("18563\n18564\n").unwrap(), (18563, 18564));
        assert!(parse_free_port("none\n").is_err());
        // 只有一个空闲端口：不够用，报错
        assert!(parse_free_port("18563\nnone\n").is_err());
    }

    #[test]
    fn test_find_free_port_pod_command_shape() {
        let cmd = find_free_port_pod_command("10.244.1.5", 18563, 10);
        assert!(cmd.contains("seq 18563 18572"), "cmd: {cmd}");
        // $p 经双引号由宿主机 shell 展开后传入内层 bash——单引号内变量不透传，
        // 空展开会让所有端口恒报空闲（重蹈 busybox /dev/tcp 失明）
        assert!(cmd.contains("bash -c \"exec 3<>/dev/tcp/10.244.1.5/$p\""), "cmd: {cmd}");
        // timeout 1 兜底网络黑洞（与 pod_ip_probe_command 同构）
        assert!(cmd.contains("timeout 1"), "cmd: {cmd}");
        // 连不上 = 空闲 → 输出端口（与 VM 版语义一致，取前两个）
        assert!(cmd.contains("|| echo $p"), "cmd: {cmd}");
        assert!(cmd.contains("head -2"), "cmd: {cmd}");
        assert!(cmd.ends_with("; true"), "cmd: {cmd}");
        // 解析复用 parse_free_port（宿主机侧探测结果与 VM 形同）
        assert_eq!(parse_free_port("18564\n18565").unwrap(), (18564, 18565));
    }

    #[test]
    fn test_port_probe_command() {
        assert!(port_probe_command(8563).contains("/dev/tcp/127.0.0.1/8563"));
    }

    #[test]
    fn test_attach_command() {
        let cmd = attach_command(
            "/tmp/friday-tools/jdk-21/bin/java",
            "/tmp/friday-tools/arthas-4.3.5",
            18563,      // http_port
            19563,      // telnet_det_port
            123,        // pid
        );
        assert!(cmd.contains("cd /tmp/friday-tools/arthas-4.3.5"));
        assert!(cmd.contains("--attach-only"));
        assert!(cmd.contains("--http-port 18563"));
        assert!(cmd.contains("--telnet-port 19563"));
        // pid 是位置参数（arthas-boot 无 --pid 选项），放在最后
        assert!(cmd.contains("arthas-boot.jar --attach-only --http-port 18563 --telnet-port 19563 123"));
        assert!(!cmd.contains("--pid"));
        assert!(cmd.contains("< /dev/null"));
        assert!(cmd.contains("&"));
        assert!(cmd.contains(">> /tmp/arthas-friday-123.log 2>&1"));
    }

    #[test]
    fn test_write_properties_command_quotes_content() {
        let content = arthas_properties_content(18563, "tok123");
        let cmd = write_properties_command("/tmp/friday-tools/arthas-4.3.5", &content);
        assert!(cmd.starts_with("printf '%s' 'arthas."));
        assert!(cmd.contains("> /tmp/friday-tools/arthas-4.3.5/arthas.properties"));
        assert!(cmd.contains("chmod 644 /tmp/friday-tools/arthas-4.3.5/arthas.properties"));
    }

    #[test]
    fn test_stop_command_contains_auth_and_payload() {
        let cmd = stop_command(18563, "tok123");
        assert!(cmd.contains("Authorization: Bearer tok123"));
        assert!(cmd.contains("http://127.0.0.1:18563/api"));
        assert!(cmd.contains("\"command\":\"stop\""));
    }

    #[test]
    fn test_generate_token_charset() {
        for _ in 0..10 {
            let t = generate_token();
            assert_eq!(t.len(), 32);
            assert!(t.chars().all(|c| c.is_ascii_alphanumeric()));
        }
    }

    // ── 残留清理 / 失败收尾（RecordingChannel stub，模型对齐 provision/arthas.rs 测试）──

    use crate::exec::channel::ExecOutput;
    use std::collections::VecDeque;

    /// 记录 run 调用、按队列顺序返回预置输出
    struct RecordingChannel {
        calls: tokio::sync::Mutex<Vec<String>>,
        responses: tokio::sync::Mutex<VecDeque<ExecOutput>>,
    }

    impl RecordingChannel {
        fn new(responses: Vec<(&str, i32)>) -> Arc<Self> {
            let dq = responses
                .into_iter()
                .map(|(o, c)| ExecOutput { stdout: o.to_string(), stderr: String::new(), exit_code: c })
                .collect();
            Arc::new(Self {
                calls: tokio::sync::Mutex::new(Vec::new()),
                responses: tokio::sync::Mutex::new(dq),
            })
        }

        async fn calls(&self) -> Vec<String> {
            self.calls.lock().await.clone()
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
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        async fn disconnect(&self) {}
        async fn is_alive(&self) -> bool {
            true
        }
    }

    /// run 恒定失败的通道（best-effort 收尾的失败路径用）
    struct FailingChannel;

    #[async_trait::async_trait]
    impl ExecChannel for FailingChannel {
        async fn run(&self, _cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            Err("ssh broken".into())
        }
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        async fn disconnect(&self) {}
        async fn is_alive(&self) -> bool {
            true
        }
    }

    /// 测试用快参数（预算 200ms / 轮询 50ms），避免 15s 生产等待拖慢 CI
    const FAST_BUDGET: std::time::Duration = std::time::Duration::from_millis(200);
    const FAST_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

    #[tokio::test]
    async fn test_cleanup_stale_instances_stops_occupied_non_active_port() {
        // 18563 被残留占用：probe busy → stop → 复查 free；其余 9 个候选端口 free
        let channel = RecordingChannel::new(vec![
            ("busy", 0), // 18563 探测：占用
            ("", 0),     // 18563 HTTP stop
            ("free", 0), // 18563 复查：已释放
            ("free", 0), ("free", 0), ("free", 0), ("free", 0), ("free", 0),
            ("free", 0), ("free", 0), ("free", 0), ("free", 0), // 18564~18572
        ]);
        cleanup_stale_instances_with(
            channel.as_ref(),
            channel.as_ref(),
            &port_probe_command,
            &[],
            FAST_BUDGET,
            FAST_INTERVAL,
        )
        .await
        .unwrap();
        let calls = channel.calls().await;
        assert_eq!(calls.len(), 12, "calls: {calls:?}");
        assert!(calls[0].contains("/dev/tcp/127.0.0.1/18563"), "calls[0]: {}", calls[0]);
        // stop 必须指向 18563 的 /api 且携带 stop payload
        assert!(calls[1].contains("http://127.0.0.1:18563/api"), "calls[1]: {}", calls[1]);
        assert!(calls[1].contains("\"command\":\"stop\""), "calls[1]: {}", calls[1]);
        assert!(calls[2].contains("/dev/tcp/127.0.0.1/18563"), "calls[2]: {}", calls[2]);
    }

    #[tokio::test]
    async fn test_cleanup_stale_instances_skips_active_ports() {
        // 18563 是活跃会话端口：不得探测、不得 stop（否则会误杀在用会话）
        let channel = RecordingChannel::new(vec![
            ("free", 0), ("free", 0), ("free", 0), ("free", 0), ("free", 0),
            ("free", 0), ("free", 0), ("free", 0), ("free", 0), // 18564~18572
        ]);
        cleanup_stale_instances_with(
            channel.as_ref(),
            channel.as_ref(),
            &port_probe_command,
            &[18563],
            FAST_BUDGET,
            FAST_INTERVAL,
        )
        .await
        .unwrap();
        let calls = channel.calls().await;
        assert_eq!(calls.len(), 9, "active port must be skipped entirely, calls: {calls:?}");
        assert!(calls.iter().all(|c| !c.contains("18563")), "calls: {calls:?}");
        assert!(calls.iter().all(|c| !c.contains("/api")), "no stop must be issued, calls: {calls:?}");
    }

    #[tokio::test]
    async fn test_cleanup_stale_instances_unstoppable_port_errors() {
        // 18563 stop 后仍占用（旧版本实例拒绝 stop）→ 结构化报错指路重启目标服务
        let mut responses = vec![
            ("busy", 0), // 18563 探测：占用
            ("", 0),     // 18563 stop（被拒绝）
        ];
        for _ in 0..30 {
            responses.push(("busy", 0)); // 复查始终 busy，直到预算耗尽
        }
        let channel = RecordingChannel::new(responses);
        let err = cleanup_stale_instances_with(
            channel.as_ref(),
            channel.as_ref(),
            &port_probe_command,
            &[],
            FAST_BUDGET,
            FAST_INTERVAL,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("重启目标服务"), "err: {err}");
        assert!(err.to_string().contains("18563"), "err: {err}");
    }

    #[tokio::test]
    async fn test_cleanup_stale_instances_pod_probes_podip_on_base_stops_in_container() {
        // pod 模式：探测走宿主机侧 podIP（busybox 容器内 /dev/tcp 恒 free 失明），
        // stop 走容器内 curl（127.0.0.1 回环在容器内可达）
        let mut probe_script = vec![
            ("busy", 0), // 18563 探测（base 打 podIP）：占用
            ("free", 0), // 18563 复查：已释放
        ];
        for _ in 0..(ARTHAS_PORT_CANDIDATES - 1) {
            probe_script.push(("free", 0)); // 18564~18572
        }
        let base = RecordingChannel::new(probe_script);
        let k8s = RecordingChannel::new(vec![("", 0)]); // 18563 HTTP stop（容器内）
        cleanup_stale_instances_with(
            base.as_ref(),
            k8s.as_ref(),
            &|port| pod_ip_probe_command("10.244.1.5", port),
            &[],
            FAST_BUDGET,
            FAST_INTERVAL,
        )
        .await
        .unwrap();
        let base_calls = base.calls().await;
        assert_eq!(base_calls.len(), 11, "base_calls: {base_calls:?}");
        assert!(base_calls[0].contains("/dev/tcp/10.244.1.5/18563"), "probe: {}", base_calls[0]);
        assert!(base_calls[1].contains("/dev/tcp/10.244.1.5/18563"), "recheck: {}", base_calls[1]);
        // stop 只出现在容器通道（宿主机侧无 /api 请求）
        assert!(!base_calls.iter().any(|c| c.contains("/api")), "no stop on base: {base_calls:?}");
        let k8s_calls = k8s.calls().await;
        assert_eq!(k8s_calls.len(), 1, "k8s_calls: {k8s_calls:?}");
        assert!(k8s_calls[0].contains("http://127.0.0.1:18563/api"), "stop: {}", k8s_calls[0]);
        assert!(k8s_calls[0].contains("\"command\":\"stop\""), "stop: {}", k8s_calls[0]);
    }

    #[tokio::test]
    async fn test_cleanup_partial_attach_issues_best_effort_stop() {
        // 正常路径：发出 stop 命令（含 token 与端口）
        let channel = RecordingChannel::new(vec![("", 0)]);
        cleanup_partial_attach(channel.as_ref(), 18563, "tok123").await;
        let calls = channel.calls().await;
        assert_eq!(calls.len(), 1);
        assert!(calls[0].contains("Authorization: Bearer tok123"), "calls[0]: {}", calls[0]);
        assert!(calls[0].contains("http://127.0.0.1:18563/api"), "calls[0]: {}", calls[0]);

        // 通道失败路径：best-effort（仅告警），不 panic、不向上抛错
        cleanup_partial_attach(&FailingChannel, 18563, "tok123").await;
    }

    // ── 容器分支（pod_attach_prepare：双通道编排 + podIP 探活 + 用户对齐跳过）──

    use crate::exec::k8s::K8sChannel;
    use crate::exec::pool::ExecChannelPool;
    use std::future::Future;
    use std::pin::Pin;

    /// 容器分支探活快参数（预算 200ms / 轮询 50ms），避免 60s 生产等待拖慢 CI
    const FAST_PROBE_BUDGET: std::time::Duration = std::time::Duration::from_millis(200);
    const FAST_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

    fn pod_req() -> AttachRequest {
        AttachRequest {
            session_id: "s1".into(),
            env_id: "env-1".into(),
            pid: 1234,
            java_bin: "java".into(),
            pod: Some("svc-1".into()),
            container: None,
        }
    }

    fn pod_deps() -> AttachDeps {
        pod_deps_with_active_ports(Vec::new())
    }

    /// 指定活跃会话端口的 deps（busybox 失明回归测试用：同 Pod 并发会话场景）
    fn pod_deps_with_active_ports(active: Vec<u16>) -> AttachDeps {
        let active_ports_fn: ActivePortsFn = Arc::new(move |_env_id: &str| {
            let active = active.clone();
            Box::pin(async move { active }) as Pin<Box<dyn Future<Output = Vec<u16>> + Send>>
        });
        AttachDeps {
            db: sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            exec_pool: Arc::new(Mutex::new(ExecChannelPool::new())),
            jdk_cache: Arc::new(crate::tools::builtin::jvm::jdk_cache::JdkCache::new()),
            cache_dir: PathBuf::from("/tmp/unused-cache"),
            arthas_zip: None, // 缓存命中路径不触 zip；装备失败路径由 provision::arthas 测试覆盖
            bus: crate::app::events::EventBus::disabled(),
            active_ports_fn,
            tunnels: noop_tunnels(),
        }
    }

    /// no-op tunnels 替身（不触隧道路径的测试用）
    fn noop_tunnels() -> Arc<dyn ArthasTunnels> {
        Arc::new(NoopTunnels)
    }

    struct NoopTunnels;

    #[async_trait]
    impl ArthasTunnels for NoopTunnels {
        async fn open(&self, _env_id: &str, _remote_host: &str, _remote_port: u16) -> Result<u16, String> {
            unreachable!("test must not open tunnels")
        }
        async fn close(&self, _env_id: &str, _remote_host: &str, _remote_port: u16) {}
    }

    /// 容器内命令脚本：java 解析 + 写 properties + attach（残留清理/端口分配的
    /// 探测已移宿主机侧——容器 sh 无 /dev/tcp）
    fn pod_channel_script() -> Vec<(&'static str, i32)> {
        vec![
            ("/usr/lib/jvm/java-21/bin/java", 0),
            ("", 0), // 写 properties
            ("attach-started", 0),
        ]
    }

    /// 宿主机侧脚本：ensure_k8s 缓存命中 + podIP 查询（只查一次）+ 残留清理探测
    /// ×10（宿主机侧打 podIP，全 free）+ 端口分配探测（18563/18564 空闲）+ 探活 busy
    fn pod_base_script() -> Vec<(&'static str, i32)> {
        let mut script = vec![
            ("", 0),           // kubectl exec test -f arthas-boot.jar（缓存命中）
            ("10.244.1.5", 0), // kubectl get pod jsonpath（端口探测/探活共用）
        ];
        for _ in 0..ARTHAS_PORT_CANDIDATES {
            script.push(("free", 0)); // 残留清理探测：宿主机侧打 podIP，全 free
        }
        script.push(("18563\n18564", 0)); // 端口分配：宿主机侧探测结果
        script.push(("busy", 0));         // 探活：就绪
        script
    }

    /// 真实 K8sChannel 包装的容器通道（记录到底层 base，命令带 kubectl exec 包装）
    fn k8s_recording_channel(script: Vec<(&'static str, i32)>) -> (Arc<RecordingChannel>, Arc<dyn ExecChannel>) {
        let inner = RecordingChannel::new(script);
        let k8s: Arc<dyn ExecChannel> = Arc::new(K8sChannel {
            base: inner.clone(),
            pod: "svc-1".into(),
            container: None,
        });
        (inner, k8s)
    }

    #[tokio::test]
    async fn test_pod_attach_prepare_orchestration() {
        // base（宿主机）：ensure_k8s 缓存命中 + podIP 查询（早取一次）+ 残留清理
        // 探测 ×10 + 端口分配探测 + /dev/tcp 探活成功
        let base = RecordingChannel::new(pod_base_script());
        let base_ch: Arc<dyn ExecChannel> = base.clone();
        let (pod_ch, k8s_ch) = k8s_recording_channel(pod_channel_script());
        let deps = pod_deps();
        let req = pod_req();
        let (port, token) = pod_attach_prepare(
            &deps,
            &req,
            &base_ch,
            &k8s_ch,
            "svc-1",
            FAST_PROBE_BUDGET,
            &|_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(port, 18563);
        assert_eq!(token.len(), 32);

        let base_calls = base.calls().await;
        assert_eq!(base_calls.len(), 14, "base_calls: {base_calls:?}");
        // ① 装备走 ensure_k8s：宿主机侧 kubectl exec 缓存检查（POD_TOOLS_DIR/arthas-dist）
        assert!(base_calls[0].contains("kubectl exec"), "ensure_k8s check: {}", base_calls[0]);
        assert!(
            base_calls[0].contains("arthas-dist/arthas-boot.jar"),
            "ensure_k8s check: {}",
            base_calls[0]
        );
        // ② podIP 查询（早取，端口探测/探活共用——只查一次）
        assert!(base_calls[1].contains("kubectl get pod"), "podIP cmd: {}", base_calls[1]);
        assert!(
            base_calls.iter().filter(|c| c.contains("kubectl get pod")).count() == 1,
            "podIP must be fetched once and reused: {base_calls:?}"
        );
        // ③ 残留清理探测走宿主机侧打 podIP（容器 sh 无 /dev/tcp，容器内探测恒 free）
        for (i, probe_port) in (18563..18563 + ARTHAS_PORT_CANDIDATES).enumerate() {
            assert!(
                base_calls[2 + i].contains(&format!("/dev/tcp/10.244.1.5/{probe_port}")),
                "cleanup probe[{i}]: {}",
                base_calls[2 + i]
            );
        }
        // ④ 端口分配走宿主机侧：循环探测 podIP:$p（$p 双引号展开传入内层 bash）
        let find_free = &base_calls[12];
        assert!(find_free.contains("seq 18563 18572"), "find-free: {find_free}");
        assert!(
            find_free.contains("bash -c \"exec 3<>/dev/tcp/10.244.1.5/$p\""),
            "find-free: {find_free}"
        );
        // ⑤ 探活复用早取的 podIP（不重复 kubectl get pod）
        assert!(base_calls[13].contains("/dev/tcp/10.244.1.5/18563"), "probe: {}", base_calls[13]);

        let pod_calls = pod_ch.calls().await;
        assert_eq!(pod_calls.len(), 3, "pod_calls: {pod_calls:?}");
        // 容器内命令全部经 kubectl exec 包装（真实 K8sChannel 语义）
        for c in &pod_calls {
            assert!(c.contains("kubectl exec 'svc-1' -- sh -c"), "must be kubectl-wrapped: {c}");
        }
        // java 解析在容器内（第一个容器命令）
        assert!(pod_calls[0].contains("command -v java"), "java resolve: {}", pod_calls[0]);
        // 端口探测全部在宿主机侧：容器内无 /dev/tcp（busybox 失明修复）
        assert!(
            pod_calls.iter().all(|c| !c.contains("/dev/tcp")),
            "no port probing inside container: {pod_calls:?}"
        );
        // ⑥ properties 写到容器内 dist 目录，内容绑 0.0.0.0（宿主侧探测/T6 隧道可达）
        let write = pod_calls.iter().find(|c| c.contains("arthas.properties")).expect("write props cmd");
        assert!(
            write.contains("/opt/log/dump/coredump/friday-tools/arthas-dist/arthas.properties"),
            "write: {write}"
        );
        assert!(write.contains("arthas.ip=0.0.0.0"), "write: {write}");
        assert!(!write.contains("arthas.ip=127.0.0.1"), "write: {write}");
        // attach 命令：容器内 dist 目录 + arthas-boot.jar
        let attach = pod_calls.iter().find(|c| c.contains("arthas-boot.jar")).expect("attach cmd");
        assert!(attach.contains("cd /opt/log/dump/coredump/friday-tools/arthas-dist"), "attach: {attach}");
        assert!(attach.contains("--attach-only"), "attach: {attach}");
        assert!(attach.contains("1234"), "attach pid: {attach}");
        // 容器执行不泄漏到宿主机通道（properties/attach/java 都不在 base 上）
        assert!(
            base_calls.iter().all(|c| !c.contains("arthas.properties") && !c.contains("arthas-boot.jar --attach-only")),
            "container cmds must not run on base: {base_calls:?}"
        );
        // 用户对齐被跳过：双通道均无 ps -o user= / id -un
        for c in base_calls.iter().chain(pod_calls.iter()) {
            assert!(
                !c.contains("ps -o user=") && !c.contains("id -un"),
                "user alignment must be skipped in pod branch: {c}"
            );
        }
    }

    #[tokio::test]
    async fn test_pod_attach_prepare_concurrent_session_skips_occupied_port() {
        // busybox /dev/tcp 失明回归：同 Pod 第二个并发会话——18563 已被活跃会话
        // 占用，宿主机侧端口分配必须探到占用并跳到下一候选（容器内探测恒 free
        // 会恒选 18563，第二个会话 attach 撞 already-bind）
        let mut base_script = vec![
            ("", 0),           // ensure_k8s 缓存命中
            ("10.244.1.5", 0), // podIP 查询
        ];
        for _ in 0..(ARTHAS_PORT_CANDIDATES - 1) {
            base_script.push(("free", 0)); // 残留清理：18564~18572（18563 活跃跳过）
        }
        base_script.push(("18564\n18565", 0)); // 端口分配：18563 被占 → 18564/18565
        base_script.push(("busy", 0));          // 探活（18564）：就绪
        let base = RecordingChannel::new(base_script);
        let base_ch: Arc<dyn ExecChannel> = base.clone();
        let (pod_ch, k8s_ch) = k8s_recording_channel(pod_channel_script());
        let deps = pod_deps_with_active_ports(vec![18563]); // 第一会话占用 18563
        let req = pod_req();
        let (port, _token) = pod_attach_prepare(
            &deps,
            &req,
            &base_ch,
            &k8s_ch,
            "svc-1",
            FAST_PROBE_BUDGET,
            &|_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(port, 18564, "must skip the port held by the concurrent session");

        // attach 命令带新分配的端口对（http 18564 / telnet 预检 18565）
        let pod_calls = pod_ch.calls().await;
        let attach = pod_calls.iter().find(|c| c.contains("arthas-boot.jar")).expect("attach cmd");
        assert!(attach.contains("--http-port 18564"), "attach: {attach}");
        assert!(attach.contains("--telnet-port 18565"), "attach: {attach}");

        let base_calls = base.calls().await;
        assert_eq!(base_calls.len(), 13, "base_calls: {base_calls:?}");
        // 端口分配在宿主机侧打 podIP（$p 循环探测）
        let find_free = &base_calls[11];
        assert!(find_free.contains("seq 18563 18572"), "find-free: {find_free}");
        assert!(find_free.contains("/dev/tcp/10.244.1.5/$p"), "find-free: {find_free}");
        // 活跃端口 18563 未被残留清理探测（避免误杀在用会话）
        assert!(
            !base_calls.iter().any(|c| c.contains("/dev/tcp/10.244.1.5/18563")),
            "active port must not be probed: {base_calls:?}"
        );
        // 探活打新分配的端口 18564
        assert!(base_calls[12].contains("/dev/tcp/10.244.1.5/18564"), "probe: {}", base_calls[12]);
    }

    #[tokio::test]
    async fn test_pod_attach_prepare_probe_unreachable_falls_back() {
        // 探活恒 free（宿主→Pod 网络不通）+ 短预算：prepare 仍成功（交由 MCP 握手兜底）
        let mut base_script = pod_base_script();
        base_script.pop(); // 去掉探活 busy
        for _ in 0..8 {
            base_script.push(("free", 0)); // 探活循环：始终不可达
        }
        let base = RecordingChannel::new(base_script);
        let base_ch: Arc<dyn ExecChannel> = base.clone();
        let (pod_ch, k8s_ch) = k8s_recording_channel(pod_channel_script());
        let deps = pod_deps();
        let req = pod_req();
        let (port, _token) = pod_attach_prepare(
            &deps,
            &req,
            &base_ch,
            &k8s_ch,
            "svc-1",
            FAST_PROBE_BUDGET,
            &|_, _| {},
        )
        .await
        .expect("probe failure must fall through to mcp handshake, not hard-fail");
        assert_eq!(port, 18563);
        let base_calls = base.calls().await;
        // 探活（打已分配端口 18563）必须发生在端口分配之后
        let find_free_idx = base_calls
            .iter()
            .position(|c| c.contains("seq 18563 18572"))
            .expect("find-free cmd");
        let last_probe_idx = base_calls
            .iter()
            .rposition(|c| c.contains("/dev/tcp/10.244.1.5/18563"))
            .expect("probe cmd");
        assert!(find_free_idx < last_probe_idx, "probe must follow allocation: {base_calls:?}");
        assert!(pod_ch.calls().await.len() == 3, "no probing inside container");
    }

    #[tokio::test]
    async fn test_pod_attach_prepare_bad_pod_ip_falls_back() {
        // kubectl 返回非 IP（垃圾输出）：残留清理/端口分配降级容器内探测（busybox
        // 下失明，与修复前行为一致），探活重取一次后跳过；prepare 仍成功
        let base = RecordingChannel::new(vec![
            ("", 0),        // ensure_k8s 缓存命中
            ("garbage", 0), // podIP 早取：非 IP
            ("garbage", 0), // 探活前幂等重取：仍非 IP
        ]);
        let base_ch: Arc<dyn ExecChannel> = base.clone();
        // 容器内降级脚本：java + 残留清理探测 ×10 + 端口分配 + 写 properties + attach
        let mut pod_script = vec![("/usr/lib/jvm/java-21/bin/java", 0)];
        for _ in 0..ARTHAS_PORT_CANDIDATES {
            pod_script.push(("free", 0));
        }
        pod_script.push(("18563\n18564", 0));
        pod_script.push(("", 0));
        pod_script.push(("attach-started", 0));
        let (pod_ch, k8s_ch) = k8s_recording_channel(pod_script);
        let deps = pod_deps();
        let req = pod_req();
        let result = pod_attach_prepare(
            &deps,
            &req,
            &base_ch,
            &k8s_ch,
            "svc-1",
            FAST_PROBE_BUDGET,
            &|_, _| {},
        )
        .await;
        assert!(result.is_ok(), "bad podIP must fall through to mcp handshake: {result:?}");
        let base_calls = base.calls().await;
        assert_eq!(base_calls.len(), 3, "ensure + podIP ×2（早取 + 探活重取）: {base_calls:?}");
        let pod_calls = pod_ch.calls().await;
        assert_eq!(pod_calls.len(), 14, "degraded cleanup + find-free in container: {pod_calls:?}");
        // 降级路径：容器内探测 127.0.0.1（busybox 恒 free——失明但可用，行为与修复前一致）
        assert!(pod_calls[1].contains("/dev/tcp/127.0.0.1/18563"), "degraded probe: {}", pod_calls[1]);
        assert!(pod_calls[11].contains("seq 18563 18572"), "degraded find-free: {}", pod_calls[11]);
    }

    #[test]
    fn test_pod_ip_command_shape() {
        let cmd = pod_ip_command("svc-abc");
        assert!(cmd.contains("kubectl get pod 'svc-abc'"), "cmd: {cmd}");
        assert!(cmd.contains("jsonpath='{.status.podIP}'"), "cmd: {cmd}");
        // pod 名注入防护：单引号转义
        let evil = pod_ip_command("x'; rm -rf /; '");
        assert!(evil.contains(r"'\''"), "must escape quotes: {evil}");
    }

    #[test]
    fn test_pod_ip_probe_command_shape() {
        let cmd = pod_ip_probe_command("10.244.1.5", 18563);
        assert!(cmd.contains("timeout 1"), "cmd: {cmd}");
        assert!(cmd.contains("/dev/tcp/10.244.1.5/18563"), "cmd: {cmd}");
        assert!(cmd.contains("busy"), "cmd: {cmd}");
        assert!(cmd.contains("free"), "cmd: {cmd}");
    }

    #[test]
    fn test_validate_pod_ip() {
        assert!(validate_pod_ip("10.244.1.5").is_ok());
        assert!(validate_pod_ip("fd00::1").is_ok());
        // 非 IP 字面量一律拒绝（防注入）
        assert!(validate_pod_ip("garbage").is_err());
        assert!(validate_pod_ip("10.244.1.5; rm -rf /").is_err());
        assert!(validate_pod_ip("127.0.0.1/24").is_err());
        assert!(validate_pod_ip("").is_err());
    }

    #[test]
    fn test_arthas_properties_content_pod_binds_wildcard() {
        let content = arthas_properties_content_pod(18563, "abc123");
        // 容器分支绑 0.0.0.0（宿主侧探活 / T6 隧道从 Pod 网络进入）
        assert!(content.contains("arthas.ip=0.0.0.0\n"));
        // 其余键与 VM 模式一致
        assert!(content.contains("arthas.config.overrideAll=true\n"));
        assert!(content.contains("arthas.mcpEndpoint=/mcp\n"));
        assert!(content.contains("arthas.telnetPort=-1\n"));
        assert!(content.contains("arthas.httpPort=18563\n"));
        assert!(content.contains("arthas.password=abc123\n"));
        assert!(content.contains("arthas.localConnectionNonAuth=true\n"));
        // 无单引号/美元符（安全嵌入 shell 单引号）
        assert!(!content.contains('\''));
        assert!(!content.contains('$'));
        // VM 模式维持官方默认回环绑定
        assert!(arthas_properties_content(18563, "abc123").contains("arthas.ip=127.0.0.1\n"));
    }

    // ── T6 pf 隧道：MCP 通路编排（establish_pod_mcp）──

    use crate::arthas::manager::CallOutcome;

    type EventLog = Arc<Mutex<Vec<String>>>;

    fn mock_client(events: EventLog) -> Arc<dyn ArthasClient> {
        Arc::new(MockArthasClient { events })
    }

    struct MockArthasClient {
        events: EventLog,
    }

    #[async_trait]
    impl ArthasClient for MockArthasClient {
        async fn call_tool(&self, _name: &str, _args: &serde_json::Value) -> Result<CallOutcome, String> {
            Ok(CallOutcome { text: "ok".into(), is_error: false })
        }
        async fn shutdown(&self) {
            self.events.lock().await.push("client-shutdown".into());
        }
    }

    /// 事件记录 + 脚本化输出通道：每次 run 把 "label:cmd" 记入事件日志
    /// （跨组件顺序断言用），响应按脚本顺序返回
    struct ScriptedEventChannel {
        events: EventLog,
        label: &'static str,
        script: Mutex<VecDeque<(&'static str, i32)>>,
    }

    impl ScriptedEventChannel {
        fn new(events: EventLog, label: &'static str, script: Vec<(&'static str, i32)>) -> Arc<Self> {
            Arc::new(Self {
                events,
                label,
                script: Mutex::new(script.into_iter().collect()),
            })
        }
    }

    #[async_trait]
    impl ExecChannel for ScriptedEventChannel {
        async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            self.events.lock().await.push(format!("{}:{}", self.label, cmd));
            let (stdout, exit_code) = self.script.lock().await.pop_front().unwrap_or(("", 0));
            Ok(ExecOutput { stdout: stdout.to_string(), stderr: String::new(), exit_code })
        }
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        async fn disconnect(&self) {}
        async fn is_alive(&self) -> bool {
            true
        }
    }

    /// tunnels mock：open/close 记入事件日志；open 按脚本返回
    struct MockTunnels {
        events: EventLog,
        opens: Mutex<VecDeque<Result<u16, String>>>,
    }

    impl MockTunnels {
        fn new(events: EventLog, opens: Vec<Result<u16, String>>) -> Arc<Self> {
            Arc::new(Self { events, opens: Mutex::new(opens.into_iter().collect()) })
        }
    }

    #[async_trait]
    impl ArthasTunnels for MockTunnels {
        async fn open(&self, env_id: &str, remote_host: &str, remote_port: u16) -> Result<u16, String> {
            self.events
                .lock()
                .await
                .push(format!("tunnels-open:{env_id}/{remote_host}/{remote_port}"));
            self.opens.lock().await.pop_front().unwrap_or(Ok(0))
        }
        async fn close(&self, env_id: &str, remote_host: &str, remote_port: u16) {
            self.events
                .lock()
                .await
                .push(format!("tunnels-close:{env_id}/{remote_host}/{remote_port}"));
        }
    }

    /// MCP 连接器 mock：native/bridge 各按脚本成败，URL 记入事件日志
    struct ScriptedConnector {
        events: EventLog,
        native: Mutex<VecDeque<Result<(), String>>>,
        bridge: Mutex<VecDeque<Result<(), String>>>,
    }

    impl ScriptedConnector {
        fn new(events: EventLog, native: Vec<Result<(), String>>, bridge: Vec<Result<(), String>>) -> Self {
            Self {
                events,
                native: Mutex::new(native.into_iter().collect()),
                bridge: Mutex::new(bridge.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl PodMcpConnector for ScriptedConnector {
        async fn connect_native(&self, url: &str) -> Result<Arc<dyn ArthasClient>, String> {
            self.events.lock().await.push(format!("native-connect:{url}"));
            match self.native.lock().await.pop_front() {
                Some(Ok(())) => Ok(mock_client(self.events.clone())),
                Some(Err(e)) => Err(e),
                None => unreachable!("native script exhausted"),
            }
        }
        async fn connect_bridge(&self, _k8s_ch: &Arc<dyn ExecChannel>, url: &str) -> Result<Arc<dyn ArthasClient>, String> {
            self.events.lock().await.push(format!("bridge-connect:{url}"));
            match self.bridge.lock().await.pop_front() {
                Some(Ok(())) => Ok(mock_client(self.events.clone())),
                Some(Err(e)) => Err(e),
                None => unreachable!("bridge script exhausted"),
            }
        }
    }

    /// 绑一个本地监听端口（保持存活供健康检查连通）
    async fn live_local_port() -> (u16, tokio::net::TcpListener) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (port, listener)
    }

    /// 建立成功的 base 脚本：清残留 → pf 启动（PID 4242）→ 日志空 → Forwarding（P=54321）
    fn pf_base_script() -> Vec<(&'static str, i32)> {
        vec![
            ("", 0),
            ("4242", 0),
            ("", 0),
            ("Forwarding from 127.0.0.1:54321 -> 18563", 0),
        ]
    }

    /// pf 隧道快参数（预算 300ms / 轮询 20ms / 健康检查 400ms），避免 10s 生产等待拖慢 CI
    fn fast_pf_params() -> PfTunnelParams {
        PfTunnelParams {
            log_budget: std::time::Duration::from_millis(300),
            log_interval: std::time::Duration::from_millis(20),
            health_budget: std::time::Duration::from_millis(400),
        }
    }

    #[tokio::test]
    async fn test_establish_pod_mcp_tunnel_happy_path() {
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        let base = ScriptedEventChannel::new(events.clone(), "base", pf_base_script());
        let base_ch: Arc<dyn ExecChannel> = base;
        let k8s_ch: Arc<dyn ExecChannel> = ScriptedEventChannel::new(events.clone(), "k8s", vec![]);
        let (live_port, _guard) = live_local_port().await;
        let tunnels = MockTunnels::new(events.clone(), vec![Ok(live_port)]);
        let connector = ScriptedConnector::new(events.clone(), vec![Ok(())], vec![]);

        let (client, lease) = establish_pod_mcp(
            &base_ch,
            &k8s_ch,
            tunnels.as_ref(),
            &connector,
            "env-1",
            "svc-1",
            None,
            18563,
            "tok123",
            &fast_pf_params(),
            &|_, _| {},
        )
        .await
        .unwrap();
        client.shutdown().await; // mock 可正常 shutdown

        let lease = lease.expect("tunnel lease");
        assert_eq!(lease.pf_pid, 4242);
        assert_eq!(lease.host_port, 54321);
        assert_eq!(lease.local_port, live_port);

        let ev = events.lock().await.clone();
        // 顺序：清残留 → 起 pf → 日志轮询 → tunnels.open → 原生握手
        let idx = |needle: &str| ev.iter().position(|e| e.contains(needle)).expect(needle);
        assert!(idx("pkill -f") < idx("nohup kubectl port-forward"), "ev: {ev:?}");
        assert!(idx("nohup kubectl port-forward") < idx("tunnels-open:"), "ev: {ev:?}");
        assert!(
            ev.iter().any(|e| e == &format!("tunnels-open:env-1/127.0.0.1/54321")),
            "ev: {ev:?}"
        );
        assert!(
            ev.iter().any(|e| e == &format!("native-connect:http://127.0.0.1:{live_port}/mcp")),
            "ev: {ev:?}"
        );
        // 主路径成功：不走桥、不拆隧道
        assert!(!ev.iter().any(|e| e.starts_with("bridge-connect:")), "ev: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("tunnels-close:")), "ev: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("base:kill")), "ev: {ev:?}");
        // 容器通道不被触（原生 transport 不经 k8s exec）
        assert!(!ev.iter().any(|e| e.starts_with("k8s:")), "ev: {ev:?}");
    }

    #[tokio::test]
    async fn test_establish_pod_mcp_native_fail_falls_back_to_bridge() {
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        let base = ScriptedEventChannel::new(events.clone(), "base", pf_base_script());
        let base_ch: Arc<dyn ExecChannel> = base;
        let k8s_ch: Arc<dyn ExecChannel> = ScriptedEventChannel::new(events.clone(), "k8s", vec![]);
        let (live_port, _guard) = live_local_port().await;
        let tunnels = MockTunnels::new(events.clone(), vec![Ok(live_port)]);
        let connector = ScriptedConnector::new(
            events.clone(),
            vec![Err("native handshake boom".to_string())],
            vec![Ok(())],
        );

        let (client, lease) = establish_pod_mcp(
            &base_ch,
            &k8s_ch,
            tunnels.as_ref(),
            &connector,
            "env-1",
            "svc-1",
            None,
            18563,
            "tok123",
            &fast_pf_params(),
            &|_, _| {},
        )
        .await
        .unwrap();
        client.shutdown().await;
        assert!(lease.is_none(), "fallback session has no pf lease");

        let ev = events.lock().await.clone();
        // 拆隧道：kill pf + tunnels.close（kill 在 close 前）
        let kill_idx = ev.iter().position(|e| e.starts_with("base:kill 4242")).expect("kill pf");
        let close_idx = ev
            .iter()
            .position(|e| e == "tunnels-close:env-1/127.0.0.1/54321")
            .expect("close tunnel");
        assert!(kill_idx < close_idx, "kill must precede close: {ev:?}");
        // 拆除先于桥握手
        let bridge_idx = ev
            .iter()
            .position(|e| e == "bridge-connect:http://127.0.0.1:18563/mcp")
            .expect("bridge fallback");
        assert!(close_idx < bridge_idx, "teardown must precede bridge: {ev:?}");
    }

    #[tokio::test]
    async fn test_establish_pod_mcp_tunnel_fail_falls_back_to_bridge() {
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        // pf 日志一直不出现 Forwarding（快预算内耗尽）→ 隧道建立失败 → 降级桥
        let mut script = vec![("", 0), ("4242", 0)];
        for _ in 0..50 {
            script.push(("", 0));
        }
        let base = ScriptedEventChannel::new(events.clone(), "base", script);
        let base_ch: Arc<dyn ExecChannel> = base;
        let k8s_ch: Arc<dyn ExecChannel> = ScriptedEventChannel::new(events.clone(), "k8s", vec![]);
        let tunnels = MockTunnels::new(events.clone(), vec![]); // open 不应被调
        let connector = ScriptedConnector::new(events.clone(), vec![], vec![Ok(())]);

        let (client, lease) = establish_pod_mcp(
            &base_ch,
            &k8s_ch,
            tunnels.as_ref(),
            &connector,
            "env-1",
            "svc-1",
            None,
            18563,
            "tok123",
            &fast_pf_params(),
            &|_, _| {},
        )
        .await
        .unwrap();
        client.shutdown().await;
        assert!(lease.is_none());

        let ev = events.lock().await.clone();
        assert!(!ev.iter().any(|e| e.starts_with("tunnels-open:")), "open must not be called: {ev:?}");
        assert!(
            ev.iter().any(|e| e == "bridge-connect:http://127.0.0.1:18563/mcp"),
            "ev: {ev:?}"
        );
        // 失败路径 pf 已被 kill（清残留）
        assert!(ev.iter().any(|e| e.starts_with("base:kill 4242")), "ev: {ev:?}");
    }

    #[tokio::test]
    async fn test_establish_pod_mcp_both_fail_cleans_partial_attach() {
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        // 隧道建立失败 + 桥握手失败 → 报错 + cleanup_partial_attach（k8s 通道 stop）
        let mut script = vec![("", 0), ("4242", 0)];
        for _ in 0..50 {
            script.push(("", 0));
        }
        let base = ScriptedEventChannel::new(events.clone(), "base", script);
        let base_ch: Arc<dyn ExecChannel> = base;
        let k8s_ch: Arc<dyn ExecChannel> = ScriptedEventChannel::new(events.clone(), "k8s", vec![]);
        let tunnels = MockTunnels::new(events.clone(), vec![]);
        let connector = ScriptedConnector::new(
            events.clone(),
            vec![],
            vec![Err("bridge handshake boom".to_string())],
        );

        let err = match establish_pod_mcp(
            &base_ch,
            &k8s_ch,
            tunnels.as_ref(),
            &connector,
            "env-1",
            "svc-1",
            None,
            18563,
            "tok123",
            &fast_pf_params(),
            &|_, _| {},
        )
        .await
        {
            Ok(_) => panic!("both-fail path must return Err"),
            Err(e) => e,
        };
        assert!(matches!(err, ManagerError::Attach(_)), "err: {err}");
        assert!(err.to_string().contains("arthas MCP 握手失败"), "err: {err}");

        let ev = events.lock().await.clone();
        // cleanup_partial_attach 经 k8s 通道发 stop（/api + token）
        assert!(
            ev.iter()
                .any(|e| e.starts_with("k8s:curl") && e.contains("http://127.0.0.1:18563/api")),
            "ev: {ev:?}"
        );
        assert!(
            ev.iter().any(|e| e.starts_with("k8s:curl") && e.contains("Bearer tok123")),
            "ev: {ev:?}"
        );
        // pf 已被清理
        assert!(ev.iter().any(|e| e.starts_with("base:kill 4242")), "ev: {ev:?}");
    }

    // ── T6 pf 隧道：stop 编排（run_production_stop）──

    fn pf_lease() -> PfLease {
        PfLease { pf_pid: 4242, host_port: 54321, local_port: 18080 }
    }

    fn channel_getter(
        events: EventLog,
        label: &'static str,
    ) -> impl Fn() -> Pin<Box<dyn Future<Output = Result<Arc<dyn ExecChannel>, String>> + Send>> + Sync {
        let ch: Arc<dyn ExecChannel> = ScriptedEventChannel::new(events, label, vec![]);
        move || {
            let ch = ch.clone();
            Box::pin(async move { Ok(ch) })
                as Pin<Box<dyn Future<Output = Result<Arc<dyn ExecChannel>, String>> + Send>>
        }
    }

    fn tunnel_stop_fn(
        events: EventLog,
        ok: bool,
    ) -> impl Fn(u16) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Sync {
        move |port: u16| {
            let events = events.clone();
            Box::pin(async move {
                events.lock().await.push(format!("tunnel-stop:{port}"));
                if ok {
                    Ok(())
                } else {
                    Err("tunnel dead".to_string())
                }
            }) as Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        }
    }

    #[tokio::test]
    async fn test_run_stop_tunnel_mode_full_order_no_exec_fallback() {
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        let client = MockArthasClient { events: events.clone() };
        let tunnels = MockTunnels::new(events.clone(), vec![]);
        let get_base = channel_getter(events.clone(), "base");
        let get_target = channel_getter(events.clone(), "target");
        let tunnel_stop = tunnel_stop_fn(events.clone(), true);

        run_production_stop(
            &client,
            tunnels.as_ref(),
            Some(&pf_lease()),
            "env-1",
            Some("svc-1"),
            18563,
            "tok123",
            &tunnel_stop,
            &get_base,
            &get_target,
        )
        .await;

        let ev = events.lock().await.clone();
        // 固定顺序：client shutdown → 隧道 stop → kill pf → tunnels.close
        let p = [
            ev.iter().position(|e| e == "client-shutdown").expect("shutdown"),
            ev.iter().position(|e| e == "tunnel-stop:18080").expect("tunnel stop"),
            ev.iter().position(|e| e.starts_with("base:kill 4242")).expect("kill pf"),
            ev.iter().position(|e| e == "tunnels-close:env-1/127.0.0.1/54321").expect("close tunnel"),
        ];
        assert!(p.windows(2).all(|w| w[0] < w[1]), "order: {ev:?}");
        // 隧道 stop 成功 → 不走 exec 通道兜底
        assert!(!ev.iter().any(|e| e.starts_with("target:")), "no exec fallback: {ev:?}");
    }

    #[tokio::test]
    async fn test_run_stop_tunnel_stop_fails_falls_back_to_exec() {
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        let client = MockArthasClient { events: events.clone() };
        let tunnels = MockTunnels::new(events.clone(), vec![]);
        let get_base = channel_getter(events.clone(), "base");
        let get_target = channel_getter(events.clone(), "target");
        let tunnel_stop = tunnel_stop_fn(events.clone(), false);

        run_production_stop(
            &client,
            tunnels.as_ref(),
            Some(&pf_lease()),
            "env-1",
            Some("svc-1"),
            18563,
            "tok123",
            &tunnel_stop,
            &get_base,
            &get_target,
        )
        .await;

        let ev = events.lock().await.clone();
        // 拆隧道照常（kill + close），随后 exec 通道 curl stop 兜底
        let p = [
            ev.iter().position(|e| e == "client-shutdown").expect("shutdown"),
            ev.iter().position(|e| e == "tunnel-stop:18080").expect("tunnel stop"),
            ev.iter().position(|e| e.starts_with("base:kill 4242")).expect("kill pf"),
            ev.iter().position(|e| e == "tunnels-close:env-1/127.0.0.1/54321").expect("close tunnel"),
            ev.iter().position(|e| e.starts_with("target:curl")).expect("exec fallback stop"),
        ];
        assert!(p.windows(2).all(|w| w[0] < w[1]), "order: {ev:?}");
        let stop_cmd = ev.iter().find(|e| e.starts_with("target:curl")).expect("stop cmd");
        assert!(stop_cmd.contains("http://127.0.0.1:18563/api"), "cmd: {stop_cmd}");
        assert!(stop_cmd.contains("Bearer tok123"), "cmd: {stop_cmd}");
    }

    #[tokio::test]
    async fn test_run_stop_without_pf_uses_exec_channel_only() {
        // VM / 桥降级模式（pf=None）：原 T5 行为——只走 exec 通道 stop
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        let client = MockArthasClient { events: events.clone() };
        let tunnels = MockTunnels::new(events.clone(), vec![]);
        let get_base = channel_getter(events.clone(), "base");
        let get_target = channel_getter(events.clone(), "target");
        let tunnel_stop = tunnel_stop_fn(events.clone(), true);

        run_production_stop(
            &client,
            tunnels.as_ref(),
            None,
            "env-1",
            None,
            18563,
            "tok123",
            &tunnel_stop,
            &get_base,
            &get_target,
        )
        .await;

        let ev = events.lock().await.clone();
        assert!(ev.iter().any(|e| e == "client-shutdown"), "ev: {ev:?}");
        assert!(ev.iter().any(|e| e.starts_with("target:curl")), "exec stop issued: {ev:?}");
        // 无 pf：不触隧道段
        assert!(!ev.iter().any(|e| e.starts_with("tunnel-stop:")), "ev: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("tunnels-close:")), "ev: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("base:")), "ev: {ev:?}");
    }

    #[tokio::test]
    async fn test_run_stop_base_channel_unavailable_still_stops_via_tunnel() {
        // 宿主机通道拿不到（SSH 断）：隧道 stop 走 TunnelManager 专属连接、不依赖
        // base——必须仍先试；成功 → arthas 已停，无需 exec 兜底。仍关隧道条目；
        // kill pf 失败由下次 attach 的 pkill 清残留兜底
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        let client = MockArthasClient { events: events.clone() };
        let tunnels = MockTunnels::new(events.clone(), vec![]);
        let get_base = || {
            Box::pin(async { Err("ssh down".to_string()) })
                as Pin<Box<dyn Future<Output = Result<Arc<dyn ExecChannel>, String>> + Send>>
        };
        let get_target = channel_getter(events.clone(), "target");
        let tunnel_stop = tunnel_stop_fn(events.clone(), true);

        run_production_stop(
            &client,
            tunnels.as_ref(),
            Some(&pf_lease()),
            "env-1",
            Some("svc-1"),
            18563,
            "tok123",
            &tunnel_stop,
            &get_base,
            &get_target,
        )
        .await;

        let ev = events.lock().await.clone();
        // 隧道 stop 仍被尝试（不依赖 base 通道）且成功 → 不走 exec 兜底
        let p = [
            ev.iter().position(|e| e == "client-shutdown").expect("shutdown"),
            ev.iter().position(|e| e == "tunnel-stop:18080").expect("tunnel stop"),
            ev.iter().position(|e| e == "tunnels-close:env-1/127.0.0.1/54321").expect("close tunnel"),
        ];
        assert!(p.windows(2).all(|w| w[0] < w[1]), "order: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("base:kill")), "no kill without base channel: {ev:?}");
        assert!(
            !ev.iter().any(|e| e.starts_with("target:")),
            "tunnel stop succeeded, no exec fallback: {ev:?}"
        );
    }

    #[tokio::test]
    async fn test_run_stop_base_unavailable_tunnel_stop_fails_falls_back_to_exec() {
        // 宿主机通道拿不到 + 隧道 stop 也失败 → exec 通道 curl stop 兜底仍要跑
        let events: EventLog = Arc::new(Mutex::new(Vec::new()));
        let client = MockArthasClient { events: events.clone() };
        let tunnels = MockTunnels::new(events.clone(), vec![]);
        let get_base = || {
            Box::pin(async { Err("ssh down".to_string()) })
                as Pin<Box<dyn Future<Output = Result<Arc<dyn ExecChannel>, String>> + Send>>
        };
        let get_target = channel_getter(events.clone(), "target");
        let tunnel_stop = tunnel_stop_fn(events.clone(), false);

        run_production_stop(
            &client,
            tunnels.as_ref(),
            Some(&pf_lease()),
            "env-1",
            Some("svc-1"),
            18563,
            "tok123",
            &tunnel_stop,
            &get_base,
            &get_target,
        )
        .await;

        let ev = events.lock().await.clone();
        let p = [
            ev.iter().position(|e| e == "client-shutdown").expect("shutdown"),
            ev.iter().position(|e| e == "tunnel-stop:18080").expect("tunnel stop"),
            ev.iter().position(|e| e == "tunnels-close:env-1/127.0.0.1/54321").expect("close tunnel"),
            ev.iter().position(|e| e.starts_with("target:curl")).expect("exec fallback stop"),
        ];
        assert!(p.windows(2).all(|w| w[0] < w[1]), "order: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("base:kill")), "no kill without base channel: {ev:?}");
        let stop_cmd = ev.iter().find(|e| e.starts_with("target:curl")).expect("stop cmd");
        assert!(stop_cmd.contains("http://127.0.0.1:18563/api"), "cmd: {stop_cmd}");
    }
}
