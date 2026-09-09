//! k8s Pod MCP 正向隧道（T6 主路径）。
//!
//! 隧道链：Friday 本地端口 L ──TunnelManager direct-tcpip──▶ 宿主机
//! 127.0.0.1:P ──kubectl port-forward（宿主机 nohup 长驻进程）──▶ Pod {mcp_port}。
//! 主路径成功后 MCP 走 rmcp 原生 reqwest transport 打 http://127.0.0.1:{L}/mcp
//! （client::connect_arthas_client_native），不再依赖容器内 curl；任一步失败
//! 降级 exec HTTP 桥（bridge.rs，容器需 curl）。
//!
//! 已知假设：kubectl port-forward 不带 -n，依赖 kubeconfig context 的当前
//! namespace（与 kubectl exec / kubectl get pod 行为一致，见 attach_arthas_in_pod）。

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;

use crate::exec::channel::{ExecChannel, ExecOutput};
use crate::exec::ssh::shell_quote_single;
use crate::exec::tunnel::TunnelManager;

/// 宿主机侧 pf 监听地址（--address 127.0.0.1；TunnelManager 的 direct-tcpip
/// 从宿主机回环进入）
pub const TUNNEL_REMOTE_HOST: &str = "127.0.0.1";
/// Friday 本地隧道监听地址（TunnelManager 绑 127.0.0.1 临时端口）
pub const TUNNEL_LOCAL_HOST: &str = "127.0.0.1";

/// pf 日志轮询预算 / 轮询间隔 / 本地健康检查预算（生产值；测试用快参数）
pub const PF_LOG_POLL_BUDGET: Duration = Duration::from_secs(10);
pub const PF_LOG_POLL_INTERVAL: Duration = Duration::from_millis(500);
pub const PF_HEALTH_BUDGET: Duration = Duration::from_secs(2);

/// TunnelManager 的 arthas 侧最小抽象（AttachDeps 注入 seam，照 AttachFactory
/// 的注入模式；测试用 mock 替身）。open 返回 Friday 本地监听端口 L。
#[async_trait]
pub trait ArthasTunnels: Send + Sync {
    async fn open(&self, env_id: &str, remote_host: &str, remote_port: u16) -> Result<u16, String>;
    async fn close(&self, env_id: &str, remote_host: &str, remote_port: u16);
}

#[async_trait]
impl ArthasTunnels for TunnelManager {
    async fn open(&self, env_id: &str, remote_host: &str, remote_port: u16) -> Result<u16, String> {
        self.open(env_id, remote_host, remote_port)
            .await
            .map(|lease| lease.local_port)
            .map_err(|e| e.to_string())
    }

    async fn close(&self, env_id: &str, remote_host: &str, remote_port: u16) {
        self.close(env_id, remote_host, remote_port).await;
    }
}

/// 一条已建立的 pf 隧道（生命周期挂 arthas stop handle：所有会话销毁路径
/// 统一经 stop() 释放——close / LRU 逐出 / reaper / invalidate / close_for_environment）
#[derive(Clone, Debug)]
pub struct PfLease {
    /// 宿主机 kubectl port-forward 进程 PID
    pub pf_pid: u32,
    /// 宿主机侧端口 P（kubectl 自选随机端口，从 pf 日志解析）
    pub host_port: u16,
    /// Friday 本地隧道端口 L（TunnelManager 分配）
    pub local_port: u16,
}

/// pf 隧道建立参数（预算/间隔；生产 = Default，测试用快参数）
#[derive(Clone, Copy, Debug)]
pub struct PfTunnelParams {
    /// pf 日志轮询预算（等 Forwarding 行）
    pub log_budget: Duration,
    /// pf 日志轮询间隔
    pub log_interval: Duration,
    /// 本地隧道健康检查预算（TCP connect 127.0.0.1:L）
    pub health_budget: Duration,
}

impl Default for PfTunnelParams {
    fn default() -> Self {
        Self {
            log_budget: PF_LOG_POLL_BUDGET,
            log_interval: PF_LOG_POLL_INTERVAL,
            health_budget: PF_HEALTH_BUDGET,
        }
    }
}

// ─────────────────────────── 纯函数（命令构造 / 解析） ───────────────────────────

/// 清残留 pf（best-effort）：pattern 按 (pod, container, mcp_port) 唯一定位本
/// 会话的 pf（同 Pod 多会话的 mcp_port 不同，不会误杀）；pkill_pattern 转义
/// ERE 元字符 + 首字符 bracket 化自排除。Friday 崩溃后宿主机残留的 pf 由
/// 下次 attach 的本命令清理。container 参与构造时用 argv 形式（无 shell 引号
/// ——pkill -f 匹配的是 /proc/{pid}/cmdline）。
pub fn pf_cleanup_command(pod: &str, container: Option<&str>, mcp_port: u16) -> String {
    let ctr = container.map(|c| format!("-c {c} ")).unwrap_or_default();
    let cmdline = format!("kubectl port-forward pod/{pod} {ctr}--address 127.0.0.1 0:{mcp_port}");
    format!(
        "pkill -f {} || true",
        shell_quote_single(&crate::exec::k8s::pkill_pattern(&cmdline))
    )
}

/// pf 日志路径（宿主机 /tmp/friday-tools）。pod 名为 DNS-1123 字符集
/// （小写字母数字 + 连字符），可安全作路径段；mcp_port 参与命名保证同 Pod
/// 多会话日志互不覆盖。
pub fn pf_log_path(pod: &str, mcp_port: u16) -> String {
    format!("/tmp/friday-tools/pf-{pod}-{mcp_port}.log")
}

/// 启动 pf：nohup 后台驻留 + echo $! 拿 PID。--address 127.0.0.1 只绑宿主机
/// 回环（TunnelManager 的 direct-tcpip 从宿主机 127.0.0.1:P 进入）；0:{mcp_port}
/// 让 kubectl 自选宿主机随机端口（从日志解析）。stdin 接 /dev/null 防 SSH 会话
/// 关闭后交互等待（对齐 attach_command 的 nohup 模式）；mkdir -p 兜底日志目录
/// （k8s 装备流程通常已建，幂等）。
pub fn pf_start_command(pod: &str, container: Option<&str>, mcp_port: u16) -> String {
    let ctr_flag = container
        .map(|c| format!("-c {} ", shell_quote_single(c)))
        .unwrap_or_default();
    format!(
        "mkdir -p /tmp/friday-tools && nohup kubectl port-forward pod/{} {ctr_flag}--address 127.0.0.1 0:{mcp_port} < /dev/null > {} 2>&1 & echo $!",
        shell_quote_single(pod),
        shell_quote_single(&pf_log_path(pod, mcp_port)),
    )
}

/// 解析 pf 启动输出（echo $!）→ 宿主机 pf 进程 PID
pub fn parse_pf_pid(stdout: &str) -> Option<u32> {
    stdout.trim().parse().ok()
}

/// 读 pf 日志（best-effort：日志文件未创建时 cat 失败 → 空输出，由轮询重试）
pub fn pf_log_read_command(pod: &str, mcp_port: u16) -> String {
    format!(
        "cat {} 2>/dev/null || true",
        shell_quote_single(&pf_log_path(pod, mcp_port))
    )
}

/// 解析 pf 日志：提取 "Forwarding from 127.0.0.1:{P} -> {mcp_port}" 的宿主机
/// 侧端口 P。多行取首个匹配；无匹配 / 垃圾行 → None。匹配用 split_once
/// 而非前缀匹配（容忍 klog 时间戳等行前缀）；--address 127.0.0.1 下 kubectl
/// 只打 IPv4 行，[::1] 行不匹配。
pub fn forwarded_from_port(log: &str) -> Option<u16> {
    log.lines().find_map(|line| {
        let rest = line.split_once("Forwarding from 127.0.0.1:")?.1;
        let (port, _target) = rest.split_once(" -> ")?;
        port.trim().parse::<u16>().ok()
    })
}

/// kill 宿主机 pf 进程（best-effort：进程已死时 kill 非零 → || true 吞掉）
pub fn kill_pf_command(pid: u32) -> String {
    format!("kill {pid} 2>/dev/null || true")
}

// ─────────────────────────── 编排 ───────────────────────────

/// 带超时的远端执行（秒级命令；对齐 attach.rs run_with_timeout 语义）
async fn run_timed(
    channel: &dyn ExecChannel,
    cmd: &str,
    secs: u64,
) -> Result<ExecOutput, String> {
    let out = match tokio::time::timeout(Duration::from_secs(secs), channel.run(cmd)).await {
        Err(_) => return Err(format!("远端命令执行超时（{secs}s）: {cmd}")),
        Ok(Err(e)) => return Err(format!("远端命令执行失败: {e}（命令: {cmd}）")),
        Ok(Ok(out)) => out,
    };
    if !out.stderr.trim().is_empty() {
        tracing::debug!(cmd, stderr = %out.stderr, "pf tunnel remote command stderr");
    }
    Ok(out)
}

/// 建立 pf 隧道：① 清残留 ② 起 pf ③ 轮询 pf 日志拿宿主机端口 P
/// ④ TunnelManager forward 到宿主机 127.0.0.1:P 得本地端口 L ⑤ 本地 TCP
/// 健康检查。任一步失败：kill pf + 关已建立的隧道（幂等）后报错，
/// 调用方降级 exec HTTP 桥。
pub async fn establish_pf_tunnel(
    base: &dyn ExecChannel,
    tunnels: &dyn ArthasTunnels,
    env_id: &str,
    pod: &str,
    container: Option<&str>,
    mcp_port: u16,
    params: &PfTunnelParams,
) -> Result<PfLease, String> {
    // ① 清残留（best-effort：上次 Friday 崩溃遗留的同目标 pf）
    if let Err(e) = run_timed(base, &pf_cleanup_command(pod, container, mcp_port), 15).await {
        tracing::warn!(pod, mcp_port, error = %e, "清残留 kubectl port-forward 失败（best-effort 继续）");
    }

    // ② 起 pf + 解析 PID
    let out = run_timed(base, &pf_start_command(pod, container, mcp_port), 15).await?;
    if out.exit_code != 0 {
        return Err(format!(
            "kubectl port-forward 启动失败（exit {}）: {}",
            out.exit_code,
            out.stderr.trim()
        ));
    }
    let pf_pid = parse_pf_pid(&out.stdout)
        .ok_or_else(|| format!("kubectl port-forward 启动输出无 PID: {:?}", out.stdout))?;

    // ③ 轮询 pf 日志等 Forwarding 行（kubectl 的启动错误只写日志不写 stdout）
    let host_port = match wait_pf_forwarding(base, pod, mcp_port, params.log_budget, params.log_interval).await {
        Ok(port) => port,
        Err(e) => {
            teardown_pf(base, tunnels, env_id, pf_pid, None).await;
            return Err(e);
        }
    };

    // ④ TunnelManager 建本地转发（Friday L → 宿主机 127.0.0.1:P）
    let local_port = match tunnels.open(env_id, TUNNEL_REMOTE_HOST, host_port).await {
        Ok(port) => port,
        Err(e) => {
            let err = format!("TunnelManager 建立到宿主机 {TUNNEL_REMOTE_HOST}:{host_port} 的隧道失败: {e}");
            teardown_pf(base, tunnels, env_id, pf_pid, Some(host_port)).await;
            return Err(err);
        }
    };

    // ⑤ 本地健康检查：TCP connect 127.0.0.1:L（几次重试，预算内）
    if let Err(e) = wait_local_port(local_port, params.health_budget).await {
        teardown_pf(base, tunnels, env_id, pf_pid, Some(host_port)).await;
        return Err(e);
    }

    tracing::info!(env_id, pod, pf_pid, host_port, local_port, mcp_port, "pf tunnel established");
    Ok(PfLease { pf_pid, host_port, local_port })
}

/// 轮询 pf 日志直至出现 Forwarding 行（预算内）。日志一直不出现 = pf 起不来
/// （宿主机 kubectl 缺失 / Pod 不存在 / API 不可达——错误都写进日志文件）。
async fn wait_pf_forwarding(
    base: &dyn ExecChannel,
    pod: &str,
    mcp_port: u16,
    budget: Duration,
    interval: Duration,
) -> Result<u16, String> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let out = run_timed(base, &pf_log_read_command(pod, mcp_port), 15).await?;
        if let Some(port) = forwarded_from_port(&out.stdout) {
            return Ok(port);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "kubectl port-forward 在 {:?} 内未就绪（日志 {} 无 Forwarding 行）。\
                 可能原因：宿主机 kubectl 缺失、Pod 不存在或 API Server 不可达",
                budget,
                pf_log_path(pod, mcp_port),
            ));
        }
        tokio::time::sleep(interval).await;
    }
}

/// 本地健康检查：TCP connect 127.0.0.1:{local_port}（几次重试，预算内）。
/// 通过 = TunnelManager 本地监听可达（整链最终可达性由 MCP 握手判定）。
async fn wait_local_port(local_port: u16, budget: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let attempt = tokio::time::timeout(
            Duration::from_millis(500),
            tokio::net::TcpStream::connect((TUNNEL_LOCAL_HOST, local_port)),
        )
        .await;
        if let Ok(Ok(_)) = attempt {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "pf 隧道本地健康检查失败：127.0.0.1:{local_port} 在 {budget:?} 内不可连"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 拆除 pf 隧道资源（best-effort）：kill 宿主机 pf 进程；host_port=Some 时再关
/// TunnelManager 隧道。顺序固定：kill pf 先于关隧道（反序时 close 失败会泄漏
/// 宿主机 pf 进程——泄漏的隧道条目可由 close_all_for_env 收尾，pf 进程无人回收）。
pub async fn teardown_pf(
    base: &dyn ExecChannel,
    tunnels: &dyn ArthasTunnels,
    env_id: &str,
    pf_pid: u32,
    host_port: Option<u16>,
) {
    match base.run(&kill_pf_command(pf_pid)).await {
        Ok(out) if out.exit_code == 0 => tracing::info!(pf_pid, "kubectl port-forward killed"),
        Ok(out) => tracing::warn!(
            pf_pid, exit_code = out.exit_code, stderr = %out.stderr,
            "kill kubectl port-forward 返回非零（best-effort）"
        ),
        Err(e) => tracing::warn!(pf_pid, error = %e, "kill kubectl port-forward 失败（best-effort）"),
    }
    if let Some(host_port) = host_port {
        tunnels.close(env_id, TUNNEL_REMOTE_HOST, host_port).await;
    }
}

/// 经隧道原生 HTTP 打 arthas stop 端点（POST /api，Bearer token）——替换容器内
/// curl 依赖（隧道模式下容器无需 curl）。任意 2xx 即成功。
pub async fn http_stop_via_tunnel(local_port: u16, token: &str) -> Result<(), String> {
    let url = format!("http://{TUNNEL_LOCAL_HOST}:{local_port}/api");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("构建 HTTP client 失败: {e}"))?;
    let resp = client
        .post(&url)
        .bearer_auth(token)
        .json(&serde_json::json!({"action": "exec", "command": "stop"}))
        .send()
        .await
        .map_err(|e| format!("隧道 stop 请求失败（{url}）: {e}"))?;
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        Err(format!("隧道 stop 返回 HTTP {status}: {body}"))
    }
}

/// 隧道会话停止编排（ProductionStopHandle::stop 的隧道段）：
/// ① 经隧道原生 HTTP stop arthas（tunnel_stop 注入：生产 = http_stop_via_tunnel，
///    测试 = mock）② kill pf ③ tunnels.close。返回 ① 是否成功（false → 调用方
/// 回落 exec 通道 curl stop）。kill pf 固定先于 tunnels.close（见 teardown_pf）。
pub async fn teardown_pod_tunnel_session(
    base: &dyn ExecChannel,
    tunnels: &dyn ArthasTunnels,
    tunnel_stop: &(dyn Fn(u16) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Sync),
    env_id: &str,
    pf: &PfLease,
) -> bool {
    let stopped = match tunnel_stop(pf.local_port).await {
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
    teardown_pf(base, tunnels, env_id, pf.pf_pid, Some(pf.host_port)).await;
    stopped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Arc;

    type EventLog = Arc<tokio::sync::Mutex<Vec<String>>>;

    /// 事件记录 + 脚本化输出通道：每次 run 把 "label:cmd" 记入事件日志
    /// （跨组件顺序断言用），响应按脚本顺序返回
    struct ScriptedEventChannel {
        events: EventLog,
        label: &'static str,
        script: tokio::sync::Mutex<VecDeque<(&'static str, i32)>>,
    }

    impl ScriptedEventChannel {
        fn new(events: EventLog, label: &'static str, script: Vec<(&'static str, i32)>) -> Arc<Self> {
            Arc::new(Self {
                events,
                label,
                script: tokio::sync::Mutex::new(script.into_iter().collect()),
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
        opens: tokio::sync::Mutex<VecDeque<Result<u16, String>>>,
    }

    impl MockTunnels {
        fn new(events: EventLog, opens: Vec<Result<u16, String>>) -> Arc<Self> {
            Arc::new(Self {
                events,
                opens: tokio::sync::Mutex::new(opens.into_iter().collect()),
            })
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

    /// 绑一个本地监听端口（保持存活供健康检查连通）
    async fn live_local_port() -> (u16, tokio::net::TcpListener) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (port, listener)
    }

    /// 绑后立即释放的端口（健康检查应不可连）
    async fn dead_local_port() -> u16 {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        port
    }

    fn fast_params() -> PfTunnelParams {
        PfTunnelParams {
            log_budget: Duration::from_millis(300),
            log_interval: Duration::from_millis(20),
            health_budget: Duration::from_millis(400),
        }
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

    // ── 纯函数 ──

    #[test]
    fn test_pf_cleanup_command_shape_and_session_scoped_pattern() {
        let cmd = pf_cleanup_command("svc-1", None, 18563);
        // bracket 化首字符（自排除）+ 精确到 (pod, mcp_port)，不误杀同 Pod 其他会话
        assert!(cmd.contains("pkill -f '[k]ubectl port-forward pod/svc-1 --address 127\\.0\\.0\\.1 0:18563'"), "cmd: {cmd}");
        assert!(cmd.ends_with("' || true"), "cmd: {cmd}");
        // container 参与 pattern（argv 形式，无 shell 引号）
        let cmd_c = pf_cleanup_command("svc-1", Some("main"), 18563);
        assert!(cmd_c.contains("port-forward pod/svc-1 -c main --address"), "cmd: {cmd_c}");
        // 不同 mcp_port 的 pattern 不同（同 Pod 多会话互不干扰）
        let cmd_other = pf_cleanup_command("svc-1", None, 18564);
        assert_ne!(cmd, cmd_other);
    }

    #[test]
    fn test_pf_start_command_shape() {
        let cmd = pf_start_command("svc-1", None, 18563);
        assert!(cmd.contains("nohup kubectl port-forward pod/'svc-1'"), "cmd: {cmd}");
        assert!(cmd.contains("--address 127.0.0.1"), "cmd: {cmd}");
        assert!(cmd.contains(" 0:18563"), "cmd: {cmd}");
        assert!(cmd.contains("< /dev/null"), "cmd: {cmd}");
        assert!(cmd.contains("> '/tmp/friday-tools/pf-svc-1-18563.log' 2>&1"), "cmd: {cmd}");
        assert!(cmd.ends_with("& echo $!"), "cmd: {cmd}");
        assert!(cmd.starts_with("mkdir -p /tmp/friday-tools &&"), "cmd: {cmd}");
        // container 旗标
        let cmd_c = pf_start_command("svc-1", Some("main"), 18563);
        assert!(cmd_c.contains("-c 'main' "), "cmd: {cmd_c}");
        // pod 名注入防护：单引号转义
        let evil = pf_start_command("x'; rm -rf /; '", None, 18563);
        assert!(evil.contains(r"'\''"), "must escape quotes: {evil}");
    }

    #[test]
    fn test_parse_pf_pid() {
        assert_eq!(parse_pf_pid("4242\n"), Some(4242));
        assert_eq!(parse_pf_pid("  4242  "), Some(4242));
        assert_eq!(parse_pf_pid(""), None);
        assert_eq!(parse_pf_pid("garbage"), None);
        assert_eq!(parse_pf_pid("-1"), None);
    }

    #[test]
    fn test_forwarded_from_port() {
        assert_eq!(forwarded_from_port("Forwarding from 127.0.0.1:54321 -> 18563"), Some(54321));
        // 多行日志取首个匹配
        let log = "I0912 01:02:03.456   123 main.go:123] Forwarding from 127.0.0.1:54321 -> 18563\nForwarding from [::1]:54321 -> 18563\n";
        assert_eq!(forwarded_from_port(log), Some(54321));
        // 垃圾行 / 空 → None
        assert_eq!(forwarded_from_port("some random log line"), None);
        assert_eq!(forwarded_from_port(""), None);
        // 只有 IPv6 行（--address 127.0.0.1 下不出现，防御）→ None
        assert_eq!(forwarded_from_port("Forwarding from [::1]:54321 -> 18563"), None);
        // 端口越界 → None
        assert_eq!(forwarded_from_port("Forwarding from 127.0.0.1:99999 -> 18563"), None);
    }

    #[test]
    fn test_kill_pf_command() {
        assert_eq!(kill_pf_command(4242), "kill 4242 2>/dev/null || true");
    }

    // ── establish_pf_tunnel 编排 ──

    #[tokio::test]
    async fn test_establish_pf_tunnel_happy_path() {
        let events: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let base = ScriptedEventChannel::new(events.clone(), "base", pf_base_script());
        let (live_port, _guard) = live_local_port().await;
        let tunnels = MockTunnels::new(events.clone(), vec![Ok(live_port)]);

        let lease = establish_pf_tunnel(base.as_ref(), tunnels.as_ref(), "env-1", "svc-1", None, 18563, &fast_params())
            .await
            .unwrap();
        assert_eq!(lease.pf_pid, 4242);
        assert_eq!(lease.host_port, 54321);
        assert_eq!(lease.local_port, live_port);

        let ev = events.lock().await.clone();
        // 顺序：清残留 → 起 pf → 日志轮询 ×2 → tunnels.open
        let idx = |needle: &str| ev.iter().position(|e| e.contains(needle)).expect(needle);
        assert!(idx("pkill -f") < idx("nohup kubectl port-forward"), "ev: {ev:?}");
        assert!(idx("nohup kubectl port-forward") < idx("cat '/tmp/friday-tools/pf-svc-1-18563.log'"), "ev: {ev:?}");
        assert!(ev.iter().any(|e| e == "tunnels-open:env-1/127.0.0.1/54321"), "ev: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("tunnels-close:")), "tunnel must stay open: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("base:kill")), "pf must stay alive: {ev:?}");
    }

    #[tokio::test]
    async fn test_establish_pf_tunnel_start_failure_no_pf_to_kill() {
        let events: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        // pf 启动 exit 1（如 mkdir 失败）
        let base = ScriptedEventChannel::new(events.clone(), "base", vec![("", 0), ("", 1)]);
        let tunnels = MockTunnels::new(events.clone(), vec![]);

        let err = establish_pf_tunnel(base.as_ref(), tunnels.as_ref(), "env-1", "svc-1", None, 18563, &fast_params())
            .await
            .unwrap_err();
        assert!(err.contains("kubectl port-forward 启动失败"), "err: {err}");
        let ev = events.lock().await.clone();
        // 启动失败时拿不到 PID：无 kill、无 open
        assert!(!ev.iter().any(|e| e.starts_with("base:kill")), "ev: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("tunnels-open:")), "ev: {ev:?}");
    }

    #[tokio::test]
    async fn test_establish_pf_tunnel_log_timeout_kills_pf() {
        let events: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        // 日志一直不出现 Forwarding（快预算内耗尽）
        let mut script = vec![("", 0), ("4242", 0)];
        for _ in 0..50 {
            script.push(("kubectl: pod not found", 0));
        }
        let base = ScriptedEventChannel::new(events.clone(), "base", script);
        let tunnels = MockTunnels::new(events.clone(), vec![]);

        let err = establish_pf_tunnel(base.as_ref(), tunnels.as_ref(), "env-1", "svc-1", None, 18563, &fast_params())
            .await
            .unwrap_err();
        assert!(err.contains("未就绪"), "err: {err}");
        let ev = events.lock().await.clone();
        assert!(ev.iter().any(|e| e.starts_with("base:kill 4242")), "pf must be killed: {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("tunnels-open:")), "ev: {ev:?}");
    }

    #[tokio::test]
    async fn test_establish_pf_tunnel_open_failure_kills_pf() {
        let events: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let base = ScriptedEventChannel::new(events.clone(), "base", pf_base_script());
        let tunnels = MockTunnels::new(events.clone(), vec![Err("ssh refused".to_string())]);

        let err = establish_pf_tunnel(base.as_ref(), tunnels.as_ref(), "env-1", "svc-1", None, 18563, &fast_params())
            .await
            .unwrap_err();
        assert!(err.contains("TunnelManager"), "err: {err}");
        assert!(err.contains("ssh refused"), "err: {err}");
        let ev = events.lock().await.clone();
        assert!(ev.iter().any(|e| e.starts_with("base:kill 4242")), "pf must be killed: {ev:?}");
    }

    #[tokio::test]
    async fn test_establish_pf_tunnel_health_failure_tears_down() {
        let events: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let base = ScriptedEventChannel::new(events.clone(), "base", pf_base_script());
        let dead = dead_local_port().await;
        let tunnels = MockTunnels::new(events.clone(), vec![Ok(dead)]);

        let err = establish_pf_tunnel(base.as_ref(), tunnels.as_ref(), "env-1", "svc-1", None, 18563, &fast_params())
            .await
            .unwrap_err();
        assert!(err.contains("健康检查失败"), "err: {err}");
        let ev = events.lock().await.clone();
        // 降级路径：kill pf + 关隧道（kill 在 close 前）
        let kill_idx = ev.iter().position(|e| e.starts_with("base:kill 4242")).expect("kill");
        let close_idx = ev
            .iter()
            .position(|e| e == "tunnels-close:env-1/127.0.0.1/54321")
            .expect("close");
        assert!(kill_idx < close_idx, "kill must precede close: {ev:?}");
    }

    // ── teardown_pf / teardown_pod_tunnel_session ──

    #[tokio::test]
    async fn test_teardown_pf_kills_then_closes_with_params() {
        let events: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let base = ScriptedEventChannel::new(events.clone(), "base", vec![]);
        let tunnels = MockTunnels::new(events.clone(), vec![]);

        teardown_pf(base.as_ref(), tunnels.as_ref(), "env-1", 4242, Some(54321)).await;
        let ev = events.lock().await.clone();
        let kill_idx = ev
            .iter()
            .position(|e| e == "base:kill 4242 2>/dev/null || true")
            .expect("kill cmd");
        let close_idx = ev
            .iter()
            .position(|e| e == "tunnels-close:env-1/127.0.0.1/54321")
            .expect("close");
        assert!(kill_idx < close_idx, "kill must precede close: {ev:?}");
        // host_port=None（隧道未建立）：只 kill 不 close
        let events2: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let base2 = ScriptedEventChannel::new(events2.clone(), "base", vec![]);
        let tunnels2 = MockTunnels::new(events2.clone(), vec![]);
        teardown_pf(base2.as_ref(), tunnels2.as_ref(), "env-1", 4242, None).await;
        let ev2 = events2.lock().await.clone();
        assert!(ev2.iter().any(|e| e.starts_with("base:kill 4242")), "ev: {ev2:?}");
        assert!(!ev2.iter().any(|e| e.starts_with("tunnels-close:")), "ev: {ev2:?}");
    }

    #[tokio::test]
    async fn test_teardown_pod_tunnel_session_order_success() {
        let events: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let base = ScriptedEventChannel::new(events.clone(), "base", vec![]);
        let tunnels = MockTunnels::new(events.clone(), vec![]);
        let tunnel_stop = {
            let events = events.clone();
            move |port: u16| {
                let events = events.clone();
                Box::pin(async move {
                    events.lock().await.push(format!("tunnel-stop:{port}"));
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
            }
        };
        let pf = PfLease { pf_pid: 4242, host_port: 54321, local_port: 18080 };

        let stopped = teardown_pod_tunnel_session(base.as_ref(), tunnels.as_ref(), &tunnel_stop, "env-1", &pf).await;
        assert!(stopped, "tunnel stop must be reported as success");
        let ev = events.lock().await.clone();
        // 固定顺序：tunnel stop → kill pf → close tunnel
        let p1 = ev.iter().position(|e| e == "tunnel-stop:18080").expect("stop");
        let p2 = ev.iter().position(|e| e.starts_with("base:kill 4242")).expect("kill");
        let p3 = ev.iter().position(|e| e == "tunnels-close:env-1/127.0.0.1/54321").expect("close");
        assert!(p1 < p2 && p2 < p3, "order: {ev:?}");
    }

    #[tokio::test]
    async fn test_teardown_pod_tunnel_session_stop_fail_still_tears_down() {
        let events: EventLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let base = ScriptedEventChannel::new(events.clone(), "base", vec![]);
        let tunnels = MockTunnels::new(events.clone(), vec![]);
        let tunnel_stop = {
            let events = events.clone();
            move |port: u16| {
                let events = events.clone();
                Box::pin(async move {
                    events.lock().await.push(format!("tunnel-stop:{port}"));
                    Err("connection refused".to_string())
                }) as Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
            }
        };
        let pf = PfLease { pf_pid: 4242, host_port: 54321, local_port: 18080 };

        let stopped = teardown_pod_tunnel_session(base.as_ref(), tunnels.as_ref(), &tunnel_stop, "env-1", &pf).await;
        assert!(!stopped, "tunnel stop failure must be reported");
        let ev = events.lock().await.clone();
        // stop 失败仍要 kill pf + close（顺序不变）
        let p1 = ev.iter().position(|e| e == "tunnel-stop:18080").expect("stop");
        let p2 = ev.iter().position(|e| e.starts_with("base:kill 4242")).expect("kill");
        let p3 = ev.iter().position(|e| e == "tunnels-close:env-1/127.0.0.1/54321").expect("close");
        assert!(p1 < p2 && p2 < p3, "order: {ev:?}");
    }
}
