use super::channel::ExecChannel;
use super::k8s::K8sChannel;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("environment {env_id} not found")]
    EnvironmentNotFound { env_id: String },
    #[error("connection error: {0}")]
    Connection(String),
    #[error("transport not implemented: {0}")]
    TransportNotImplemented(String),
}

/// 连接池键：环境 + 可选 Pod/容器。pod=None 表示宿主机目标（VM 模式）。
/// 每个 key 一条独立 SSH 连接（spec：不按宿主机共享，语义可预测）。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TargetKey {
    pub env_id: String,
    pub pod: Option<String>,
    pub container: Option<String>,
}

impl TargetKey {
    pub fn base(env_id: &str) -> Self {
        Self { env_id: env_id.to_string(), pod: None, container: None }
    }

    pub fn k8s(env_id: &str, pod: &str, container: Option<&str>) -> Self {
        Self {
            env_id: env_id.to_string(),
            pod: Some(pod.to_string()),
            container: container.map(|s| s.to_string()),
        }
    }

    /// 工具参数 → key：空串视为未传
    pub fn from_parts(env_id: &str, pod: Option<&str>, container: Option<&str>) -> Self {
        Self {
            env_id: env_id.to_string(),
            pod: pod.filter(|p| !p.is_empty()).map(|s| s.to_string()),
            container: container.filter(|c| !c.is_empty()).map(|s| s.to_string()),
        }
    }
}

impl From<String> for TargetKey {
    fn from(env_id: String) -> Self {
        TargetKey::base(&env_id)
    }
}

struct PooledConnection {
    channel: Arc<dyn ExecChannel>,
    last_used: Instant,
}

/// 后台执行 disconnect（fire-and-forget）。
/// disconnect 可能等待 transport 内部连接锁（长命令持有时无界阻塞），
/// 绝不能在持有 pool 锁的路径上 await。TCP 连接短暂残留可接受（russh Drop 也会清理）。
fn spawn_disconnect(key: TargetKey, channel: Arc<dyn ExecChannel>) {
    tokio::spawn(async move {
        tracing::debug!(target = ?key, "disconnecting ssh connection in background");
        channel.disconnect().await;
        tracing::debug!(target = ?key, "ssh connection disconnected");
    });
}

pub struct ExecChannelPool {
    connections: HashMap<TargetKey, PooledConnection>,
}

impl ExecChannelPool {
    pub fn new() -> Self {
        Self { connections: HashMap::new() }
    }

    /// 按目标获取或建连（pod=None = 宿主机 VM 模式）。缓存命中即复用（刷新 last_used）。
    pub async fn get_or_create(
        &mut self,
        environment_id: &str,
        pod: Option<&str>,
        container: Option<&str>,
        pool: &sqlx::SqlitePool,
    ) -> Result<Arc<dyn ExecChannel>, PoolError> {
        let key = TargetKey::from_parts(environment_id, pod, container);
        if let Some(conn) = self.connections.get_mut(&key) {
            conn.last_used = Instant::now();
            return Ok(conn.channel.clone());
        }

        let env = fetch_environment(pool, environment_id).await?;
        let channel = build_transport(environment_id, &env, pod, container)?;

        channel
            .connect()
            .await
            .map_err(|e| PoolError::Connection(e.to_string()))?;

        self.connections.insert(
            key,
            PooledConnection { channel: channel.clone(), last_used: Instant::now() },
        );
        Ok(channel)
    }

    /// 测试与内部注入用：直接放入一条已建好的 channel（String = 宿主机 base key）
    pub async fn insert_channel(&mut self, key: impl Into<TargetKey>, channel: Arc<dyn ExecChannel>) {
        self.connections.insert(key.into(), PooledConnection { channel, last_used: Instant::now() });
    }

    /// 清理空闲超时连接。返回清理数量。
    /// disconnect 在后台 task 中执行（fire-and-forget）：transport 内部锁可能被长命令持有，
    /// 若在持有 pool 锁时 await disconnect，会阻塞所有环境的连接获取。
    pub async fn cleanup_idle(&mut self, idle_timeout: Duration) -> usize {
        let stale: Vec<TargetKey> = self
            .connections
            .iter()
            .filter(|(_, c)| c.last_used.elapsed() > idle_timeout)
            .map(|(k, _)| k.clone())
            .collect();
        for key in &stale {
            if let Some(conn) = self.connections.remove(key) {
                tracing::info!(target = ?key, idle_secs = conn.last_used.elapsed().as_secs(), "closing idle ssh connection");
                spawn_disconnect(key.clone(), conn.channel);
            }
        }
        stale.len()
    }

    /// 断开该环境的全部连接（base + 所有 Pod 目标）。环境删除/配置变更用。
    pub async fn disconnect(&mut self, environment_id: &str) {
        let stale: Vec<TargetKey> = self
            .connections
            .keys()
            .filter(|k| k.env_id == environment_id)
            .cloned()
            .collect();
        for key in stale {
            if let Some(conn) = self.connections.remove(&key) {
                tracing::info!(env_id = %key.env_id, "closing ssh connection (env-wide disconnect)");
                spawn_disconnect(key, conn.channel);
            }
        }
    }

    /// 断开单个目标连接（超时杀进程路径：不波及同环境其他目标的会话）
    pub async fn disconnect_target(&mut self, key: &TargetKey) {
        if let Some(conn) = self.connections.remove(key) {
            spawn_disconnect(key.clone(), conn.channel);
        }
    }

    pub async fn disconnect_all(&mut self) {
        for (key, conn) in self.connections.drain() {
            spawn_disconnect(key, conn.channel);
        }
    }

    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    #[cfg(test)]
    pub fn mark_last_used_for_test(&mut self, key: &TargetKey, at: Instant) {
        if let Some(conn) = self.connections.get_mut(key) {
            conn.last_used = at;
        }
    }

    #[cfg(test)]
    pub async fn get_or_create_unchecked_for_test(&mut self, key: &TargetKey) -> Arc<dyn ExecChannel> {
        self.connections.get(key).map(|c| c.channel.clone()).unwrap()
    }
}

impl Default for ExecChannelPool {
    fn default() -> Self {
        Self::new()
    }
}

pub struct EnvironmentInfo {
    /// 提示性元数据（spec：不再参与通道分发，保留供 UI 展示/Agent 发现顺序建议）
    #[allow(dead_code)]
    pub transport_type: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub auth_type: Option<String>,
    pub private_key_path: Option<String>,
    /// 默认凭证 id（env_credentials.id）；无凭证行时 None（退回 environments 列）
    pub default_cred_id: Option<String>,
}

/// 构造宿主机 SSH 通道（具体类型）：隧道转发（open_direct_tcpip）等
/// 需要 SshTransport 专属能力的调用方使用。
pub fn build_ssh_transport(
    environment_id: &str,
    env: &EnvironmentInfo,
) -> Result<super::ssh::SshTransport, PoolError> {
    let auth = super::ssh::SshAuth::from_row(
        env.auth_type.as_deref().unwrap_or("private_key"),
        env.private_key_path.as_deref(),
    )
    .ok_or_else(|| {
        PoolError::TransportNotImplemented(format!(
            "invalid auth config for environment {environment_id}"
        ))
    })?;
    Ok(match &env.default_cred_id {
        Some(cred_id) => super::ssh::SshTransport::with_cred(
            environment_id,
            env.host.as_deref().unwrap_or_default(),
            env.port.unwrap_or(22),
            env.user.as_deref().unwrap_or_default(),
            auth,
            cred_id,
        ),
        None => super::ssh::SshTransport::new(
            environment_id,
            env.host.as_deref().unwrap_or_default(),
            env.port.unwrap_or(22),
            env.user.as_deref().unwrap_or_default(),
            auth,
        ),
    })
}

/// 按 pod 参数分发通道构造：pod=None → 纯 SshTransport（宿主机 VM 模式）；
/// pod=Some → SshTransport 外包 K8sChannel（命令透明转发进容器）。
pub fn build_transport(
    environment_id: &str,
    env: &EnvironmentInfo,
    pod: Option<&str>,
    container: Option<&str>,
) -> Result<Arc<dyn ExecChannel>, PoolError> {
    let transport = build_ssh_transport(environment_id, env)?;
    // spec：按 pod 参数分发。transport_type 仅提示性元数据（UI 展示/Agent 发现顺序建议）。
    Ok(match pod {
        None => Arc::new(transport),
        Some(pod) => Arc::new(K8sChannel {
            base: Arc::new(transport),
            pod: pod.to_string(),
            container: container.map(|s| s.to_string()),
        }),
    })
}

pub async fn fetch_environment(
    pool: &sqlx::SqlitePool,
    environment_id: &str,
) -> Result<EnvironmentInfo, PoolError> {
    let row: Option<(
        Option<String>,
        Option<i64>,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT e.host, e.port, e.user, e.transport_type, \
                COALESCE(c.auth_type, e.auth_type), \
                COALESCE(c.private_key_path, e.private_key_path), \
                COALESCE(c.username, e.user), \
                c.id \
         FROM environments e \
         LEFT JOIN env_credentials c ON c.environment_id = e.id AND c.is_default = 1 \
         WHERE e.id = ?",
    )
    .bind(environment_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| PoolError::Connection(e.to_string()))?;

    let row = row.ok_or(PoolError::EnvironmentNotFound {
        env_id: environment_id.to_string(),
    })?;

    Ok(EnvironmentInfo {
        transport_type: row.3,
        host: row.0,
        port: row.1.map(|p| p as u16),
        user: row.6,
        auth_type: row.4,
        private_key_path: row.5,
        default_cred_id: row.7,
    })
}

/// k8s 目标超时补刀（best-effort）：断开 SSH 只能杀死宿主机上的 kubectl，
/// 容器内进程可能存活（CRI exec 服务端语义）。独立建连（不走池、不持池锁）
/// 在容器内 `pkill -f <命令签名>`；VM 目标（pod=None）no-op。失败仅告警。
pub fn spawn_timeout_kill(db: sqlx::SqlitePool, target: TargetKey, command: String) {
    let Some(pod) = target.pod.clone() else { return };
    tokio::spawn(async move {
        let env = match fetch_environment(&db, &target.env_id).await {
            Ok(env) => env,
            Err(e) => {
                tracing::warn!(env_id = %target.env_id, error = %e, "timeout kill: fetch environment failed");
                return;
            }
        };
        let channel = match build_transport(&target.env_id, &env, Some(&pod), target.container.as_deref()) {
            Ok(ch) => ch,
            Err(e) => {
                tracing::warn!(env_id = %target.env_id, error = %e, "timeout kill: build transport failed");
                return;
            }
        };
        if let Err(e) = channel.connect().await {
            tracing::warn!(env_id = %target.env_id, error = %e, "timeout kill: reconnect failed");
            return;
        }
        let kill_cmd = format!(
            "pkill -f {}",
            super::ssh::shell_quote_single(&super::k8s::pkill_pattern(&command))
        );
        match channel.run(&kill_cmd).await {
            Ok(out) => tracing::info!(env_id = %target.env_id, pod = %pod, exit_code = out.exit_code, "timeout kill executed"),
            Err(e) => tracing::warn!(env_id = %target.env_id, error = %e, "timeout kill: pkill failed (best-effort)"),
        }
        channel.disconnect().await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::channel::{ExecChannel, ExecOutput};
    use async_trait::async_trait;

    struct MockChannel;

    #[async_trait]
    impl ExecChannel for MockChannel {
        async fn run(&self, _cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            Ok(ExecOutput { stdout: String::new(), stderr: String::new(), exit_code: 0 })
        }
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
        async fn disconnect(&self) {}
        async fn is_alive(&self) -> bool { true }
    }

    async fn insert_test_environment(pool: &sqlx::SqlitePool, id: &str, name: &str) {
        sqlx::query(
            "INSERT INTO environments (id, name, host, port, user, transport_type, auth_type, created_at) \
             VALUES (?, ?, '10.0.0.1', 22, 'root', 'ssh', 'password', '2026-01-01T00:00:00Z')",
        )
        .bind(id).bind(name).execute(pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_disconnect_removes_connection() {
        let mut pool = ExecChannelPool::new();
        pool.insert_channel("env-1".to_string(), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;

        pool.disconnect("env-1").await;
        assert_eq!(pool.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_disconnect_nonexistent_is_noop() {
        let mut pool = ExecChannelPool::new();
        pool.disconnect("nonexistent").await;
        assert_eq!(pool.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_disconnect_all_removes_all() {
        let mut pool = ExecChannelPool::new();
        pool.insert_channel("env-1".to_string(), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        pool.insert_channel("env-2".to_string(), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;

        pool.disconnect_all().await;
        assert_eq!(pool.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_channel_trait_exposes_is_alive() {
        let ch: Arc<dyn ExecChannel> = Arc::new(MockChannel);
        assert!(ch.is_alive().await);
    }

    #[tokio::test]
    async fn test_get_or_create_caches_by_environment_id() {
        let tmp = tempfile::tempdir().unwrap();
        let db_pool = crate::infra::db::init(tmp.path().join("friday.db")).await.unwrap();
        insert_test_environment(&db_pool, "env-1", "prod").await;

        let mut pool = ExecChannelPool::new();
        // 第一次：缓存未命中 → 注入 channel 后复用
        pool.insert_channel("env-1".to_string(), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        let ch = pool.get_or_create("env-1", None, None, &db_pool).await.unwrap();
        assert!(ch.run("echo").await.is_ok());
        // 第二次：命中同一缓存（同一 Arc）
        let ch2 = pool.get_or_create("env-1", None, None, &db_pool).await.unwrap();
        assert_eq!(pool.connection_count(), 1);
        assert!(std::sync::Arc::ptr_eq(&ch, &ch2));
    }

    #[tokio::test]
    async fn test_get_or_create_unknown_environment_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let db_pool = crate::infra::db::init(tmp.path().join("friday.db")).await.unwrap();

        let mut pool = ExecChannelPool::new();
        let result = pool.get_or_create("no-such-env", None, None, &db_pool).await;
        assert!(matches!(result, Err(PoolError::EnvironmentNotFound { .. })));
    }

    struct SlowDisconnectChannel;

    #[async_trait]
    impl ExecChannel for SlowDisconnectChannel {
        async fn run(&self, _cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            Ok(ExecOutput { stdout: String::new(), stderr: String::new(), exit_code: 0 })
        }
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
        async fn disconnect(&self) {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
        async fn is_alive(&self) -> bool { true }
    }

    #[tokio::test]
    async fn test_cleanup_idle_returns_promptly_when_disconnect_blocks() {
        let mut pool = ExecChannelPool::new();
        pool.insert_channel("env-slow".to_string(), Arc::new(SlowDisconnectChannel) as Arc<dyn ExecChannel>).await;
        pool.mark_last_used_for_test(&TargetKey::base("env-slow"), std::time::Instant::now() - std::time::Duration::from_secs(660));

        let start = std::time::Instant::now();
        let removed = pool.cleanup_idle(std::time::Duration::from_secs(600)).await;
        assert_eq!(removed, 1);
        // cleanup must return well before the 3s disconnect completes
        assert!(start.elapsed() < std::time::Duration::from_secs(1), "cleanup_idle blocked on disconnect: {:?}", start.elapsed());
        // entry must be gone from the pool immediately
        assert_eq!(pool.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_idle_cleanup_removes_stale_connections() {
        let mut pool = ExecChannelPool::new();
        pool.insert_channel("env-1".to_string(), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        assert_eq!(pool.connection_count(), 1);

        pool.mark_last_used_for_test(&TargetKey::base("env-1"), std::time::Instant::now() - std::time::Duration::from_secs(660));
        let removed = pool.cleanup_idle(std::time::Duration::from_secs(600)).await;
        assert_eq!(removed, 1);
        assert_eq!(pool.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_idle_cleanup_keeps_recent_connections() {
        let mut pool = ExecChannelPool::new();
        pool.insert_channel("env-1".to_string(), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;

        let removed = pool.cleanup_idle(std::time::Duration::from_secs(600)).await;
        assert_eq!(removed, 0);
        assert_eq!(pool.connection_count(), 1);
    }

    #[tokio::test]
    async fn test_fetch_environment_prefers_default_credential() {
        let tmp = tempfile::tempdir().unwrap();
        let db_pool = crate::infra::db::init(tmp.path().join("friday.db")).await.unwrap();
        insert_test_environment(&db_pool, "env-1", "prod").await;
        // 环境行：user=root/password；凭证行：svcapp/private_key
        sqlx::query(
            "INSERT INTO env_credentials (id, environment_id, username, auth_type, private_key_path, is_default, created_at) \
             VALUES ('c1', 'env-1', 'svcapp', 'private_key', '~/.ssh/svc', 1, '2026-01-01T00:00:00Z')",
        )
        .execute(&db_pool).await.unwrap();

        let info = fetch_environment(&db_pool, "env-1").await.unwrap();
        assert_eq!(info.user.as_deref(), Some("svcapp"));
        assert_eq!(info.auth_type.as_deref(), Some("private_key"));
        assert_eq!(info.private_key_path.as_deref(), Some("~/.ssh/svc"));
        assert_eq!(info.default_cred_id.as_deref(), Some("c1"));

        let transport = build_ssh_transport("env-1", &info).unwrap();
        assert_eq!(transport.user, "svcapp");
        assert_eq!(transport.cred_id_as_ref(), Some("c1"));
    }

    #[tokio::test]
    async fn test_fetch_environment_falls_back_to_env_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let db_pool = crate::infra::db::init(tmp.path().join("friday.db")).await.unwrap();
        insert_test_environment(&db_pool, "env-1", "prod").await;

        let info = fetch_environment(&db_pool, "env-1").await.unwrap();
        assert_eq!(info.user.as_deref(), Some("root"));
        assert_eq!(info.auth_type.as_deref(), Some("password"));
        assert!(info.default_cred_id.is_none());
    }

    fn db_noop() -> sqlx::SqlitePool {
        sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap()
    }

    #[tokio::test]
    async fn test_k8s_target_keyed_independently_from_base() {
        let mut pool = ExecChannelPool::new();
        pool.insert_channel(TargetKey::base("env-1"), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        pool.insert_channel(TargetKey::k8s("env-1", "pod-a", None), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        assert_eq!(pool.connection_count(), 2);
        // 命中各自缓存（不会互相顶掉）
        let _ = pool.get_or_create("env-1", None, None, &db_noop()).await;
        assert_eq!(pool.connection_count(), 2);
    }

    #[tokio::test]
    async fn test_disconnect_env_removes_base_and_k8s_keys() {
        let mut pool = ExecChannelPool::new();
        pool.insert_channel(TargetKey::base("env-1"), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        pool.insert_channel(TargetKey::k8s("env-1", "pod-a", Some("c1")), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        pool.insert_channel(TargetKey::base("env-2"), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        pool.disconnect("env-1").await;
        assert_eq!(pool.connection_count(), 1, "only env-2 survives");
    }

    #[tokio::test]
    async fn test_disconnect_target_removes_only_that_key() {
        let mut pool = ExecChannelPool::new();
        pool.insert_channel(TargetKey::base("env-1"), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        pool.insert_channel(TargetKey::k8s("env-1", "pod-a", None), Arc::new(MockChannel) as Arc<dyn ExecChannel>).await;
        pool.disconnect_target(&TargetKey::k8s("env-1", "pod-a", None)).await;
        assert_eq!(pool.connection_count(), 1, "base key survives");
    }

    #[test]
    fn test_from_parts_normalizes_empty_strings() {
        let k = TargetKey::from_parts("e", Some(""), Some(""));
        assert_eq!(k, TargetKey::base("e"));
    }
}
