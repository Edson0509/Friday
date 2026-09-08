# K8s 容器工具适配 · Phase 1（通道基础设施 + 发现 + JVM 工具容器化 + 环境类型）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Friday 诊断工具支持 Kubernetes 容器目标：SSH 到宿主机后经 kubectl exec 进 Pod，对 Pod 内 JVM 执行 jstat/jcmd 类诊断，并支持动态发现 Pod、按环境类型管理宿主机。

**Architecture:** 新增 `K8sChannel`（装饰器，包装到宿主机的 `SshTransport`，实现 `ExecChannel`），连接池 key 从 `env_id` 扩为 `(env_id, pod, container)`，`build_transport` **按 pod 参数分发**（transport_type 仅作提示性元数据，不做硬分发）。所有远端工具 schema 增加可选 `pod`/`container` 参数，`resolve_environment` 一处统一解析。JDK 装备在容器内走 `K8sJdkPackage`（musl 保险丝、容器自带工具优先、`--no-same-owner` 解压、`chgrp ossgroup` 属组修正、固定目录 `/opt/log/dump/heapdump/friday-tools`）。超时杀进程对容器目标做显式补刀（独立连接 `pkill -f`）。

**Tech Stack:** Rust（russh 既有栈，无新依赖）、React/TypeScript 前端、SQLite。

**Spec:** `docs/superpowers/specs/2026-09-08-k8s-container-tooling-design.md`（已评审通过）

**Phase 边界（本计划不做，Phase 2 做）：** K8sChannel::download 两跳拉回 + TransferManager 集成、heap dump/JFR 的 Pod 内路径与空间检查、Arthas 容器化（正向隧道 + attach + 会话 key）、file_upload/file_download 工具容器行为。本计划中 heap_dump/jfr/file_transfer 只做**参数贯通**（schema + resolve 透传），保证编译与 VM 行为不变。

---

## 文件结构

| 文件 | 动作 | 职责 |
|---|---|---|
| `src-tauri/src/exec/k8s.rs` | 新建 | K8sChannel（run/upload + 命令包装 + pkill 转义纯函数） |
| `src-tauri/src/exec/mod.rs` | 修改 | 声明 `pub mod k8s;` |
| `src-tauri/src/exec/pool.rs` | 修改 | TargetKey、get_or_create 4 参、build_transport 分发、disconnect_target、spawn_timeout_kill |
| `src-tauri/src/exec/tunnel.rs` | 修改 | build_transport 调用点 + 一处测试替换 |
| `src-tauri/src/transfer/mod.rs` | 修改 | dedicated_channel 的 build_transport 调用点 |
| `src-tauri/src/tools/builtin/jvm/core.rs` | 修改 | resolve_environment 5 参、exec_jdk_command target 化、超时补刀 |
| `src-tauri/src/tools/builtin/jvm/jdk_cache.rs` | 修改 | cache_key 复合键函数 |
| `src-tauri/src/tools/builtin/jvm/simple.rs` | 修改 | pod/container 参数 + schema |
| `src-tauri/src/tools/builtin/jvm/processes.rs` | 修改 | 同上 |
| `src-tauri/src/tools/builtin/jvm/heap_dump.rs` | 修改 | 参数贯通（仅编译/透传） |
| `src-tauri/src/tools/builtin/jfr/mod.rs` | 修改 | 参数贯通（仅编译/透传） |
| `src-tauri/src/tools/builtin/run_command.rs` | 修改 | pod/container + 超时补刀 |
| `src-tauri/src/provision/package.rs` | 修改 | ProvisionContext 增 remote_tools_dir |
| `src-tauri/src/provision/jdk.rs` | 修改 | 目录参数化（VM 行为不变） |
| `src-tauri/src/provision/k8s.rs` | 新建 | K8sJdkPackage + POD_TOOLS_DIR/POD_DUMP_DIR 常量 |
| `src-tauri/src/provision/mod.rs` | 修改 | 声明 `pub mod k8s;` |
| `src-tauri/src/tools/builtin/ensure_tool.rs` | 修改 | pod 分发 + 缓存复合键 |
| `src-tauri/src/tools/builtin/k8s.rs` | 新建 | k8s_find_pods 工具 |
| `src-tauri/src/tools/builtin/mod.rs` | 修改 | 声明 `pub mod k8s;` |
| `src-tauri/src/tools/category.rs` | 修改 | 增 `K8s` 变体 |
| `src-tauri/src/app/environments.rs` | 修改 | EnvironmentRow 增 transport_type |
| `src-tauri/src/app/env_save.rs` | 修改 | save_environment_with_transport |
| `src-tauri/src/lib.rs` | 修改 | 注册 k8s_find_pods |
| `src/lib/types.ts` | 修改 | ToolCategory 增 "k8s"、EnvironmentRow 增 transport_type |
| `src/lib/ipc.ts` / `src/store/envStore.ts` | 修改 | saveEnvironment 增 transportType |
| `src/components/environments/EnvironmentDialog.tsx` | 修改 | 环境类型选择 |
| `src/components/tools/ToolsPanel.tsx` | 修改 | K8s 分组 |

**约定**：Rust 测试命令 `cargo test --manifest-path src-tauri/Cargo.toml`（下文简写 `cargo test`）；前端 `pnpm typecheck`。提交信息沿用仓库风格（无 Conventional Commits 强制）。日志规范：新代码错误路径必须有 `tracing::warn!/error!`。

---

### Task 1: `exec/k8s.rs` — K8sChannel 骨架（run + 生命周期 + 纯函数）

**Files:**
- Create: `src-tauri/src/exec/k8s.rs`
- Modify: `src-tauri/src/exec/mod.rs`（加一行）

- [ ] **Step 1.1：写文件头 + 纯函数 + 失败测试**

新建 `src-tauri/src/exec/k8s.rs`，先只放纯函数与测试（struct 留到 Step 1.3）：

```rust
//! K8s 容器通道：包装到宿主机的 SSH 通道，把命令透明转发进 Pod。
//! 分发规则：pod 参数存在 → K8sChannel（spec：2026-09-08-k8s-container-tooling-design.md）。

use async_trait::async_trait;
use std::sync::Arc;

use super::channel::{ExecChannel, ExecOutput};
use super::ssh::shell_quote_single;

/// 宿主机侧暂存目录（两跳传输中转）
pub const STAGING_DIR: &str = "/tmp/friday-tools/staging";

/// Pod 内 Friday 工具目录（用户指定：该目录不会触发 ephemeral-storage 驱逐）
pub const POD_TOOLS_DIR: &str = "/opt/log/dump/heapdump/friday-tools";

/// Pod 内 dump 产物目录（Phase 2 的 heap dump / JFR 落这里）
pub const POD_DUMP_DIR: &str = "/opt/log/dump/heapdump";

/// 文件属组要求：非 ossgroup 无法被目标 JVM 用户使用（用户约束，exec 用户 = ossadm 非 root）
pub const OSS_GROUP: &str = "ossgroup";

/// 构造 `kubectl exec ... -- sh -c ...`（纯函数）。
/// container 缺省时省略 -c（kubectl 默认容器 = spec 第一个容器）。
/// 整条命令会再经 SshTransport 的 bash -lc 包装，内嵌单引号由 shell_quote_single 转义。
pub fn wrap_exec_command(pod: &str, container: Option<&str>, cmd: &str) -> String {
    let ctr = match container {
        Some(c) => format!("-c {} ", shell_quote_single(c)),
        None => String::new(),
    };
    format!(
        "kubectl exec {}{} -- sh -c {}",
        ctr,
        shell_quote_single(pod),
        shell_quote_single(cmd)
    )
}

/// pkill -f 的 pattern：转义 ERE 元字符（pkill -f 按扩展正则匹配整条命令行，
/// 命令串里的 `. + ( )` 等字面量必须转义，防止误杀无关进程）
pub fn pkill_pattern(command: &str) -> String {
    let mut out = String::with_capacity(command.len() + 8);
    for c in command.chars() {
        if matches!(
            c,
            '\\' | '.' | '^' | '$' | '|' | '?' | '*' | '+' | '(' | ')' | '[' | ']' | '{' | '}'
        ) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Pod 内路径校验：必须绝对路径且不含 NUL
pub fn validate_pod_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/') {
        return Err(format!("pod path must be absolute: {path:?}"));
    }
    if path.contains('\0') {
        return Err("pod path must not contain NUL".to_string());
    }
    Ok(())
}
```

文件尾先加测试模块（此时只测纯函数）：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_exec_command_without_container() {
        assert_eq!(
            wrap_exec_command("svc-abc", None, "jstat -gcutil 1"),
            "kubectl exec 'svc-abc' -- sh -c 'jstat -gcutil 1'"
        );
    }

    #[test]
    fn test_wrap_exec_command_with_container() {
        assert_eq!(
            wrap_exec_command("svc-abc", Some("main"), "ps -ef"),
            "kubectl exec -c 'main' 'svc-abc' -- sh -c 'ps -ef'"
        );
    }

    #[test]
    fn test_wrap_exec_command_escapes_inner_quotes() {
        let wrapped = wrap_exec_command("p", None, "grep 'x' /log/a");
        assert!(wrapped.contains(r"-- sh -c 'grep '\''x'\'' /log/a'"), "got: {wrapped}");
    }

    #[test]
    fn test_pkill_pattern_escapes_regex_metachars() {
        assert_eq!(pkill_pattern("/jdk-21.0.11+9/bin/jcmd 1 GC.heap_dump"), "/jdk-21\\.0\\.11\\+9/bin/jcmd 1 GC\\.heap_dump");
    }

    #[test]
    fn test_validate_pod_path() {
        assert!(validate_pod_path("/opt/log/dump/heapdump/x").is_ok());
        assert!(validate_pod_path("relative/x").is_err());
        assert!(validate_pod_path("/a\0b").is_err());
    }
}
```

- [ ] **Step 1.2：跑测试确认通过（纯函数一次成形，无失败态）**

Run: `cargo test --manifest-path src-tauri/Cargo.toml exec::k8s`
Expected: `test result: ok. 5 passed`

- [ ] **Step 1.3：加 K8sChannel struct + run/生命周期 + 委托测试**

在 `validate_pod_path` 之后追加：

```rust
/// K8s 容器通道：run 语义 = 在 Pod 容器内执行（sh -c，busybox 无 bash）；
/// upload = 两跳注入（Task 2 实现）。连接生命周期完全委托 base SSH 通道。
pub struct K8sChannel {
    pub base: Arc<dyn ExecChannel>,
    pub pod: String,
    pub container: Option<String>,
}

impl K8sChannel {
    pub fn ctr_flag(&self) -> String {
        match &self.container {
            Some(c) => format!("-c {} ", shell_quote_single(c)),
            None => String::new(),
        }
    }
}

#[async_trait]
impl ExecChannel for K8sChannel {
    async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
        let wrapped = wrap_exec_command(&self.pod, self.container.as_deref(), cmd);
        tracing::debug!(pod = %self.pod, "k8s exec");
        self.base.run(&wrapped).await
    }

    async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.base.connect().await
    }

    async fn disconnect(&self) {
        self.base.disconnect().await;
    }

    async fn is_alive(&self) -> bool {
        self.base.is_alive().await
    }
}
```

测试模块追加：

```rust
    mod run_tests {
        use super::super::*;
        use crate::exec::channel::ExecChannel;
        use async_trait::async_trait;

        /// 记录所有 run 调用的 mock base
        struct RecordingBase {
            runs: tokio::sync::Mutex<Vec<String>>,
        }

        #[async_trait]
        impl ExecChannel for RecordingBase {
            async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
                self.runs.lock().await.push(cmd.to_string());
                Ok(ExecOutput { stdout: String::new(), stderr: String::new(), exit_code: 0 })
            }
            async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
            async fn disconnect(&self) {}
            async fn is_alive(&self) -> bool { true }
        }

        #[tokio::test]
        async fn test_run_delegates_wrapped_command_to_base() {
            let base = Arc::new(RecordingBase { runs: tokio::sync::Mutex::new(Vec::new()) });
            let ch = K8sChannel { base: base.clone(), pod: "svc-1".into(), container: None };
            ch.run("jstat -gcutil 7").await.unwrap();
            let runs = base.runs.lock().await;
            assert_eq!(runs[0], "kubectl exec 'svc-1' -- sh -c 'jstat -gcutil 7'");
        }

        #[tokio::test]
        async fn test_run_with_container_flag() {
            let base = Arc::new(RecordingBase { runs: tokio::sync::Mutex::new(Vec::new()) });
            let ch = K8sChannel { base: base.clone(), pod: "svc-1".into(), container: Some("main".into()) };
            ch.run("ps -ef").await.unwrap();
            let runs = base.runs.lock().await;
            assert_eq!(runs[0], "kubectl exec -c 'main' 'svc-1' -- sh -c 'ps -ef'");
        }
    }
```

- [ ] **Step 1.4：注册模块**

`src-tauri/src/exec/mod.rs` 追加一行（保持字母序，放在 `channel` 后）：

```rust
pub mod k8s;
```

- [ ] **Step 1.5：跑测试 + 提交**

Run: `cargo test --manifest-path src-tauri/Cargo.toml exec::k8s`
Expected: `test result: ok. 7 passed`

```bash
git add src-tauri/src/exec/k8s.rs src-tauri/src/exec/mod.rs
git commit -m "feat: K8sChannel skeleton wrapping ssh base with kubectl exec"
```

---

### Task 2: `K8sChannel::upload` — 两跳注入 + 属组修正

**Files:**
- Modify: `src-tauri/src/exec/k8s.rs`

- [ ] **Step 2.1：写失败测试（upload 两跳编排）**

测试模块追加（新 `upload_tests` mod）：

```rust
    mod upload_tests {
        use super::super::*;
        use crate::exec::channel::ExecChannel;
        use async_trait::async_trait;
        use std::path::Path;

        /// 可编排响应的 base：run 第 n 次返回脚本第 n 条 (stdout, exit_code)；
        /// upload 记录 (local, remote)。默认 run 返回 exit 0。
        struct ScriptedBase {
            script: std::sync::Mutex<std::collections::VecDeque<(String, i32)>>,
            runs: tokio::sync::Mutex<Vec<String>>,
            uploads: tokio::sync::Mutex<Vec<(std::path::PathBuf, String)>>,
        }

        impl ScriptedBase {
            fn new(script: Vec<(&str, i32)>) -> Self {
                Self {
                    script: std::sync::Mutex::new(
                        script.into_iter().map(|(s, c)| (s.to_string(), c)).collect(),
                    ),
                    runs: tokio::sync::Mutex::new(Vec::new()),
                    uploads: tokio::sync::Mutex::new(Vec::new()),
                }
            }
        }

        #[async_trait]
        impl ExecChannel for ScriptedBase {
            async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
                self.runs.lock().await.push(cmd.to_string());
                let (stdout, exit_code) = self
                    .script
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or((String::new(), 0));
                Ok(ExecOutput { stdout, stderr: String::new(), exit_code })
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

        fn chan(script: Vec<(&str, i32)>) -> (Arc<ScriptedBase>, K8sChannel) {
            let base = Arc::new(ScriptedBase::new(script));
            let ch = K8sChannel { base: base.clone(), pod: "svc-1".into(), container: None };
            (base, ch)
        }

        #[tokio::test]
        async fn test_upload_happy_path_two_legs_and_chgrp() {
            let (base, ch) = chan(vec![]);
            ch.upload(Path::new("/local/jdk.tar.gz"), "/opt/log/dump/heapdump/friday-tools/jdk.tar.gz")
                .await
                .unwrap();
            // leg A：SFTP 到宿主机 staging（路径含随机前缀）
            let uploads = base.uploads.lock().await;
            assert_eq!(uploads.len(), 1);
            assert!(uploads[0].1.starts_with("/tmp/friday-tools/staging/"), "staging: {}", uploads[0].1);
            assert!(uploads[0].1.ends_with("-jdk.tar.gz"));
            // leg B：kubectl exec -i + host 侧重定向 + 父目录创建
            let runs = base.runs.lock().await;
            let host_leg = runs.iter().find(|c| c.contains("kubectl exec -i")).expect("host leg");
            assert!(host_leg.contains("< "), "host stdin redirect: {host_leg}");
            assert!(host_leg.contains("cat > '/opt/log/dump/heapdump/friday-tools/jdk.tar.gz'"), "{host_leg}");
            assert!(host_leg.contains("mkdir -p '/opt/log/dump/heapdump/friday-tools'"), "{host_leg}");
            // 属组修正走 kubectl exec（容器内，不是宿主机）
            let chgrp = runs.iter().find(|c| c.contains("chgrp ossgroup")).expect("chgrp leg");
            assert!(chgrp.contains("kubectl exec"), "chgrp must run inside pod: {chgrp}");
            assert!(chgrp.contains("chmod g+r"));
            // staging 清理
            assert!(runs.iter().any(|c| c.contains("rm -f /tmp/friday-tools/staging/")));
        }

        #[tokio::test]
        async fn test_upload_host_leg_failure_cleans_remote_and_errors() {
            // 脚本顺序：①mkdir staging ②kubectl exec -i（exit 1）③rm staging ④rm remote（补刀清理）
            let (base, ch) = chan(vec![("", 0), ("", 1), ("", 0), ("", 0)]);
            let err = ch
                .upload(Path::new("/local/x"), "/opt/log/dump/heapdump/friday-tools/x")
                .await
                .unwrap_err();
            assert!(err.to_string().contains("kubectl exec -i failed"), "err: {err}");
            let runs = base.runs.lock().await;
            assert!(runs.iter().any(|c| c.contains("rm -f '/opt/log/dump/heapdump/friday-tools/x'")), "remote cleanup: {runs:?}");
        }

        #[tokio::test]
        async fn test_upload_chgrp_failure_cleans_remote_and_errors() {
            // ①mkdir ②kubectl -i ok ③rm staging ④chgrp(exit 1) ⑤rm remote
            let (base, ch) = chan(vec![("", 0), ("", 0), ("", 0), ("", 1), ("", 0)]);
            let err = ch
                .upload(Path::new("/local/x"), "/opt/log/dump/heapdump/friday-tools/x")
                .await
                .unwrap_err();
            assert!(err.to_string().contains("chgrp ossgroup failed"), "err: {err}");
            let runs = base.runs.lock().await;
            assert!(runs.iter().any(|c| c.contains("rm -f '/opt/log/dump/heapdump/friday-tools/x'")), "remote cleanup: {runs:?}");
        }

        #[tokio::test]
        async fn test_upload_rejects_relative_path() {
            let (_base, ch) = chan(vec![]);
            let err = ch.upload(Path::new("/local/x"), "relative/x").await.unwrap_err();
            assert!(err.to_string().contains("absolute"), "err: {err}");
        }
    }
```

- [ ] **Step 2.2：跑测试确认失败**

Run: `cargo test --manifest-path src-tauri/Cargo.toml exec::k8s::upload_tests`
Expected: FAIL（`upload` 走 trait 默认实现，返回 "upload not implemented"）

- [ ] **Step 2.3：实现 upload**

在 `impl ExecChannel for K8sChannel` 中 `run` 之后追加：

```rust
    async fn upload(
        &self,
        local: &std::path::Path,
        remote_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        validate_pod_path(remote_path)?;
        let basename = remote_path.rsplit('/').next().unwrap_or("file");
        let staging = format!("{}/{}-{}", STAGING_DIR, uuid::Uuid::new_v4(), basename);

        // ① 宿主机 staging 目录
        self.base.run(&format!("mkdir -p {}", shell_quote_single(STAGING_DIR))).await?;

        // ② leg A：SFTP → 宿主机 staging
        self.base.upload(local, &staging).await?;

        // ③ leg B：kubectl exec -i 注入容器。stdin 重定向发生在宿主机 bash 上，
        //    数据不流经 Friday 内存（dump 级大文件安全）；容器内依赖仅 sh + cat。
        let parent = match remote_path.rfind('/') {
            Some(0) => "/".to_string(),
            Some(i) => remote_path[..i].to_string(),
            None => "/".to_string(),
        };
        let inner = format!(
            "mkdir -p {} && cat > {}",
            shell_quote_single(&parent),
            shell_quote_single(remote_path)
        );
        let host_cmd = format!(
            "kubectl exec -i {}{} -- sh -c {} < {}",
            self.ctr_flag(),
            shell_quote_single(&self.pod),
            shell_quote_single(&inner),
            shell_quote_single(&staging)
        );
        let out = self.base.run(&host_cmd).await?;

        // staging 清理（成败都清）
        let _ = self
            .base
            .run(&format!("rm -f {}", shell_quote_single(&staging)))
            .await;

        if out.exit_code != 0 {
            tracing::warn!(pod = %self.pod, remote_path, exit_code = out.exit_code, stderr = %out.stderr, "k8s upload: kubectl exec -i failed");
            // 半截文件兜底清理（经 kubectl exec，容器内）
            let _ = self.run(&format!("rm -f {}", shell_quote_single(remote_path))).await;
            return Err(format!(
                "k8s upload: kubectl exec -i failed (exit {}): {}",
                out.exit_code, out.stderr
            )
            .into());
        }

        // ④ 属组修正（spec：chgrp 失败 = 上传失败，清理目标文件）
        let q = shell_quote_single(remote_path);
        let fix = format!("chgrp {OSS_GROUP} {q} && chmod g+r {q}");
        let gout = self.run(&fix).await?;
        if gout.exit_code != 0 {
            tracing::warn!(pod = %self.pod, remote_path, stderr = %gout.stderr, "k8s upload: chgrp failed");
            let _ = self.run(&format!("rm -f {q}")).await;
            return Err(format!(
                "k8s upload: chgrp {OSS_GROUP} failed (exec 用户可能不在 {OSS_GROUP} 组): {}",
                gout.stderr
            )
            .into());
        }
        Ok(())
    }
```

注意：`uuid` crate 已是依赖（pool/env_save 在用），无需加 Cargo.toml。`download` 保持 trait 默认未实现（Phase 2）。

- [ ] **Step 2.4：跑测试确认通过**

Run: `cargo test --manifest-path src-tauri/Cargo.toml exec::k8s`
Expected: `test result: ok. 11 passed`

- [ ] **Step 2.5：提交**

```bash
git add src-tauri/src/exec/k8s.rs
git commit -m "feat: K8sChannel two-leg upload with chgrp ossgroup enforcement"
```

---

### Task 3: Pool `TargetKey` + 按 pod 参数分发 + 超时补刀

**Files:**
- Modify: `src-tauri/src/exec/pool.rs`
- Modify: `src-tauri/src/exec/tunnel.rs:103`、`src-tauri/src/transfer/mod.rs:69`、`src-tauri/src/tools/builtin/jvm/core.rs:20`、`src-tauri/src/tools/builtin/run_command.rs:76`、`src-tauri/src/tools/builtin/ensure_tool.rs:56`（一行调用点）

- [ ] **Step 3.1：写失败测试（pool 测试模块追加）**

`pool.rs` 测试模块追加：

```rust
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

    #[tokio::test]
    async fn test_from_parts_normalizes_empty_strings() {
        let k = TargetKey::from_parts("e", Some(""), Some(""));
        assert_eq!(k, TargetKey::base("e"));
    }
```

`db_noop` 辅助（get_or_create 命中缓存路径不查库，但签名要 pool；用惰性连接即可）：

```rust
    fn db_noop() -> sqlx::SqlitePool {
        sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap()
    }
```

- [ ] **Step 3.2：跑测试确认编译失败**

Run: `cargo test --manifest-path src-tauri/Cargo.toml exec::pool`
Expected: 编译错误（`TargetKey` 未定义）

- [ ] **Step 3.3：实现 pool 改造**

`pool.rs` 顶部 use 改为：

```rust
use super::channel::ExecChannel;
use super::k8s::K8sChannel;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
```

在 `PoolError` 之后加 TargetKey：

```rust
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
```

`spawn_disconnect` 改签名（key 进日志）：

```rust
fn spawn_disconnect(key: TargetKey, channel: Arc<dyn ExecChannel>) {
    tokio::spawn(async move {
        tracing::debug!(target = ?key, "disconnecting ssh connection in background");
        channel.disconnect().await;
        tracing::debug!(target = ?key, "ssh connection disconnected");
    });
}
```

`ExecChannelPool`：

```rust
pub struct ExecChannelPool {
    connections: HashMap<TargetKey, PooledConnection>,
}
```

`get_or_create` 整体替换：

```rust
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
```

`insert_channel` 改为（`From<String>` 让既有测试调用不变）：

```rust
    /// 测试与内部注入用：直接放入一条已建好的 channel（String = 宿主机 base key）
    pub async fn insert_channel(&mut self, key: impl Into<TargetKey>, channel: Arc<dyn ExecChannel>) {
        self.connections.insert(key.into(), PooledConnection { channel, last_used: Instant::now() });
    }
```

`disconnect` 整体替换 + 新增 `disconnect_target`：

```rust
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
```

`mark_last_used_for_test` / `get_or_create_unchecked_for_test` 改 key 型：

```rust
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
```

`build_transport` 整体替换（**按 pod 参数分发，不再看 transport_type**）：

```rust
pub fn build_transport(
    environment_id: &str,
    env: &EnvironmentInfo,
    pod: Option<&str>,
    container: Option<&str>,
) -> Result<Arc<dyn super::channel::ExecChannel>, PoolError> {
    let auth = super::ssh::SshAuth::from_row(
        env.auth_type.as_deref().unwrap_or("private_key"),
        env.private_key_path.as_deref(),
    )
    .ok_or_else(|| {
        PoolError::TransportNotImplemented(format!(
            "invalid auth config for environment {environment_id}"
        ))
    })?;
    let transport = match &env.default_cred_id {
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
    };
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
```

文件末尾（tests 外）加 `spawn_timeout_kill`：

```rust
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
```

- [ ] **Step 3.4：修复所有调用点（编译）**

逐个改（均为一行）：

1. `src-tauri/src/exec/tunnel.rs:103`：
   `build_transport(env_id, &env)` → `build_transport(env_id, &env, None, None)`
2. `src-tauri/src/transfer/mod.rs:69-72`：
   ```rust
   let channel = crate::exec::pool::build_transport(env_id, &env, None, None)
       .map_err(|e| e.to_string())?;
   channel.connect().await.map_err(|e| e.to_string())?;
   Ok(channel)
   ```
3. `src-tauri/src/tools/builtin/jvm/core.rs:20`：
   `pool.get_or_create(&env.id, db)` → `pool.get_or_create(&env.id, None, None, db)`
4. `src-tauri/src/tools/builtin/run_command.rs:76`：
   `pool.get_or_create(&env.id, &self.db)` → `pool.get_or_create(&env.id, None, None, &self.db)`
5. `src-tauri/src/tools/builtin/ensure_tool.rs:56`：
   同上 → `pool.get_or_create(&env.id, None, None, &self.db)`

`cargo check --manifest-path src-tauri/Cargo.toml` 兜底找漏网（若 arthas/attach.rs 也有直接调用，同样加 `, None, None`）。

- [ ] **Step 3.5：修 tunnel.rs 失效测试**

`tunnel.rs` 测试 `test_open_unsupported_transport_maps_to_config_error`（382-392 行）删除，替换为：

```rust
    #[tokio::test]
    async fn test_open_invalid_auth_maps_to_config_error() {
        let (_tmp, pool) = setup().await;
        // transport_type 不再参与分发（按 pod 参数分发）；
        // 非法 auth_type 仍是 TransportNotImplemented → Config
        sqlx::query(
            "INSERT INTO environments (id, name, host, port, user, transport_type, auth_type, private_key_path, created_at) \
             VALUES ('env-d', 'd', '10.0.0.1', 22, 'root', 'local', 'bogus_auth', NULL, '2026-01-01T00:00:00Z')",
        ).execute(&pool).await.unwrap();
        let mgr = TunnelManager::new(pool);
        let r = mgr.open("env-d", "127.0.0.1", 8563).await;
        assert!(matches!(r, Err(TunnelError::Config(_))), "expected Config error, got: {r:?}");
    }
```

- [ ] **Step 3.6：跑全部测试**

Run: `cargo test --manifest-path src-tauri/Cargo.toml`
Expected: 全部通过（既有 pool 测试经 `From<String>` 兼容无需改；`mark_last_used_for_test("env-slow", ...)` 这类调用需把参数包成 key：`pool.mark_last_used_for_test(&TargetKey::base("env-slow"), ...)`，`get_or_create_unchecked_for_test("env-1")` 同理 → `&TargetKey::base("env-1")`，编译器会指出全部位置）

- [ ] **Step 3.7：提交**

```bash
git add src-tauri/src/exec/pool.rs src-tauri/src/exec/tunnel.rs src-tauri/src/transfer/mod.rs src-tauri/src/tools/builtin/jvm/core.rs src-tauri/src/tools/builtin/run_command.rs src-tauri/src/tools/builtin/ensure_tool.rs
git commit -m "feat: pool TargetKey (env,pod,container) with param-driven transport dispatch and timeout kill"
```

---

### Task 4: core 贯通 — resolve 5 参 + exec_jdk_command target 化 + jdk_cache 复合键

**Files:**
- Modify: `src-tauri/src/tools/builtin/jvm/core.rs`、`jdk_cache.rs`、`simple.rs`
- Modify: `processes.rs:32`、`heap_dump.rs:36`、`jfr/mod.rs:81`（机械 `(None, None)`）

- [ ] **Step 4.1：jdk_cache 加 cache_key（含测试）**

`jdk_cache.rs` 在 `JdkCache` impl 前加：

```rust
/// 复合缓存键：VM 目标 = env_id；k8s 目标 = env|pod=..|ctr=..
/// （JdkCache 的 HashMap<String, JdkLayout> 不变，只换 key 构造）
pub fn cache_key(env_id: &str, pod: Option<&str>, container: Option<&str>) -> String {
    match pod.filter(|p| !p.is_empty()) {
        None => env_id.to_string(),
        Some(p) => format!("{env_id}|pod={p}|ctr={}", container.filter(|c| !c.is_empty()).unwrap_or("-")),
    }
}
```

测试追加：

```rust
    #[test]
    fn test_cache_key_composite_for_k8s() {
        assert_eq!(cache_key("e1", None, None), "e1");
        assert_eq!(cache_key("e1", Some("p1"), None), "e1|pod=p1|ctr=-");
        assert_eq!(cache_key("e1", Some("p1"), Some("c1")), "e1|pod=p1|ctr=c1");
        // 空串视为未传
        assert_eq!(cache_key("e1", Some(""), None), "e1");
    }
```

- [ ] **Step 4.2：core.rs — resolve_environment 5 参 + exec_jdk_command target 化**

`resolve_environment` 整体替换：

```rust
/// 环境名 → env 记录 + channel（run_command / ensure_tool 同款语义，提取共享）。
/// pod/container：k8s 目标定位（None = 宿主机 VM 模式）。
/// Ok(None) = 环境不存在（调用方引导 list_environments）。
pub async fn resolve_environment(
    db: &sqlx::SqlitePool,
    exec_pool: &Arc<tokio::sync::Mutex<crate::exec::pool::ExecChannelPool>>,
    environment: &str,
    pod: Option<&str>,
    container: Option<&str>,
) -> Result<Option<(crate::app::environments::EnvironmentRow, Arc<dyn ExecChannel>)>, String> {
    let env = match crate::app::environments::find_by_name(db, environment).await {
        Ok(Some(env)) => env,
        Ok(None) => return Ok(None),
        Err(e) => return Err(format!("查询环境失败: {e}")),
    };
    let channel = {
        let mut pool = exec_pool.lock().await;
        pool.get_or_create(&env.id, pod, container, db).await.map_err(|e| e.to_string())?
    };
    Ok(Some((env, channel)))
}
```

`exec_jdk_command` 签名与超时分支替换（其余分支只把 `env_id` 日志字段改为 `%target.env_id`）：

```rust
    pub async fn exec_jdk_command(
        &self,
        session_id: &str,
        target: &crate::exec::pool::TargetKey,
        channel: &Arc<dyn ExecChannel>,
        bin_path: &str,
        command: &str,
        timeout_secs: u64,
        output_ext: &str,
    ) -> ToolOutput {
```

超时分支：

```rust
            Err(_) => {
                tracing::warn!(session_id, env_id = %target.env_id, timeout_secs, "jvm tool timed out, dropping connection to terminate remote process");
                {
                    let mut pool = self.exec_pool.lock().await;
                    pool.disconnect_target(target).await;
                }
                // k8s 目标：断 SSH 只杀 kubectl，容器内进程可能存活 → 独立连接补刀
                // （VM 目标 no-op）
                crate::exec::pool::spawn_timeout_kill(
                    self.db.clone(),
                    target.clone(),
                    command.to_string(),
                );
                error_output(
                    "timeout_error",
                    &format!("command timed out after {timeout_secs}s; connection closed, remote process kill is best-effort for containers"),
                )
            }
```

`is_jdk_missing` 分支的 clear 改复合键：

```rust
                    self.jdk_cache.clear(&super::jdk_cache::cache_key(
                        &target.env_id,
                        target.pod.as_deref(),
                        target.container.as_deref(),
                    )).await;
```

core.rs 测试更新（`exec_jdk_command` 调用点）：

```rust
        // 原：c.exec_jdk_command("s1", "env-1", &ch, ...)
        let target = crate::exec::pool::TargetKey::base("env-1");
        let out = c.exec_jdk_command("s1", &target, &ch, "/jdk/bin/jcmd", "/jdk/bin/jcmd 1 GC.heap_info", 30, "log").await;
```

`test_exec_timeout_drops_connection` 中：

```rust
        pool.lock().await.insert_channel(crate::exec::pool::TargetKey::base("env-1"), Arc::new(SlowChannel) as Arc<dyn ExecChannel>).await;
        let ch = pool.lock().await.get_or_create_unchecked_for_test(&crate::exec::pool::TargetKey::base("env-1")).await;
        let target = crate::exec::pool::TargetKey::base("env-1");
        let out = c.exec_jdk_command("s1", &target, &ch, "/jdk/bin/jcmd", "/jdk/bin/jcmd 1 GC.heap_info", 1, "log").await;
```

- [ ] **Step 4.3：resolve 调用点机械修复（processes/heap_dump/jfr 先传 None）**

- `processes.rs:32`：`resolve_environment(&self.core.db, &self.core.exec_pool, environment)` → 尾部加 `, None, None`
- `heap_dump.rs:36-41`：`resolve_environment(&self.core.db, &self.core.exec_pool, environment,)` → 尾部加 `, None, None`
- `jfr/mod.rs:81`：同上

- [ ] **Step 4.4：simple.rs 接入真实 pod/container 参数 + schema**

`JvmSimpleHandler::execute` 中 pid 解析后加：

```rust
        let pod = args.get("pod").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let container = args.get("container").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
```

resolve 与 cache 取用改为：

```rust
        let (env, channel) = match resolve_environment(
            &self.core.db,
            &self.core.exec_pool,
            environment,
            pod,
            container,
        )
        .await
        {
```

```rust
        let Some(layout) = self.core.jdk_cache
            .get(&crate::tools::builtin::jvm::jdk_cache::cache_key(&env.id, pod, container))
            .await
        else {
```

（下方 miss 分支错误信息改为：`"该环境尚未装备 JDK。请先调用 ensure_tool(environment, tool=\"jdk\"；容器内服务需同时传 pod/container) 装备，然后重试本工具。"`）

exec 调用改传 target：

```rust
        let target = crate::exec::pool::TargetKey::from_parts(&env.id, pod, container);
        tracing::info!(session_id = %ctx.session_id, env_id = %env.id, pod = pod.unwrap_or("-"), pid, command, "jvm tool executing");
        self.core
            .exec_jdk_command(&ctx.session_id, &target, &channel, &bin_path, &command, timeout_secs, "log")
            .await
```

`simple_schema` 的 props 循环前加两个公共属性：

```rust
    props.insert(
        "pod".into(),
        serde_json::json!({ "type": "string", "description": "Kubernetes Pod 名（容器内服务诊断时必传；VM/宿主机进程诊断不传）" }),
    );
    props.insert(
        "container".into(),
        serde_json::json!({ "type": "string", "description": "容器名（多容器 Pod 时指定；缺省用 Pod 默认容器）" }),
    );
```

simple.rs 测试 `setup()` 中 `insert_channel(env_id.clone(), ...)` 不变（From\<String\> 兼容）。

- [ ] **Step 4.5：跑测试**

Run: `cargo test --manifest-path src-tauri/Cargo.toml`
Expected: 全部通过

- [ ] **Step 4.6：提交**

```bash
git add src-tauri/src/tools/builtin/jvm/
git commit -m "feat: plumb pod/container params through jvm core and simple tools with composite jdk cache key"
```

---

### Task 5: processes / run_command / heap_dump / jfr 参数贯通 + schema

**Files:**
- Modify: `processes.rs`、`run_command.rs`、`heap_dump.rs`、`jfr/mod.rs`

- [ ] **Step 5.1：processes.rs**

pid/keyword 解析旁加（同 Task 4.4 的两行提取），resolve 改传 `pod, container`；超时分支（66 行附近）改为：

```rust
            Err(_) => {
                tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, timeout_secs, "list_processes timed out, dropping connection");
                let target = crate::exec::pool::TargetKey::from_parts(&env.id, pod, container);
                {
                    let mut pool = self.core.exec_pool.lock().await;
                    pool.disconnect_target(&target).await;
                }
                crate::exec::pool::spawn_timeout_kill(self.core.db.clone(), target, command.clone());
                error_output("timeout_error", &format!("command timed out after {timeout_secs}s"))
            }
```

schema properties 加（同 simple 的两段 json）；description 尾部追加：`传 pod 时列出的是容器内进程（PID 为容器内 PID，后续 jvm_* 工具需带相同 pod）。`

- [ ] **Step 5.2：run_command.rs**

参数提取（`command` 解析后）+ `get_or_create(&env.id, pod, container, &self.db)`；超时分支改为：

```rust
            Err(_) => {
                tracing::warn!(session_id = %ctx.session_id, env_id = %env.id, timeout_secs, "run_command timed out, dropping connection to terminate remote process");
                let target = crate::exec::pool::TargetKey::from_parts(&env.id, pod, container);
                {
                    let mut pool = self.exec_pool.lock().await;
                    pool.disconnect_target(&target).await;
                }
                crate::exec::pool::spawn_timeout_kill(self.db.clone(), target, command.to_string());
                ToolOutput {
                    success: false,
                    data: serde_json::json!({
                        "error": "timeout_error",
                        "message": format!("command timed out after {timeout_secs}s; connection was closed to terminate the remote process"),
                        "elapsed_ms": elapsed_ms,
                    }),
                    raw_stdout: None,
                }
            }
```

schema properties 加 pod/container 两段（同上）；description 追加：`传 pod 时命令在 Pod 容器内执行（sh -c）。`

- [ ] **Step 5.3：heap_dump.rs / jfr/mod.rs（仅参数贯通，行为 Phase 2）**

两个文件各做三件事（模式与 simple.rs 完全一致，此处不再重复代码）：

1. 参数提取两行（pod/container）；
2. `resolve_environment(...)` 尾参传 `pod, container`；
3. `jdk_cache.get(&env.id)` → `jdk_cache.get(&cache_key(&env.id, pod, container))`（引入 `use ...jdk_cache::cache_key`）；
4. 超时分支 `pool.disconnect(&env.id)` → `disconnect_target(&TargetKey::from_parts(&env.id, pod, container))` + `spawn_timeout_kill(...)`；
5. 各自 tool def 的 `input_schema` properties 里追加 pod/container 两段 json（同 simple 的文案）。

**明确边界**：heap_dump 的 `remote_path` 本期仍为 `/tmp/friday-tools/heapdump-...`（Pod 内同样可写，dump 拉回依赖 Phase 2 的 K8sChannel::download）；JFR 同理。工具描述不承诺容器完整可用。

- [ ] **Step 5.4：跑测试 + 提交**

Run: `cargo test --manifest-path src-tauri/Cargo.toml` → 全部通过

```bash
git add src-tauri/src/tools/builtin/
git commit -m "feat: pod/container params across processes, run_command, heap_dump and jfr tools"
```

---

### Task 6: ProvisionContext.remote_tools_dir + jdk.rs 目录参数化

**Files:**
- Modify: `provision/package.rs`、`provision/jdk.rs`、`provision/arthas.rs`（ProvisionContext 字面量）、`tools/builtin/ensure_tool.rs:74`（字面量）

- [ ] **Step 6.1：package.rs 加字段**

`ProvisionContext` 增：

```rust
    /// 远端工具根目录：VM = /tmp/friday-tools；k8s = /opt/log/dump/heapdump/friday-tools
    pub remote_tools_dir: String,
```

- [ ] **Step 6.2：jdk.rs 参数化（VM 行为不变）**

- `jdk_home_for` 改签名：
  ```rust
  pub fn jdk_home_for(tools_dir: &str, openjdk_version: &str) -> String {
      format!("{tools_dir}/jdk-{openjdk_version}")
  }
  ```
- `ensure` 内：`let dir = &ctx.remote_tools_dir;`，`home = jdk_home_for(dir, &probe.openjdk_version)`，`tarball = format!("{dir}/jdk-{}.tar.gz", probe.openjdk_version)`；缓存检查命令 `mkdir -p {dir} && test -x {home}/bin/jcmd`；`extract_cmd` 里两处 `{REMOTE_TOOLS_DIR}` → `{dir}`。
- `REMOTE_TOOLS_DIR` 常量保留（VM 默认值）。
- 测试 `test_ctx` 加 `remote_tools_dir: "/tmp/friday-tools".into(),`（断言的路径全部不变，仍过）。

- [ ] **Step 6.3：所有 ProvisionContext 字面量补字段**

`cargo check` 会逐一报错，对每处加：

```rust
            remote_tools_dir: crate::provision::jdk::REMOTE_TOOLS_DIR.to_string(),
```

已知位置：`tools/builtin/ensure_tool.rs:74`（pctx）、`provision/arthas.rs`（ArthasPackage::ensure 构造的 pctx）、`jdk.rs` 测试 `test_ctx`。

- [ ] **Step 6.4：跑测试 + 提交**

Run: `cargo test --manifest-path src-tauri/Cargo.toml` → 全部通过（VM 语义零变化）

```bash
git add src-tauri/src/provision/ src-tauri/src/tools/builtin/ensure_tool.rs
git commit -m "refactor: parameterize remote tools dir in ProvisionContext (vm behavior unchanged)"
```

---

### Task 7: K8sJdkPackage（provision/k8s.rs）

**Files:**
- Create: `src-tauri/src/provision/k8s.rs`
- Modify: `src-tauri/src/provision/mod.rs`（加 `pub mod k8s;`）、`provision/jdk.rs`（`bins_for` 改 `pub(crate)`）

- [ ] **Step 7.1：jdk.rs 开放复用**

`fn bins_for` → `pub(crate) fn bins_for`。

- [ ] **Step 7.2：写失败测试（新文件测试模块）**

新建 `provision/k8s.rs`，先写测试（实现体空的 struct 会编译失败，即失败态）：

```rust
//! K8s 容器内 JDK 装备：与 VM 模式（provision::jdk::JdkPackage）同构，
//! 差异点：musl 保险丝、容器自带工具优先、--no-same-owner 解压、chgrp 属组修正。
//! 固定目录 /opt/log/dump/heapdump/friday-tools（用户指定，不触发驱逐）。

use crate::exec::k8s::OSS_GROUP;
use crate::provision::jdk::{
    bins_for, build_download_url, jdk_home_for, run_remote, try_remote_download, JdkPackage,
    JDK_TOOL_NAME, JvmProbe,
};
use crate::provision::package::{
    emit_progress, ProvisionContext, ProvisionError, ProvisionResult, StageTimeouts, ToolPackage,
};
use async_trait::async_trait;
use std::time::Duration;

const MUSL_PROBE_PATH: &str = "/lib/ld-musl-x86_64.so.1";
const JDK_TARBALL_MIN_BYTES: u64 = 50 * 1024 * 1024;

pub struct K8sJdkPackage;
```

测试模块：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::channel::{ExecChannel, ExecOutput};
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
```

- [ ] **Step 7.3：跑测试确认编译失败**

Run: `cargo test --manifest-path src-tauri/Cargo.toml provision::k8s`
Expected: 编译错误（K8sJdkPackage 未实现 ToolPackage）

- [ ] **Step 7.4：实现 K8sJdkPackage**

struct 之后追加：

```rust
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
            // ⑤ 通道 B：本地下载 → K8sChannel.upload（两跳 + chgrp，Task 2 已实现）
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
```

`provision/mod.rs` 加 `pub mod k8s;`。

注意：`parse_probe_output`/`build_download_url` 已是 pub；`cache_path_for` 需在 `provision/transfer.rs` 中确认可见性（jdk.rs 测试已在用，若为私有则改 `pub(crate)`）。

- [ ] **Step 7.5：跑测试确认通过**

Run: `cargo test --manifest-path src-tauri/Cargo.toml provision::k8s`
Expected: `test result: ok. 4 passed`

- [ ] **Step 7.6：提交**

```bash
git add src-tauri/src/provision/
git commit -m "feat: K8sJdkPackage - container jdk provisioning with musl guard, native-first, chgrp fix"
```

---

### Task 8: ensure_tool pod 分发

**Files:**
- Modify: `src-tauri/src/tools/builtin/ensure_tool.rs`

- [ ] **Step 8.1：写失败测试**

测试模块追加：

```rust
    /// k8s 目标：pod 参数 → K8sJdkPackage + 容器自带 jcmd 短路 + 复合缓存键
    #[tokio::test]
    async fn test_ensure_with_pod_uses_k8s_package_and_composite_cache() {
        let (tmp, db, exec_pool, cache, bus) = setup().await;
        let env_id = crate::app::environments::find_by_name(&db, "prod").await.unwrap().unwrap().id;
        // 注入 k8s 目标通道（probe ok / musl 无 / native 命中）
        exec_pool.lock().await.insert_channel(
            crate::exec::pool::TargetKey::k8s(&env_id, "pod-1", None),
            Arc::new(K8sNativeChannel) as Arc<dyn ExecChannel>,
        ).await;
        let jdk_cache = Arc::new(crate::tools::builtin::jvm::jdk_cache::JdkCache::new());
        let handler = EnsureToolHandler {
            db: db.clone(),
            exec_pool,
            cache_dir: cache,
            bus,
            jdk_cache: jdk_cache.clone(),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        };
        let ctx = ToolContext { session_id: "s1".into(), channel: None };
        let out = handler
            .execute(
                serde_json::json!({"environment": "prod", "tool": "jdk", "pod": "pod-1"}),
                &ctx,
            )
            .await;
        assert!(out.success, "out: {}", out.data);
        assert_eq!(out.data["bins"]["jcmd"], "/usr/bin/jcmd");
        // 复合键写入（env|pod=..）
        let layout = jdk_cache
            .get(&crate::tools::builtin::jvm::jdk_cache::cache_key(&env_id, Some("pod-1"), None))
            .await
            .expect("composite cache key must be populated");
        assert_eq!(layout.bins["jcmd"], "/usr/bin/jcmd");
        drop(tmp);
    }

    /// K8sNativeChannel：probe 输出 / musl exit 1 / native 命中
    struct K8sNativeChannel;

    #[async_trait]
    impl ExecChannel for K8sNativeChannel {
        async fn run(&self, cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
            if cmd.contains("ld-musl") {
                return Ok(ExecOutput { stdout: String::new(), stderr: String::new(), exit_code: 1 });
            }
            if cmd.contains("command -v jcmd") {
                return Ok(ExecOutput {
                    stdout: "/usr/bin/jcmd\n/usr/bin/jstat\n".into(),
                    stderr: String::new(),
                    exit_code: 0,
                });
            }
            Ok(ExecOutput {
                stdout: "BiSheng_JDK_Enterprise_205.2.0.110.B001\nopenjdk version \"21.0.11\" 2025-04-15\n---\nx86_64\n".into(),
                stderr: String::new(),
                exit_code: 0,
            })
        }
        async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
        async fn disconnect(&self) {}
        async fn is_alive(&self) -> bool { true }
    }
```

（`ExecOutput`/`async_trait` 已在文件测试 use 中。）

- [ ] **Step 8.2：跑测试确认失败**

Run: `cargo test --manifest-path src-tauri/Cargo.toml ensure_tool`
Expected: FAIL（pod 参数被忽略 → 查的是 VM 通道 → connection_error 或 jdk cache 键不符）

- [ ] **Step 8.3：实现分发**

`execute` 中 java_bin 解析后加：

```rust
        let pod = args.get("pod").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let container = args.get("container").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
```

channel 获取改为：

```rust
        let channel = {
            let mut pool = self.exec_pool.lock().await;
            match pool.get_or_create(&env.id, pod, container, &self.db).await {
```

pctx 增 remote_tools_dir：

```rust
        let pctx = ProvisionContext {
            session_id: ctx.session_id.clone(),
            env_id: env.id.clone(),
            channel,
            cache_dir: self.cache_dir.clone(),
            artifactory_base_url: base_url,
            arthas_zip: None,
            remote_tools_dir: if pod.is_some() {
                crate::provision::k8s::POD_TOOLS_DIR.to_string()
            } else {
                crate::provision::jdk::REMOTE_TOOLS_DIR.to_string()
            },
            timeouts: StageTimeouts::default(),
            bus: self.bus.clone(),
        };
```

lock_key 与包分发：

```rust
        let lock_key = format!("{}/{}/{}", env.id, pod.unwrap_or("-"), container.unwrap_or("-"));
```

```rust
        let package: Box<dyn ToolPackage> = if pod.is_some() {
            Box::new(crate::provision::k8s::K8sJdkPackage)
        } else {
            Box::new(crate::provision::jdk::JdkPackage)
        };
        match package.ensure(&pctx, java_bin).await {
```

成功分支缓存写入：

```rust
                self.jdk_cache
                    .set(&crate::tools::builtin::jvm::jdk_cache::cache_key(&env.id, pod, container), layout)
                    .await;
```

schema properties 加（environment 之后）：

```rust
                "pod": { "type": "string", "description": "Kubernetes Pod 名（装备到容器内时必传）" },
                "container": { "type": "string", "description": "容器名（多容器 Pod 时指定）" },
```

description 追加一句：`容器内服务：先 k8s_find_pods 定位 Pod，再传 pod 参数调用本工具。`

- [ ] **Step 8.4：跑测试确认通过 + 提交**

Run: `cargo test --manifest-path src-tauri/Cargo.toml ensure_tool` → 全部通过

```bash
git add src-tauri/src/tools/builtin/ensure_tool.rs
git commit -m "feat: ensure_tool dispatches K8sJdkPackage for pod targets with composite cache key"
```

---

### Task 9: k8s_find_pods 工具 + ToolCategory::K8s

**Files:**
- Create: `src-tauri/src/tools/builtin/k8s.rs`
- Modify: `tools/category.rs`、`tools/builtin/mod.rs`、`lib.rs`、`src/lib/types.ts`、`src/components/tools/ToolsPanel.tsx`

- [ ] **Step 9.1：category 加变体**

`category.rs` 枚举 `Environment` 之后插入：

```rust
    K8s,
```

（serde snake_case → `"k8s"`；声明序即面板序，K8s 发现紧跟环境与进程。）

- [ ] **Step 9.2：写失败测试（新文件）**

新建 `tools/builtin/k8s.rs`：

```rust
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

/// pattern 过滤：Pod 名大小写不敏感包含匹配；None = 全部
pub fn filter_pods(pods: Vec<PodInfo>, pattern: Option<&str>) -> Vec<PodInfo> {
    match pattern.map(str::trim).filter(|p| !p.is_empty()) {
        None => pods,
        Some(p) => {
            let p = p.to_lowercase();
            pods.into_iter().filter(|pod| pod.name.to_lowercase().contains(&p)).collect()
        }
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
        assert_eq!(pods[0].containers, vec!["app".to_string(), "sidecar".to_string()]);
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
}
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml builtin::k8s` → 2 passed（纯函数一次成形）。

- [ ] **Step 9.3：实现 handler + tool def（含测试）**

k8s.rs 追加：

```rust
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
```

handler 测试（测试模块追加；mock channel 与 processes.rs 的 `PsChannel` 同款，stdout 给上面 SAMPLE，exit 0；环境 setup 抄 processes.rs 的 `setup`）：

```rust
    mod handler_tests {
        use super::super::*;
        use crate::exec::channel::{ExecChannel, ExecOutput};
        use crate::tools::registry::{ToolContext, ToolHandler};
        use async_trait::async_trait;

        struct KubectlChannel {
            exit_code: i32,
            stderr: &'static str,
        }

        #[async_trait]
        impl ExecChannel for KubectlChannel {
            async fn run(&self, _cmd: &str) -> Result<ExecOutput, Box<dyn std::error::Error + Send + Sync>> {
                Ok(ExecOutput { stdout: super::SAMPLE.to_string(), stderr: self.stderr.to_string(), exit_code: self.exit_code })
            }
            async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
            async fn disconnect(&self) {}
            async fn is_alive(&self) -> bool { true }
        }

        async fn setup_with(channel: Arc<dyn ExecChannel>) -> Arc<JvmExecCore> {
            // 与 processes.rs setup 同款：save_environment("prod") + insert_channel + JvmExecCore
            let tmp = tempfile::tempdir().unwrap();
            let db = crate::infra::db::init(tmp.path().join("friday.db")).await.unwrap();
            let env_id = crate::app::env_save::save_environment(
                &db, None, "prod", "10.0.0.1", 22,
                vec![crate::app::env_save::CredentialInput {
                    id: None, username: "root".to_string(), auth_type: "password".to_string(),
                    private_key_path: None, secret: None, is_default: true,
                }],
            ).await.unwrap().environment.id;
            let exec_pool = Arc::new(tokio::sync::Mutex::new(crate::exec::pool::ExecChannelPool::new()));
            exec_pool.lock().await.insert_channel(env_id, channel).await;
            std::mem::forget(tmp); // TempDir 存活到测试结束
            Arc::new(JvmExecCore {
                db, exec_pool,
                jdk_cache: Arc::new(crate::tools::builtin::jvm::jdk_cache::JdkCache::new()),
                artifacts_dir: std::path::PathBuf::from("/tmp/unused-artifacts"),
            })
        }

        #[tokio::test]
        async fn test_find_pods_filters_and_structures() {
            let core = setup_with(Arc::new(KubectlChannel { exit_code: 0, stderr: "" })).await;
            let handler = FindPodsHandler { core };
            let ctx = ToolContext { session_id: "s1".into(), channel: None };
            let out = handler
                .execute(serde_json::json!({"environment": "prod", "pattern": "snmpagent"}), &ctx)
                .await;
            assert!(out.success, "out: {}", out.data);
            assert_eq!(out.data["count"], 1);
            assert_eq!(out.data["pods"][0]["name"], "snmpagent-7d9b-x2vkl");
        }

        #[tokio::test]
        async fn test_no_kubectl_reports_not_k8s_environment() {
            let core = setup_with(Arc::new(KubectlChannel { exit_code: 127, stderr: "bash: kubectl: command not found" })).await;
            let handler = FindPodsHandler { core };
            let ctx = ToolContext { session_id: "s1".into(), channel: None };
            let out = handler.execute(serde_json::json!({"environment": "prod"}), &ctx).await;
            assert!(!out.success);
            assert_eq!(out.data["error"], "not_k8s_environment");
            assert!(out.data["message"].as_str().unwrap().contains("list_processes"));
        }

        #[tokio::test]
        async fn test_kubectl_error_passthrough() {
            let core = setup_with(Arc::new(KubectlChannel { exit_code: 1, stderr: "error: server unauthorized" })).await;
            let handler = FindPodsHandler { core };
            let ctx = ToolContext { session_id: "s1".into(), channel: None };
            let out = handler.execute(serde_json::json!({"environment": "prod"}), &ctx).await;
            assert!(!out.success);
            assert_eq!(out.data["error"], "kubectl_error");
            assert!(out.data["message"].as_str().unwrap().contains("unauthorized"));
        }
    }
```

- [ ] **Step 9.4：注册**

`tools/builtin/mod.rs` 加 `pub mod k8s;`（按字母序放 ensure_tool 之后）。
`lib.rs` 在 `crate::tools::builtin::jvm::register_all(...)` 调用块之后加：

```rust
            tool_registry.register(crate::tools::builtin::k8s::k8s_find_pods_tool_def(jvm_core.clone()));
```

- [ ] **Step 9.5：前端分类**

`src/lib/types.ts` 的 `ToolCategory` 联合类型加 `"k8s"`（environment 之后）：

```typescript
export type ToolCategory =
  | "environment"
  | "k8s"
  | "jvm"
  | "heap"
  | "jfr"
  | "arthas"
  | "file_transfer"
  | "builtin";
```

`ToolsPanel.tsx`：import 列表（2-14 行）加 `Stack,`；`CATEGORY_META` 在 environment 之后插入：

```typescript
  { key: "k8s", label: "K8s 发现", icon: Stack },
```

`collapsed` 初始对象加 `k8s: true,`。

- [ ] **Step 9.6：跑测试 + typecheck + 提交**

Run: `cargo test --manifest-path src-tauri/Cargo.toml builtin::k8s` → 5 passed；`pnpm typecheck` → 通过

```bash
git add src-tauri/src/tools/ src-tauri/src/lib.rs src/lib/types.ts src/components/tools/ToolsPanel.tsx
git commit -m "feat: k8s_find_pods discovery tool with K8s category"
```

---

### Task 10: 环境类型后端（transport_type 存取）

**Files:**
- Modify: `src-tauri/src/app/environments.rs`、`env_save.rs`

- [ ] **Step 10.1：写失败测试（env_save.rs 测试模块追加）**

```rust
    #[tokio::test]
    async fn test_save_with_transport_type_k8s_roundtrip() {
        let (_tmp, pool) = setup().await;
        let outcome = save_environment_with_transport(
            &pool, None, "prod-k8s", "10.0.0.2", 22, "k8s",
            vec![cred("opc", true)],
        ).await.unwrap();
        assert_eq!(outcome.environment.transport_type, "k8s");
        // 默认路径（旧 wrapper）仍是 ssh
        let vm = save_environment(&pool, None, "prod-vm", "10.0.0.3", 22, vec![cred("opc", true)]).await.unwrap();
        assert_eq!(vm.environment.transport_type, "ssh");
    }

    #[tokio::test]
    async fn test_save_rejects_invalid_transport_type() {
        let (_tmp, pool) = setup().await;
        let err = save_environment_with_transport(
            &pool, None, "x", "10.0.0.1", 22, "docker",
            vec![cred("opc", true)],
        ).await.unwrap_err();
        assert!(matches!(err, SaveError::Validation(_)));
    }

    #[tokio::test]
    async fn test_edit_updates_transport_type() {
        let (_tmp, pool) = setup().await;
        let first = save_environment(&pool, None, "prod", "10.0.0.1", 22, vec![cred("opc", true)]).await.unwrap();
        let env_id = first.environment.id;
        let second = save_environment_with_transport(
            &pool, Some(&env_id), "prod", "10.0.0.1", 22, "k8s",
            vec![cred("opc", true)],
        ).await.unwrap();
        assert_eq!(second.environment.transport_type, "k8s");
    }
```

- [ ] **Step 10.2：跑测试确认编译失败**

Run: `cargo test --manifest-path src-tauri/Cargo.toml env_save`
Expected: 编译错误（`save_environment_with_transport`/`transport_type` 字段未定义）

- [ ] **Step 10.3：实现**

`environments.rs`：
- `EnvironmentRow` 加字段 `pub transport_type: String,`（`created_at` 前）
- `ENV_COLUMNS` 改 `"id, name, host, port, user, auth_type, private_key_path, transport_type, created_at"`
- `row_to_env` 加 `transport_type: r.get("transport_type"),`

`env_save.rs`：
- 原 `save_environment` 整体改名 `save_environment_with_transport`，签名加 `transport_type: &str`（放 port 后），函数体开头加校验：

```rust
    if !matches!(transport_type, "ssh" | "k8s") {
        return Err(SaveError::Validation(format!(
            "transport_type 必须是 ssh 或 k8s：{transport_type:?}"
        )));
    }
```

- INSERT 语句改（`'ssh'` 字面量 → 绑定参数）：

```rust
        sqlx::query(
            "INSERT INTO environments (id, name, host, port, user, transport_type, auth_type, private_key_path, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&env_id)
        .bind(name.trim())
        .bind(host.trim())
        .bind(port as i64)
        .bind(def.username.trim())
        .bind(transport_type)
        .bind(&def.auth_type)
        .bind(bound_key_path(&def.auth_type, def.private_key_path.as_deref()))
        .bind(&now)
```

- UPDATE 语句改：

```rust
        let updated = sqlx::query(
            "UPDATE environments SET name = ?, host = ?, port = ?, user = ?, transport_type = ?, auth_type = ?, private_key_path = ? WHERE id = ?",
        )
        .bind(name.trim())
        .bind(host.trim())
        .bind(port as i64)
        .bind(def.username.trim())
        .bind(transport_type)
        .bind(&def.auth_type)
        .bind(bound_key_path(&def.auth_type, def.private_key_path.as_deref()))
        .bind(&env_id)
```

- 原名保留为兼容 wrapper（既有调用零改动）：

```rust
/// 兼容入口：transport_type 默认 ssh（既有调用方与测试不变）
pub async fn save_environment(
    pool: &SqlitePool,
    environment_id: Option<&str>,
    name: &str,
    host: &str,
    port: u16,
    credentials: Vec<CredentialInput>,
) -> Result<SaveOutcome, SaveError> {
    save_environment_with_transport(pool, environment_id, name, host, port, "ssh", credentials).await
}
```

- `SaveEnvironmentParams` 加 `pub transport_type: Option<String>,`；`save_environment_cmd` 改调：

```rust
    let outcome = save_environment_with_transport(
        &state.db,
        params.environment_id.as_deref(),
        params.name.trim(),
        params.host.trim(),
        params.port.unwrap_or(22),
        params.transport_type.as_deref().unwrap_or("ssh"),
        params.credentials,
    )
    .await
    .map_err(|e| e.to_string())?;
```

（`disconnect(&outcome.environment.id)` 调用不变——Task 3 后语义即 env-wide。）

- [ ] **Step 10.4：跑测试**

Run: `cargo test --manifest-path src-tauri/Cargo.toml` → 全部通过（既有测试经 wrapper 零改动）

- [ ] **Step 10.5：提交**

```bash
git add src-tauri/src/app/environments.rs src-tauri/src/app/env_save.rs
git commit -m "feat: environment transport_type (ssh|k8s) persistence"
```

---

### Task 11: 前端环境类型 + 全量验证

**Files:**
- Modify: `src/lib/types.ts`、`src/lib/ipc.ts`、`src/store/envStore.ts`、`src/components/environments/EnvironmentDialog.tsx`

- [ ] **Step 11.1：types.ts**

`EnvironmentAuthType` 附近加：

```typescript
export type EnvironmentTransport = "ssh" | "k8s";
```

`EnvironmentRow` 加字段（`user` 之后）：

```typescript
  transport_type: EnvironmentTransport;
```

- [ ] **Step 11.2：ipc.ts**

`saveEnvironment` 参数加 `transportType: "ssh" | "k8s";`，invoke params 对象加：

```typescript
      transportType: params.transportType,
```

- [ ] **Step 11.3：envStore.ts**

`save` 参数类型加 `transportType: "ssh" | "k8s";`（透传给 ipcSave，无需改函数体）。

- [ ] **Step 11.4：EnvironmentDialog.tsx**

- `EMPTY_FORM` 改：

```typescript
const EMPTY_FORM = { name: "", host: "", port: "22", transportType: "ssh" as "ssh" | "k8s" };
```

- 编辑初始化（41 行）改：

```typescript
      setForm(
        editing
          ? { name: editing.name, host: editing.host, port: String(editing.port), transportType: editing.transport_type }
          : { ...EMPTY_FORM },
      );
```

- 端口 Field 之后（216 行 `</div>` 后）插入：

```tsx
            <Field label="环境类型" htmlFor="env-transport">
              <select
                id="env-transport"
                value={form.transportType}
                onChange={(e) => setForm({ ...form, transportType: e.target.value as "ssh" | "k8s" })}
                className={inputCls}
              >
                <option value="ssh">宿主机 SSH</option>
                <option value="k8s">Kubernetes 宿主机</option>
              </select>
            </Field>
            {form.transportType === "k8s" && (
              <p className="text-xs text-muted-foreground">
                Kubernetes 宿主机：诊断时可先用 k8s_find_pods 发现 Pod（凭证仍是该宿主机的
                SSH 登录信息）；不传 pod 的工具直接诊断宿主机进程。
              </p>
            )}
```

- `handleSave` 的 save 调用加：

```typescript
        transportType: form.transportType,
```

- [ ] **Step 11.5：typecheck + 全量验证**

Run: `pnpm typecheck`
Expected: 无错误

Run: `cargo test --manifest-path src-tauri/Cargo.toml`
Expected: 全部通过，零跳过

Run: `cargo check --manifest-path src-tauri/Cargo.toml`
Expected: 无 error（新代码 warning 清零）

- [ ] **Step 11.6：提交**

```bash
git add src/lib/types.ts src/lib/ipc.ts src/store/envStore.ts src/components/environments/EnvironmentDialog.tsx
git commit -m "feat: environment type selector (ssh/k8s) in environment dialog"
```

---

## 手动验收（可选，有测试集群时）

1. 新建环境类型选「Kubernetes 宿主机」，填宿主机 SSH 凭证，保存。
2. 对话输入「检查 xx 服务的内存」→ Agent 应调 `k8s_find_pods` → 多实例时询问用户。
3. 选定 Pod 后 Agent 调 `list_processes(pod=...)`（容器内 ps，PID 为容器内 PID）→ `ensure_tool(pod=...)`（容器自带 jcmd 则零上传；否则观察 musl 检查 → 通道 B 两跳上传 → `chgrp ossgroup`）→ `jvm_gc_stats(pod=...)`。
4. 验证 Pod 内 `/opt/log/dump/heapdump/friday-tools/` 属组为 `ossadm:ossgroup`。
5. `jvm_gc_stats` 传 1s 超时 → 验证容器内 jcmd 进程被补刀（`ps -ef | grep jcmd` 无残留）。
6. VM 环境全流程回归（不带 pod 参数）：与改造前行为一致。

## 自审记录（writing-plans Self-Review）

- **Spec 覆盖**（Phase 1 范围内）：通道组合与参数分发（Task 3）、工具参数管道与 schema（Task 4/5）、k8s_find_pools 动态发现（Task 9）、工具包进 Pod 全链（Task 1/2/6/7/8：固定目录/chgrp/--no-same-owner/musl/容器自带优先/幂等/失败清理）、超时补刀（Task 3/4/5）、环境配置类型（Task 10/11）、多容器默认第一个（kubectl 默认语义，Task 1 wrap 不传 -c）。堆 dump 空间检查/dump 目录/JFR 路径、两跳 download、arthas —— Phase 2。
- **占位符扫描**：无 TBD/TODO；所有代码块完整可抄。
- **类型一致性**：`TargetKey::from_parts/k8s/base`、`cache_key(env, pod, container)`、`build_transport(env_id, &env, pod, container)`、`get_or_create(env_id, pod, container, db)`、`exec_jdk_command(session_id, &target, ...)` 全文一致；`From<String>` 保证既有测试 `insert_channel(String, ..)` 兼容。
