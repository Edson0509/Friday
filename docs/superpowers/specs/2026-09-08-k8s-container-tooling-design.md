# 容器环境（K8s）诊断工具适配设计

日期：2026-09-08
状态：已评审通过
关联决策：连接模型 = SSH 宿主机 + kubectl exec；工具包固定落 `/opt/log/dump/heapdump/friday-tools/`；属组 ossadm:ossgroup

## 背景

当前所有诊断工具（jvm_*、文件传输、heap dump、JFR、arthas）都假定直连虚拟机：`ExecChannel` 唯一实现是 `SshTransport`，命令在目标机本地执行，文件经 SFTP 直传。现在要扩展到 **Kubernetes 容器环境**。

用户确认的连接模型：**Friday → SSH 到宿主机 → kubectl exec 进 Pod**。保留现有 SSH 通道与凭证体系，命令层加 kubectl 包装。

### 用户确认的关键约束

1. **Pod 动态发现**：用户输入"检查 xxxservice 的内存"，Agent 先 `kubectl get pods -A -o wide | grep xxxservice` 发现 Pod；多实例时询问用户选哪一个
2. **工具包进 Pod**：与 VM 场景一样上传 JDK/arthas；**固定目录** `/opt/log/dump/heapdump/friday-tools/`（该目录不会导致 Pod 被驱逐，不探测不决策）
3. **文件属组**：Pod 内所有 Friday 写入的文件必须 `chown ossadm:ossgroup`，否则 JVM 用户（ossadm）无法使用
4. **五类工具全做**：基础 JVM 工具、文件传输、heap dump、JFR、arthas
5. **逐命令兼容性清单**是交付物之一（分析按工具家族逐命令做，实现统一走通道层）
6. **端口访问走正向代理**：`kubectl port-forward` + 现有 SSH 隧道链。**本期不做反向代理**（容器主动外连的场景目前不存在，TunnelManager 已是扩展点）
7. 数据回传不依赖容器内有 curl/wget/tar 之外的额外工具

## 方案选型

三个候选：**A. 通道装饰器 K8sChannel**（推荐、已采纳）；B. 每工具家族独立 k8s 代码路径（五套重复，违背代码复用）；C. 绕过 SSH 直连 K8s API（新凭证体系，改动最大）。

采纳 A 的核心理由：五类工具只见 `ExecChannel` trait，容器语义全部收进通道装饰器后，**工具层逻辑几乎零改动**。"每类工具单独分析"落在兼容性矩阵（本文各节），而不是五套重复代码。

## 总体架构

### 通道组合

```
build_transport（exec/pool.rs 唯一分发点）
  ├─ transport_type = "ssh" → SshTransport                     （现状不变）
  └─ transport_type = "k8s" → SshTransport(宿主机) 外包 K8sChannel(pod, container)
```

`K8sChannel implements ExecChannel`（新文件 `src-tauri/src/exec/k8s.rs`）：

| 方法 | 实现 |
|---|---|
| `run(cmd)` | `base.run("kubectl exec <pod> [-c <ctr>] -- sh -c '<cmd>'")`。`container` 缺省时省略 `-c`（使用 Pod 默认容器）。容器内用 `sh -c`（busybox 无 bash），宿主机侧仍走现有 `bash -lc` 包装 |
| `upload(local, remote)` | 两跳：SFTP → 宿主机 staging（`/tmp/friday-tools/staging/`）→ `kubectl exec -i <pod> -c <ctr> -- sh -c 'cat > <remote>' < staging文件` → `chown ossadm:ossgroup <remote>`。chown 失败视为上传失败并清理目标文件（与"失败不留半截"一致） |
| `download(remote, local, offset, progress)` | 两跳（见"文件拷出"节）：`kubectl exec <pod> -c <ctr> -- cat <remote> > 宿主机staging文件` → 现有 SFTP 下载（offset/progress 原样透传） |
| `connect/disconnect/is_alive` | 委托 base SSH 通道；超时杀进程沿用"断 SSH 连接"机制（kubectl exec 子进程随之死亡，语义不变） |

- **池 key** 从 `(env_id)` 扩为 `(env_id, pod: Option<String>, container: Option<String>)`；空闲回收、测试注入逻辑不变
- `EnvironmentInfo` 增加 `transport_type` 透传（行内已有该列，只是没带到结构体）

### 工具参数管道

- 所有远端工具（jvm_* / file_* / heap_dump / jfr_record / arthas_*）schema 增加**可选** `pod`、`container` 参数。VM 模式忽略；k8s 模式必填——缺失时报错并提示 Agent 先调 `k8s_find_pods`
- `resolve_environment` 一处统一解析 pod/container → 池取通道，工具内部零改动
- **新增工具 `k8s_find_pods(pattern)`**：`kubectl get pods -A -o custom-columns=NAME:.metadata.name,NS:.metadata.namespace,STATUS:.status.phase,CONTAINERS:.spec.containers[*].name,NODE:.spec.nodeName`，pattern 过滤在 Friday 侧 Rust 完成（不用 shell grep，列格式稳定可解析），结构化输出（pod / namespace / container 列表 / status / node）。pattern 仅做包含匹配（大小写不敏感）。多实例由 Agent 询问用户
- `list_processes` 经装饰器自动变为"容器内 ps"，无需改造
- 新工具归入 `ToolCategory` 新分组（如 `K8s`），注册时声明 category（既有约定）

### 环境配置

- 环境编辑弹窗增加**环境类型**选择：`宿主机 SSH`（默认）/ `Kubernetes 宿主机`。选 k8s 时 `transport_type='k8s'`，其余字段（host/port/SSH 凭证）复用宿主机 SSH 语义，无新凭证类型
- `k8s_namespace` 列（已存在未使用）作为可选的搜索范围提示；`kubectl get pods -A` 默认全命名空间
- `save_environment_cmd` 校验逻辑同步（k8s 环境不要求额外字段）

## 深入分析 ①：工具包进 Pod

**固定路径**：`/opt/log/dump/heapdump/friday-tools/`（JDK、arthas、后续新工具统一于此；heap dump/JFR 产物直接落 `/opt/log/dump/heapdump/`）。该目录挂载于不会触发 ephemeral-storage 驱逐的卷，不探测、不决策链。

**上传管道**（容器内依赖仅 sh+tar）：

```
JdkPackage.ensure（宿主机，现有实现）→ tar czf → SFTP 到宿主机 staging
→ kubectl exec -i <pod> -c <ctr> -- sh -c 'mkdir -p <dir> && tar xz -C <dir>'
→ chown -R ossadm:ossgroup <dir>（tar 解出的文件属主是 exec 用户，必须修正）
```

- **优先用容器自带工具**：执行前 `command -v jcmd` 一次检查，命中则零上传（不属过度设计，与 jdk_cache miss→ensure 既有流程对齐）
- **幂等**：`<dir>/.friday-ok` 标记文件记录版本+checksum；Pod 重启内容丢失后自动重装
- **失败不留半截**：tar/chown 失败即 `rm -rf` 本次解压目录
- **执行位**：tar 保留模式位；chown 后补 `chmod -R u+rx` 兜底
- JdkCache key 加 pod 维度：`(env_id, pod, container) → JdkLayout`

**逐命令兼容性清单（基础 JVM 工具）**：

| 命令 | VM 模式 | K8s 模式适配 |
|---|---|---|
| `jstat -gcutil <pid>` | 宿主机 jdk 路径 | 容器内 jdk 路径（自带或 friday-tools），经装饰器 exec |
| `jcmd <pid> ...` | 同上 | 同上；GC.heap_dump / JFR.start 的 `filename=` 指 Pod 内路径 |
| `ps -eo pid=,user=,args=` | 宿主机 | 容器内；busybox ps 无 `-eo` 时降级 `ps -o pid,user,args` 或 `/proc` 扫描，报文格式差异在解析层吸收 |
| `stat -c %s <path>` | 宿主机 | 容器内；busybox stat 支持 `-c %s`；无 stat 的镜像降级 `wc -c <` |
| 超时杀进程 | pool.disconnect | 相同（SSH 断开 → kubectl exec 死亡 → 容器内进程收到 SIGHUP） |

## 深入分析 ②：容器内文件拷出

**两跳管道**，leg2 完全复用现有 TransferManager（断点续传 / 进度 / 完成钩子 / 5 次重试 / 2h 预算全部保留）：

```
Pod 内文件 ──leg1──▶ 宿主机 staging ──leg2──▶ Friday 本地 artifacts
      kubectl exec cat >            现有 SFTP 下载
```

- **leg1 用 `cat` 重定向而非 kubectl cp**：重定向发生在宿主机 bash 上，数据**不流经 Friday 内存**（dump 可达 GB 级，`ExecOutput` 是整段缓冲的 String，绝不走 SSH stdout 回传）；容器内只需 `cat`（比 kubectl cp 更通用，cp 需要容器内有 tar）
- **leg1 进度**：先 `stat` 得 Pod 内源文件大小 → 每秒轮询宿主机 staging 文件大小 → 与 leg2 的 SFTP 进度拼接为完整进度条
- **leg1 完整性**：完成后对比源大小；失败重试时 leg1 整段重跑（宿主机本地，快），leg2 照旧 .part 续传
- **清理**：全部成功后删 Pod 内源文件（消灭驱逐源）+ 删宿主机 staging。Pod 内删除走 `channel.run`（装饰器），现有 `cleanup_remote_on_success` 语义不变
- **实现位置**：`K8sChannel::download/upload` 内部 → TransferManager、下载完成钩子（.hprof→MAT 预热 / .jfr→JMC 预热）**一行不改**
- 拷出方向属组无关紧要；拷入方向（file_upload）完成后必须 chown（见总体架构 upload 行）

## 深入分析 ③：正向代理隧道

**链路**（复用现有 TunnelManager）：

```
Friday 本地端口 ──SSH direct-tcpip──▶ 宿主机 127.0.0.1:P ──kubectl port-forward──▶ Pod 端口
              TunnelManager(现有)          宿主机长驻 kubectl 进程
```

### 启动时序

1. 宿主机起 port-forward（经 base.run，nohup 后台化）：
   `nohup kubectl port-forward pod/<pod> -n <ns> -c <ctr> --address 127.0.0.1 0:<目标端口> > /tmp/friday-tools/pf-<pod>-<port>.log 2>&1 & echo $!` → 记录 PID
2. 轮询 log 等待 `Forwarding from 127.0.0.1:<P> -> <port>` 行（超时 10s 报错并 kill）→ 得到宿主机侧随机端口 P（端口 0 让 kubectl 自选，避免冲突）
3. 复用 `TunnelManager.forward(env, "127.0.0.1", P)` → Friday 本地监听端口 L
4. 健康检查：Friday 直接 GET `http://127.0.0.1:L/mcp` 通过后，以 `K8sTunnelSession { pf_pid, tunnel_handle }` 整体交付

### 关键点

- **职责分工**：kubectl port-forward 解决"宿主机 → Pod"（kubelet 从节点侧拨 podIP，Pod 内绑 127.0.0.1 的服务只有它到达得了）；TunnelManager 解决"Friday → 宿主机"
- **数据双向性**：Friday 是主动方，arthas 分析结果作为 HTTP 响应沿**同一条已建立的 TCP 连接**流回，容器内不需要任何主动外连——这就是"反向代理不必须"的原因
- **rmcp 无感知**：`StreamableHttpClient` 打 `http://127.0.0.1:L/mcp`，与 VM 模式 exec-curl 桥对外接口一致
- **安全性**：宿主机侧绑 127.0.0.1（仅 SSH 凭证可入）；Pod 内 MCP 绑 `0.0.0.0` 但有 Bearer token（k8s 模式专属；VM 模式维持 127.0.0.1）；Pod IP 暴露面限于集群内网

### 生命周期与故障处理

| 场景 | 处理 |
|---|---|
| arthas 会话正常关闭 | 先 `kill <pf_pid>` 再关 direct-tcpip，顺序固定 |
| 空闲 15min 回收 | 复用 ArthasManager 现有 LRU，close 走同一路径 |
| Pod 重启/消失 | pf 进程自退；隧道连接失败 → 现有 `mark_failed`；下次 query 懒重建（现有 invalidate 模式） |
| 残留 pf 进程防护 | 启动前 `pkill -f "kubectl port-forward pod/<pod>"`（对齐 arthas 残留实例清理既有模式） |
| pf 启动失败/log 解析超时 | kill 兜底 → 报错；容器内有 curl 时可降级 exec-curl 桥（经装饰器自动在容器内执行） |
| Friday 进程退出 | nohup 的 pf 残留 → 下次会话启动前的 pkill 防护兜底 |

## 深入分析 ④：五类工具家族落点

| 家族 | 改动 |
|---|---|
| 基础 JVM 工具 | **零逻辑改动**。JDK 解析：容器内 `command -v jcmd` 优先 → 没有则 ensure_tool 走分析①上传链；JdkCache key 加 pod 维度 |
| 文件传输 | TransferManager **一行不改**（只认 ExecChannel 的 upload/download，装饰器已实现两跳）；`validate_remote_path` 仍校验 Pod 内路径 |
| heap dump | k8s 模式 `remote_path` 默认 `/opt/log/dump/heapdump/friday-heapdump-{pid}-{ts}.hprof`；生成前 `GC.heap_info` 估算 + 空间检查，不足报错不落盘；拉回成功后清 Pod 内源文件；MAT 本地分析不变 |
| JFR | 同 heap dump 模式：录制落 `/opt/log/dump/heapdump/`，两跳拉回，JMC 本地分析不变 |
| arthas | attach 在 Pod 内执行（exec + nohup；arthas 包走分析①上传链）；k8s 模式 MCP 绑 0.0.0.0 + Bearer；通信走正向隧道（分析③），exec-curl 桥为降级路径；用户对齐简化——跳过 SSH 凭证对齐，改用 `kubectl exec --user`（默认容器用户即 JVM 用户 ossadm，仅探测到不一致时才加 `--user`）；会话 key 加 pod 维度：`(env_id, pod, container, pid)` |

## 错误处理

- **两跳错误必须透传是哪一跳失败**（"kubectl exec cat 失败：容器内无 cat" vs "SFTP 下载失败"），不合并笼统报错
- `resolve` 时 pod 缺失 → 错误信息指导 Agent："该环境是 Kubernetes 宿主机，请先用 k8s_find_pods 定位 Pod"
- 容器内依赖缺失（无 sh/tar/cat）→ 明确报"镜像 distroless 不支持，需要 xx 依赖"
- 遵循日志规范：每个新 command/工具入口 `#[instrument]`；kubectl 子命令失败时 stderr 全量记录；不脱敏不截断

## 测试策略

- **单元**（mock ExecChannel，沿用 pool 测试的 insert_channel 模式）：K8sChannel 命令包装（kubectl exec 拼装、sh -c 转义、--user 注入）；两跳 upload/download 的编排逻辑；池 key 扩展后的命中/回收；k8s_find_pods 输出解析（含多实例、异常行）
- **单元**（纯字符串）：port-forward log 解析、pkill 模式、staging 路径构造
- **集成（手动，测试集群）**：发现→选定 Pod→jvm_gc_stats→heap_dump 拉回→MAT 预热→arthas attach→隧道 MCP 调用→会话关闭清理（pf 进程消失、Pod 内文件清理）
- 回归：VM 模式全部现有测试必须零改动通过（transport_type 分发隔离）

## 非目标（本期不做）

- 反向代理（容器主动外连）——TunnelManager 是未来扩展点
- docker exec / containerd / Podman 直连
- distroless（无 shell）镜像支持——明确报错
- 直接对接 K8s API（kubeconfig/token 凭证体系）
- Pod 内工具的 jlink 瘦身（固定目录已无驱逐风险，YAGNI）
