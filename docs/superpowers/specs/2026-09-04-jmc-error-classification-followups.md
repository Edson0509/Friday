# JMC 分析错误分类修复与遗留事项（issue #10）

- 日期：2026-09-04
- 状态：**全部已实施**——方案 1+2（当日落地）+ 方案 3（构建期补丁，2026-09-06 落地，见 §3.1 更新）
- 关联 issue：#10（jfr_thread_cpu 报 `-32603 Internal error` 并拖垮 JMC 工人进程）、#11（jfr 虚拟线程分析失效，同批补丁根治，见 §3.1.1）
- 相关设计：[JFR 飞行记录分析设计](2026-09-03-jmc-jfr-analysis-design.md)（JMC 工人进程/vendoring 模式）

## 1. 根因回顾

**上游 bug**：vendored 的 [scarletbean01/jmc-mcp-server](https://github.com/scarletbean01/jmc-mcp-server) @ `e6c3567` 有 6 个工具把可选参数 `top_n` 声明为原始类型 `int`（其余工具正确用 `Integer` + 判空）。缺省或显式传 null 时，Quarkus 生成的 invoker 对 null 拆箱 → `NullPointerException: Argument N is null, int expected` → JSON-RPC `-32603`。

上游 6 处（03-infrastructure/.../infrastructure/mcp/）：

| 文件 | 行 | Friday 是否代理 |
|---|---|---|
| `ThreadCpuTool.java` | 35 | 是（jfr_thread_cpu，issue #10 崩溃点） |
| `ThreadContentionTool.java` | 31 | 是（jfr_thread_contention） |
| `VirtualThreadsTool.java` | 33 | 是（jfr_virtual_threads） |
| `ObjectStatisticsTool.java` | 31 | 否 |
| `ThreadPoolAnalysisTool.java` | 32 | 否 |
| `VMOperationsTool.java` | 32 | 否 |

**Friday 侧放大器**：`jfr/client.rs` 曾把所有 rmcp `ServiceError` 一律当传输层错误 → `manager.rs` 判定 worker 死亡 → invalidate 整个 JMC 进程（丢上游录制缓存）。但 `-32603` 是服务器**正常应答**的 JSON-RPC 错误响应（`ServiceError::McpError(ErrorData)`，与 `TransportClosed` 等真传输错误可区分），worker 其实活着。

## 2. 已实施修复（方案 1+2）

1. **top_n 缺省兜底**（`src-tauri/src/tools/builtin/jfr/mapping.rs`）：`JfrProxyKind::needs_top_n_default()` 标记 3 个受影响 kind；`build_proxy` 对它们在 top_n 缺省或显式 null 时注入上游文档默认值 `10`（显式传值不覆盖）。
2. **错误分类纠偏**（`src-tauri/src/jfr/client.rs`）：抽出纯函数 `map_call_result`——`ServiceError::McpError`（JSON-RPC 错误响应 = worker 存活）→ 工具级错误 `is_error=true` 业务透传；仅 `TransportClosed` 等真传输错误走 `Err` → invalidate。

回归测试：`test_build_proxy_injects_top_n_default_for_affected_kinds`、`test_map_call_result_mcp_error_is_tool_level_error`、`test_map_call_result_transport_error_still_err`、`test_map_call_result_complete_and_non_complete`。

## 3. 遗留事项

> 方案 2 落地后，NPE 的爆炸半径已缩小：即使再踩中拆箱 bug 也只是**该工具本次调用失败**（业务错误透传），不再杀工人进程。剩余影响：绕过 `build_proxy` 直接调上游 JAR 的调用方仍必崩；Friday 未代理的 3 个上游工具（ObjectStatistics / ThreadPoolAnalysis / VMOperations）直接调用必崩。

### 3.1 上游 `int topN` 根因仍在（方案 3 未做）→ ✅ 已根治（2026-09-06）

采用**构建期补丁**路径（照抄 issue #9 的 analyzer 模式）：

- `scripts/jmc-topn-int-fix.patch`：5 个工具类（ThreadCpu / ThreadContention / ObjectStatistics / ThreadPoolAnalysis / VMOperations）`int topN` → `Integer topN` + 调用点判空默认 10（对齐 HotMethodsTool 既有写法）。
- `scripts/jmc-virtual-threads-fix.patch`：VirtualThreadsTool 的 top_n 修复随 issue #11 重写一并落地（见 §3.1.1），故不重复出现在 topn 补丁中。
- `jmc-jar.yml` 在 clone 后 `git apply --check` fail-fast 应用两补丁；`paths` 触发器加入补丁文件（补丁变更即自动重建发布）。
- smoke test 增补：CI 现场生成虚拟线程 fixture（GenFixture：profile 设置 + VTStart/VTEnd 开启 + 持 monitor park + 忙循环），断言 ① `threadCpu`/`virtualThreadTool` 不传 top_n 不再返回 -32603（#10 回归）② `virtualThreadTool` 报出 `Virtual threads detected` + `Pinning Summary`（#11 回归）。

**收尾清单**（已全部执行）：

1. ~~推送补丁/workflow 变更即自动重建~~（`jmc.upstream_sha` 不变，无 SHA 同步事项）。
2. ~~移除 `mapping.rs` 的 `needs_top_n_default()` 与 `build_proxy` 注入逻辑 + 对应回归测试~~——替换为反向回归测试 `test_build_proxy_no_top_n_injection_after_upstream_fix`（断言不再注入、显式 null 原样透传）。
3. 未来升级上游 SHA 前先本地 `git apply --check` 验证两补丁仍命中（上游布局可能变化）。

#### 3.1.1 issue #11：jfr 虚拟线程分析功能（同批根治）

**根因**（两层）：

1. **JMC 解析器丢弃 `Thread.virtual` 字段**：JFR 的 thread 类型元数据自 JDK 21 GA 起就带 `virtual` boolean（实测 jdk-21-ga `metadata.xml`），但 JMC 把 thread 类型映射到固定内部类 `StructTypes.JfrThread`（无 `virtual` 成员），解析时丢值并告警 `Could not find field with name 'virtual' in reader for 'thread'`（截至 JMC 9.1.2/master 仍未支持）。JMC 管线（IItemCollection）因此**完全无法识别虚拟线程**。
2. **上游 VirtualThreadsService 查询幻觉事件**：`jdk.VirtualThreadSleepFailed` 在任何 JDK 中都不存在；`jdk.VirtualThreadSubmitFailed` 的 `exception` 字段也不存在（实际是 `exceptionMessage`）。结果工具只会报 pinning/失败事件，无 pinning 时输出 "No virtual thread pinning or failure events found"——活跃但健康的虚拟线程负载被误判为"无数据"。

**修复**（`jmc-virtual-threads-fix.patch`，~730 行）：

- `VirtualThreadsService` 重写为 **jdk.jfr.consumer**（JDK 自带解析器，按录制文件元数据动态解析字段，`RecordedThread.getValue("virtual")` 对任何 JDK 21+ 录制可用，包括 worker JVM 比目标 JVM 旧的场景）：
  - 存在性与规模：distinct 虚拟线程计数（事件线程 virtual=true + VTStart/SubmitFailed 的 `javaThreadId` 字段，上限 10000 防炸内存）、按事件类型活动分布
  - pinning：计数、总耗时、top 栈、`pinnedReason`/`carrierThread`（JDK 24+ 字段，存在即用）
  - 失败：SubmitFailed（`exceptionMessage` 字段）、SleepFailed（保留查询，兼容自定义事件）
  - 时间窗过滤语义对齐 `JfrProviderImpl.parseTimeQuantity`（ISO-8601 → epoch-ms → epoch-s，忽略小数值）
- `VirtualThreadsApplicationService.analyzeFile`：保留 `loadRecording` 调用（路径校验 + JMC 缓存预热），再走 RecordingFile 二次解析
- `VirtualThreadsTool`：`Integer topN`（#10 根治）+ 全新 markdown 渲染（空结果区分"录制无 virtual 字段（JDK<21）"与"有字段但无虚拟线程活动"两种语义）
- 本地验证：上游 fixture 单测 18/18 通过；Java 21 端到端（补丁 → uber-jar → MCP stdio tools/call）报出 `Virtual threads detected: **2 distinct**` + Pinning Summary（192.4ms）

**经验**：`new Recording()` 后调 `enable()` 会**替换**整个设置映射（默认 profile 事件全部失效）——fixture 生成需先 `setSettings(Configuration.getConfiguration("profile").getSettings())` 再叠加 enable。

### 3.2 MAT heap 分析器存在同款误分类（未动）

`src-tauri/src/analyzer/client.rs:142`（McpHeapAnalyzerClient::call_tool）仍是旧模式：所有 rmcp 错误（含 `ServiceError::McpError` JSON-RPC 错误响应）统一 `Err(...)` → `analyzer/manager.rs:265-268` 判 Unavailable → **invalidate 工人进程**。MAT 有会话层（LRU 3、建索引预热分钟级），一旦误杀，所有已打开 dump 会话全丢、需重新预热，代价比 JMC（无会话层）更重。

**现状理由**：暂无上游（Djaler/jvm-heap-dump-mcp，v0.2.0 基底）返回 JSON-RPC 错误响应的报错证据，故未动；修复模式已在 jfr 侧验证就绪。

**触发即修**：一旦 MAT 侧出现 `-3260x` / "Mcp error" 类日志（`heap_analyzer` target）或用户报同类 issue，照抄 jfr 修复——

1. `analyzer/client.rs` 抽出同款 `map_call_result`（McpError → `is_error=true` 透传；其余 Err → 传输错误）。
2. 补同款三个单测（McpError 工具级 / TransportClosed 传输级 / Complete + 非 Complete）。
3. `extract_text` 与 CallOutcome 语义与 jfr 侧一致，可直接复用 `analyzer::client` 内现有函数。

也可预防性顺手修（改动小、模式已验证），修时同步更新本文状态。注意：issue #9 已将 analyzer 切到本仓库自建管线（tag `analyzer-v0.2.0-friday` + 补丁），与错误分类是两回事，互不影响。

## 4. rmcp 错误分类速查（3.1/3.2 都要用）

rmcp 3.1.4 `ServiceError` 关键变体：

| 变体 | 含义 | 应归为 |
|---|---|---|
| `McpError(ErrorData)` | 服务器返回的 JSON-RPC 错误**响应**（-32602 invalid params / -32603 internal error 等） | 工具级错误（worker 存活，业务透传） |
| `TransportClosed` / `TransportSend(_)` | 传输层断开/发送失败 | 传输错误（invalidate + 懒重建） |
| `Timeout` / `Cancelled` | rmcp 层超时/取消 | 视调用方超时策略（Friday 用 tokio::time::timeout 外包，正常不出现） |

`ErrorData` 实现 `Display`（`{code}: {message}({data})`），构造用 `ErrorData::internal_error(msg, data)` 等。
