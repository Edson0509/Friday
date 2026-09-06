<div align="center">

<img src="app-icon.svg" width="110" alt="Friday logo" />

# Friday

**面向软件开发人员的远程环境运行时故障诊断 Agent**

[![Release](https://img.shields.io/github/v/release/Ewan-90n9/Friday)](https://github.com/Ewan-90n9/Friday/releases)
[![Platform](https://img.shields.io/badge/platform-Windows-blue)](https://github.com/Ewan-90n9/Friday/releases/latest)

</div>

---

## Friday 是什么

Friday 是一个跑在你本地电脑上的桌面应用（当前支持 Windows）。你只需要告诉它"哪个环境、哪个服务、什么症状"，比如：

> xx.xx.xx.xx 环境 OOMService OOM 了，帮我定位

它会自动通过 SSH 连接目标环境，像一位熟练的值班工程师那样逐步执行诊断：列出 JVM 进程、采集 GC 统计、抓线程转储、生成堆快照并用 MAT 分析泄漏疑点、录制 JFR 飞行记录、attach Arthas 动态观测方法耗时……过程中每一步工具调用都在界面上实时可见、风险操作先经你确认，最终给出根因分析与结论。

一次典型的 OOM 诊断长这样：

1. Friday 通过 `friday_list_environments` / `friday_list_processes` 找到目标机器上的 JVM 进程
2. `friday_jvm_heap_info` / `friday_jvm_class_histogram` 初步判断内存构成
3. `friday_jvm_heap_dump` 生成堆快照（高风险操作，会先请求你确认），完成后自动拉回本地并用 MAT 建好索引
4. Agent 调用 `friday_heap_leak_suspects` / `friday_heap_dominator_tree` / `friday_heap_path_to_gc_roots` 定位泄漏对象及其引用链
5. 结合业务代码给出结论与修复建议，全程可追问、可补充信息

你不需要提前在目标机上安装任何诊断工具：MAT / JMC 分析引擎托管在 Friday 本地，JDK 工具链按需自动部署到目标环境临时目录，Arthas 包随应用分发、使用时自动上传。Friday 复用你本机已安装的 Agent CLI（opencode 或 codeagentcli）作为诊断大脑，无需额外配置 API Key。

![Friday 主界面](docs/assets/readme-main.png)

## 核心能力

### 环境连接与命令执行

SSH 单通道直连目标环境（K8s 场景同样经 SSH 执行 kubectl，无需额外开放端口）；连接按环境池化复用，空闲 10 分钟自动断开。密码类凭证存 OS 密钥链，私钥直接引用 `~/.ssh/` 路径；同一环境可配置多套用户凭证。

| 工具 | 用途 | 确认级别 |
|------|------|----------|
| `friday_list_environments` | 列出已配置的环境供 Agent 选择 | 只读，自主执行 |
| `friday_ensure_tool` | 按需部署 JDK 诊断工具链到目标机（`/tmp/friday-tools`） | 低风险，确认后执行 |
| `friday_run_command` | 任意 shell 命令兜底，覆盖未结构化的检查 | 高风险，强制确认 |

### JVM 基础诊断

封装 jps / jstat / jcmd，自动检测目标机 JDK 路径与版本，输出结构化结果。

| 工具 | 用途 | 确认级别 |
|------|------|----------|
| `friday_list_processes` | 列出目标环境的 Java 进程（PID / 主类 / 参数） | 只读 |
| `friday_jvm_gc_stats` | GC 统计（分代使用率 / GC 次数与耗时） | 只读 |
| `friday_jvm_heap_info` | 堆内存分布（分代容量与占用） | 只读 |
| `friday_jvm_vm_info` | JVM 版本、启动参数、系统信息 | 只读 |
| `friday_jvm_thread_dump` | 线程转储（锁与阻塞分析） | 只读 |
| `friday_jvm_class_histogram` | 类直方图，快速定位大对象 | 低风险 |
| `friday_jvm_heap_dump` | 生成堆快照并自动拉回本地 | 高风险，强制确认 |

### 堆快照分析（MAT 引擎）

内置 Eclipse MAT（Memory Analyzer）分析引擎：堆快照拉回完成后自动建索引，Agent 直接调用分析工具定位泄漏；分析会话 LRU 管理、空闲 15 分钟自动回收。

| 工具 | 用途 | 确认级别 |
|------|------|----------|
| `friday_heap_open` | 打开（预热）指定堆快照 | 只读 |
| `friday_heap_leak_suspects` | 泄漏疑点报告 | 只读 |
| `friday_heap_dominator_tree` | 支配树（retained 排序，可逐层下钻） | 只读 |
| `friday_heap_histogram` | 类直方图（retained / shallow / 实例数排序） | 只读 |
| `friday_heap_object_info` | 对象详情（类、字段、大小） | 只读 |
| `friday_heap_path_to_gc_roots` | 对象到 GC root 的完整引用链 | 只读 |
| `friday_heap_references` | 出向 / 入向引用列表 | 只读 |
| `friday_heap_threads` | 线程栈与本地变量分析 | 只读 |
| `friday_heap_close` | 关闭分析会话，释放内存 | 只读 |

### JFR 飞行记录（JMC 引擎）

内置 JMC 分析引擎。`jfr_record` 通过 jcmd 在目标 JVM 上定时录制（无需重启进程），结束自动拉回本地并预热，随后 Agent 用 21 个分析工具做多维诊断：

| 工具（节选） | 用途 | 确认级别 |
|------|------|----------|
| `friday_jfr_record` | 远程定时录制 JFR，自动拉回 | 低风险 |
| `friday_jfr_quick_analysis` | 一键智能诊断 | 只读 |
| `friday_jfr_rules` | JMC 规则引擎全量扫描 | 只读 |
| `friday_jfr_gc_detail` | GC 详情 | 只读 |
| `friday_jfr_hot_methods` | CPU 热点方法 | 只读 |
| `friday_jfr_thread_contention` | 锁竞争分析 | 只读 |
| `friday_jfr_deadlock_detection` | 死锁检测 | 只读 |
| `friday_jfr_memory_leaks` / `friday_jfr_predictive_leak` | 内存泄漏 / 预测性泄漏 | 只读 |
| `friday_jfr_compare` | 两份录制 A-B 对比 | 只读 |

其余分析工具共 22 个：录制概览、异常/错误分析、IO 热点、safepoint、虚拟线程、分配热点、调用栈检索、指标相关性、请求瀑布图等，可在应用内工具面板查阅全量列表。

### Arthas 动态诊断

对接 Arthas 4.x 官方内置 MCP Server：Friday 经 SSH exec 通道 HTTP 桥代理通信，**无需目标机开放 TCP 转发**（`AllowTcpForwarding no` 的加固环境同样可用）。Arthas 包随应用分发，attach 时 SFTP 自动上传到目标机；同一 JVM 并发去重，空闲 15 分钟自动退出。

| 工具（节选） | 用途 | 确认级别 |
|------|------|----------|
| `friday_arthas_open` | attach 到目标 JVM | 低风险 |
| `friday_arthas_dashboard` | 实时面板（线程 / 内存 / GC） | 只读 |
| `friday_arthas_thread` | 线程分析（CPU / 死锁 / 阻塞） | 只读 |
| `friday_arthas_sc` / `friday_arthas_sm` | 查找类 / 方法 | 只读 |
| `friday_arthas_jad` | 反编译指定类 | 只读 |
| `friday_arthas_watch` | 观察方法出入参与异常 | 低风险 |
| `friday_arthas_trace` / `friday_arthas_stack` | 调用链耗时 / 调用栈 | 低风险 |
| `friday_arthas_ognl` / `friday_arthas_vmtool` | 表达式取值 / 强制 GC 等 | 低风险 |
| `friday_arthas_profiler` | async-profiler 火焰图 | 低风险 |
| `friday_arthas_close` | 停止并卸载 Arthas | 只读 |

共 27 个工具，另含 monitor / tt / mbean / classloader / vmoption / getstatic / dump 等。

### 文件传输

专用 SSH 连接后台异步传输，不阻塞诊断对话；断点续传、失败自动重试（5 次 / 2 小时预算）、聊天流内实时进度卡片。堆快照与 JFR 录制产物完成后自动拉回本地。

| 工具 | 用途 | 确认级别 |
|------|------|----------|
| `friday_file_download` | 目标机 → 本地 | 低风险 |
| `friday_file_upload` | 本地 → 目标机 | 高风险，强制确认 |
| `friday_transfer_status` | 查询传输进度 | 只读 |
| `friday_transfer_cancel` | 取消传输 | 只读 |
