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
