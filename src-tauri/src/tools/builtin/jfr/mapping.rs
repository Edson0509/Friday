use serde_json::{json, Value};

/// 代理型分析工具的 Friday → 上游映射（Compare 单独走 build_compare）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JfrProxyKind {
    Overview,
    Rules,
    QuickAnalysis,
    GcDetail,
    MemoryLeaks,
    PredictiveLeak,
    AllocationHotspots,
    HotMethods,
    ThreadCpu,
    CpuFlame,
    ThreadContention,
    DeadlockDetection,
    IoHotspots,
    Exceptions,
    Errors,
    Safepoints,
    VirtualThreads,
    StackTraceSearch,
    Correlate,
    RequestWaterfall,
}

impl JfrProxyKind {
    /// 上游实际注册名是 lowerCamelCase（Quarkus MCP 从 Java 方法名派生，与上游
    /// README 文档的 snake_case 不符，实测 tools/list 得出；参数名则保持 snake_case）。
    /// 注意两个特例：VirtualThreads → virtualThreadTool、compare → compareRecordings。
    pub fn upstream_name(&self) -> &'static str {
        match self {
            JfrProxyKind::Overview => "jfrOverview",
            JfrProxyKind::Rules => "jfrRules",
            JfrProxyKind::QuickAnalysis => "smartQuickAnalysis",
            JfrProxyKind::GcDetail => "gcDetail",
            JfrProxyKind::MemoryLeaks => "memoryLeaks",
            JfrProxyKind::PredictiveLeak => "smartPredictiveLeakAnalysis",
            JfrProxyKind::AllocationHotspots => "allocationHotspots",
            JfrProxyKind::HotMethods => "hotMethods",
            JfrProxyKind::ThreadCpu => "threadCpu",
            JfrProxyKind::CpuFlame => "cpuFlame",
            JfrProxyKind::ThreadContention => "threadContention",
            JfrProxyKind::DeadlockDetection => "deadlockDetection",
            JfrProxyKind::IoHotspots => "ioHotspots",
            JfrProxyKind::Exceptions => "exceptionAnalysis",
            JfrProxyKind::Errors => "errorAnalysis",
            JfrProxyKind::Safepoints => "safepointAnalysis",
            JfrProxyKind::VirtualThreads => "virtualThreadTool",
            JfrProxyKind::StackTraceSearch => "smartStackTraceSearch",
            JfrProxyKind::Correlate => "smartCorrelate",
            JfrProxyKind::RequestWaterfall => "smartRequestWaterfall",
        }
    }
}

/// jfr_record 参数校验：duration_secs（10..=600，默认 60）+ settings 白名单（默认 profile）。
/// Err(String) → invalid_args。
pub fn validate_record_params(args: &Value) -> Result<(u32, String), String> {
    let duration = match args.get("duration_secs").and_then(|v| v.as_i64()) {
        None => 60,
        Some(n) if (10..=600).contains(&n) => n as u32,
        Some(n) => return Err(format!("duration_secs 必须在 10~600 之间，收到 {n}")),
    };
    let settings = match args.get("settings").and_then(|v| v.as_str()) {
        None | Some("profile") => "profile".to_string(),
        Some("default") => "default".to_string(),
        Some(other) => return Err(format!("settings 非法: {other}（可选 profile / default）")),
    };
    Ok((duration, settings))
}

/// jfr_record 有效总超时：默认 600/上限 1800，但必须容纳 duration + 120s 落盘余量。
pub fn effective_record_timeout(user: Option<i64>, duration_secs: u32) -> u64 {
    let base = match user {
        Some(t) if t > 0 => (t as u64).min(1800),
        _ => 600,
    };
    base.max(duration_secs as u64 + 120).min(1800)
}

/// JFR.start 命令构造（一次性定时录制；name/remote_path 由 handler 生成，纯函数可测）。
/// disk=false 时数据驻留 JVM 堆、录制结束直写 filename——彻底不落目标 JVM 的
/// java.io.tmpdir（issue #23：该目录在容器内常为 /opt/tmp ephemeral-storage，
/// 写满即驱逐；JFR repository 位置运行时不可改，只能用 disk=false 绕开）。
pub fn jfr_start_command(
    jcmd: &str,
    pid: u32,
    name: &str,
    duration_secs: u32,
    settings: &str,
    disk: bool,
    remote_path: &str,
) -> String {
    format!("{jcmd} {pid} JFR.start name={name} settings={settings} disk={disk} duration={duration_secs}s filename={remote_path}")
}

/// jfr_record 的 disk 参数解析：缺省按目标类型（容器 false 防驱逐 / 虚机 true
/// 无驱逐压力省堆内存）；显式传入必须是布尔值。
pub fn resolve_record_disk(user: Option<&serde_json::Value>, pod_target: bool) -> Result<bool, String> {
    match user {
        None => Ok(!pod_target),
        Some(v) => v.as_bool().ok_or_else(|| "disk 必须是布尔值（true/false）".to_string()),
    }
}

/// jfr_check_command 命令构造（issue #23：JFR.start 成功后校验录制确实在运行）
pub fn jfr_check_command(jcmd: &str, pid: u32, name: &str) -> String {
    format!("{jcmd} {pid} JFR.check name={name}")
}

/// jcmd VM.version 命令构造（issue #23 四轮：JFR.start 前的目标 JVM 版本预检）
pub fn vm_version_command(jcmd: &str, pid: u32) -> String {
    format!("{jcmd} {pid} VM.version")
}

/// VM.version 输出 → 主版本号（issue #23 四轮）。识别两类形态：
/// - 旧式 1.x：`1234:\n1.8.0_392` → 8
/// - 新式：`OpenJDK 64-Bit Server VM version 21.0.10+7-LTS` / `JDK 21.0.10` /
///   `11.0.21+9-LTS` → 21 / 11
/// jcmd 自带 `pid:` 头行跳过；行内扫描首个「数字+.」版本串（"64-Bit" 等裸数字
/// 不匹配）；无法解析 → None（调用方不得据此阻断）。
pub fn parse_vm_major_version(stdout: &str) -> Option<u32> {
    for line in stdout.lines() {
        let line = line.trim();
        // jcmd 头行（"12345:"）
        if line.ends_with(':') && line.trim_end_matches(':').chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        for (i, c) in line.char_indices() {
            if !c.is_ascii_digit() {
                continue;
            }
            let rest = &line[i..];
            // 版本串必须形如 N. 或 N_（排除行内裸数字，如 "64-Bit"）
            let after_digits = rest.trim_start_matches(|c: char| c.is_ascii_digit());
            if !after_digits.starts_with('.') && !after_digits.starts_with('_') {
                continue;
            }
            let mut parts = rest.split(|c: char| c == '.' || c == '_' || c == '-' || c == '+');
            let Ok(major) = parts.next().unwrap_or("").parse::<u32>() else { continue };
            if major == 1 {
                // 旧式 1.x：主版本 = 次号（1.8.0_392 → 8）
                let Ok(minor) = parts.next().unwrap_or("").parse::<u32>() else { continue };
                return Some(minor);
            }
            return Some(major);
        }
    }
    None
}

/// JFR.check 输出判定：JDK 11+ 输出形如 `Recording 1: name=... duration=30s (running)`。
/// 大小写不敏感匹配 "running"（容错不同 JDK 版本措辞）。
pub fn recording_check_passes(stdout: &str, stderr: &str) -> bool {
    format!("{stdout}\n{stderr}").to_lowercase().contains("running")
}

/// 代理工具：local_path → jfr_file_path + args 透传合并；路径与 async 为 handler
/// 权威值，合并后强制写入（透传对象不得覆盖）；async 固定 false（禁用上游后台
/// 任务模式，靠 Friday 超时分层，spec §3.2）。
/// 上游 top_n 原始 int 拆箱 NPE 已由构建期补丁根治（jmc-topn-int-fix.patch /
/// jmc-virtual-threads-fix.patch，issue #10），无需再注入默认值。
pub fn build_proxy(kind: JfrProxyKind, local_path: &str, extra: Option<&Value>) -> (String, Value) {
    let mut map = serde_json::Map::new();
    if let Some(Value::Object(extra)) = extra {
        for (k, v) in extra {
            map.insert(k.clone(), v.clone());
        }
    }
    // 最后强制覆盖：路径由 handler 解析（local_path 是唯一来源）；async 压回 false
    map.insert("jfr_file_path".to_string(), json!(local_path));
    map.insert("async".to_string(), json!(false));
    (kind.upstream_name().to_string(), Value::Object(map))
}

/// A/B 对比：双路径映射 + args 透传合并；路径与 async 合并后强制写入
pub fn build_compare(baseline: &str, target: &str, extra: Option<&Value>) -> (String, Value) {
    let mut map = serde_json::Map::new();
    if let Some(Value::Object(extra)) = extra {
        for (k, v) in extra {
            map.insert(k.clone(), v.clone());
        }
    }
    map.insert("baseline_jfr_path".to_string(), json!(baseline));
    map.insert("target_jfr_path".to_string(), json!(target));
    map.insert("async".to_string(), json!(false));
    ("compareRecordings".to_string(), Value::Object(map))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_record_params_defaults() {
        let (d, s) = validate_record_params(&json!({})).unwrap();
        assert_eq!(d, 60);
        assert_eq!(s, "profile");
    }

    #[test]
    fn test_validate_record_params_bounds() {
        assert!(validate_record_params(&json!({"duration_secs": 10})).is_ok());
        assert!(validate_record_params(&json!({"duration_secs": 600})).is_ok());
        assert!(validate_record_params(&json!({"duration_secs": 9})).is_err());
        assert!(validate_record_params(&json!({"duration_secs": 601})).is_err());
        assert!(validate_record_params(&json!({"settings": "default"})).is_ok());
        assert!(validate_record_params(&json!({"settings": "boot"})).is_err());
    }

    #[test]
    fn test_jfr_start_command_shape() {
        let cmd = jfr_start_command(
            "/tmp/jdk/bin/jcmd",
            1234,
            "friday-777",
            60,
            "profile",
            false,
            "/tmp/friday-tools/recording-1234-777.jfr",
        );
        assert_eq!(
            cmd,
            "/tmp/jdk/bin/jcmd 1234 JFR.start name=friday-777 settings=profile disk=false duration=60s filename=/tmp/friday-tools/recording-1234-777.jfr"
        );
        // VM 默认 disk=true 形态
        let cmd = jfr_start_command(
            "/tmp/jdk/bin/jcmd",
            1234,
            "friday-777",
            60,
            "profile",
            true,
            "/tmp/friday-tools/recording-1234-777.jfr",
        );
        assert!(cmd.contains("disk=true"));
    }

    #[test]
    fn test_resolve_record_disk_defaults_by_target() {
        // 缺省：容器 false（防驱逐）/ 虚机 true（无压力）
        assert_eq!(resolve_record_disk(None, true), Ok(false));
        assert_eq!(resolve_record_disk(None, false), Ok(true));
        // 显式覆盖
        assert_eq!(resolve_record_disk(Some(&json!(true)), true), Ok(true));
        assert_eq!(resolve_record_disk(Some(&json!(false)), false), Ok(false));
        // 非布尔拒绝
        assert!(resolve_record_disk(Some(&json!("yes")), true).is_err());
        assert!(resolve_record_disk(Some(&json!(1)), true).is_err());
    }

    #[test]
    fn test_jfr_check_command_shape() {
        let cmd = jfr_check_command("/tmp/jdk/bin/jcmd", 1234, "friday-777");
        assert_eq!(cmd, "/tmp/jdk/bin/jcmd 1234 JFR.check name=friday-777");
    }

    #[test]
    fn test_vm_version_command_shape() {
        assert_eq!(vm_version_command("/tmp/jdk/bin/jcmd", 1234), "/tmp/jdk/bin/jcmd 1234 VM.version");
    }

    /// issue #23 四轮：VM.version 输出形态实测（JDK 21 本地验证）+ JDK 8 旧式
    #[test]
    fn test_parse_vm_major_version_formats() {
        // JDK 11+ 实测形态（jcmd 自带 pid 头行）
        assert_eq!(
            parse_vm_major_version("102668:\nOpenJDK 64-Bit Server VM version 21.0.10+7-LTS\nJDK 21.0.10\n"),
            Some(21)
        );
        assert_eq!(parse_vm_major_version("1234:\n11.0.21+9-LTS\n"), Some(11));
        assert_eq!(parse_vm_major_version("1234:\n17.0.10\n"), Some(17));
        // JDK 8 旧式（1.x → 主版本取次号）
        assert_eq!(parse_vm_major_version("1234:\n1.8.0_392\n"), Some(8));
        // Oracle JDK 8：可能带 vendor 行，版本行在后
        assert_eq!(
            parse_vm_major_version("1234:\nJava HotSpot(TM) 64-Bit Server VM\n1.8.0_292\n"),
            Some(8)
        );
    }

    #[test]
    fn test_parse_vm_major_version_unparseable_is_none() {
        assert_eq!(parse_vm_major_version(""), None);
        assert_eq!(parse_vm_major_version("1234:\n"), None);
        assert_eq!(parse_vm_major_version("garbage output"), None);
        // 裸数字不误配（无 . 或 _ 后缀）
        assert_eq!(parse_vm_major_version("JDK 21"), None);
    }

    #[test]
    fn test_recording_check_passes_running_states() {
        // JDK 11+ 典型输出
        assert!(recording_check_passes(
            "Recording 1: name=friday-777 duration=300s (running)\n",
            ""
        ));
        // 大小写容错 + stderr 输出
        assert!(recording_check_passes("", "Recording 1: name=x (RUNNING)\n"));
    }

    #[test]
    fn test_recording_check_rejects_non_running() {
        // 找不到录制（JFR.start 静默失败场景，issue #23 问题 2）
        assert!(!recording_check_passes("Could not find recording with name friday-777\n", ""));
        // 空输出
        assert!(!recording_check_passes("", ""));
    }

    #[test]
    fn test_effective_record_timeout_matrix() {
        assert_eq!(effective_record_timeout(None, 60), 600);
        assert_eq!(effective_record_timeout(None, 300), 600);
        assert_eq!(effective_record_timeout(None, 600), 720);
        assert_eq!(effective_record_timeout(Some(1000), 60), 1000);
        assert_eq!(effective_record_timeout(Some(9999), 60), 1800);
        assert_eq!(effective_record_timeout(Some(30), 600), 720);
        assert_eq!(effective_record_timeout(Some(0), 60), 600);
        assert_eq!(effective_record_timeout(Some(-5), 60), 600);
    }

    #[test]
    fn test_build_proxy_maps_path_and_forces_sync() {
        let (name, args) = build_proxy(
            JfrProxyKind::HotMethods,
            r"C:\artifacts\a.jfr",
            Some(&json!({"top_n": 5, "async": true})),
        );
        assert_eq!(name, "hotMethods");
        assert_eq!(args["jfr_file_path"], r"C:\artifacts\a.jfr");
        assert_eq!(args["top_n"], 5);
        assert_eq!(args["async"], false, "async must be forced false even if caller passes true");

        // 透传对象不得覆盖 handler 权威路径键
        let (_, args) = build_proxy(
            JfrProxyKind::HotMethods,
            r"C:\artifacts\a.jfr",
            Some(&json!({"jfr_file_path": "/stale/hallucinated.jfr"})),
        );
        assert_eq!(args["jfr_file_path"], r"C:\artifacts\a.jfr", "path key must not be overridable");
    }

    #[test]
    fn test_build_proxy_without_extra_args() {
        let (name, args) = build_proxy(JfrProxyKind::QuickAnalysis, "/tmp/a.jfr", None);
        assert_eq!(name, "smartQuickAnalysis");
        assert_eq!(args["jfr_file_path"], "/tmp/a.jfr");
        assert_eq!(args["async"], false);
        assert_eq!(args.as_object().unwrap().len(), 2);
    }

    /// issue #10 根治回归：上游 top_n 拆箱 NPE 已由构建期补丁修复
    /// （jmc-topn-int-fix.patch / jmc-virtual-threads-fix.patch），
    /// build_proxy 不再注入 top_n 默认值——缺省透传（上游 Integer + 判空兜底）。
    #[test]
    fn test_build_proxy_no_top_n_injection_after_upstream_fix() {
        let (_, args) = build_proxy(JfrProxyKind::ThreadCpu, "/tmp/a.jfr", None);
        assert!(
            args.get("top_n").is_none(),
            "top_n must not be injected anymore (upstream patch handles the default)"
        );
        let (_, args) = build_proxy(JfrProxyKind::VirtualThreads, "/tmp/a.jfr", Some(&json!({"top_n": null})));
        assert!(
            args.get("top_n").map(|v| v.is_null()).unwrap_or(false),
            "explicit null must be passed through untouched (upstream handles it)"
        );
        let (_, args) = build_proxy(JfrProxyKind::ThreadContention, "/tmp/a.jfr", Some(&json!({"top_n": 5})));
        assert_eq!(args["top_n"], 5);
    }

    #[test]
    fn test_build_compare_two_paths() {
        let (name, args) =
            build_compare("/tmp/base.jfr", "/tmp/target.jfr", Some(&json!({"async": true})));
        assert_eq!(name, "compareRecordings");
        assert_eq!(args["baseline_jfr_path"], "/tmp/base.jfr");
        assert_eq!(args["target_jfr_path"], "/tmp/target.jfr");
        assert_eq!(args["async"], false);

        // 透传对象不得覆盖 handler 权威路径键
        let (_, args) = build_compare(
            "/tmp/base.jfr",
            "/tmp/target.jfr",
            Some(&json!({"baseline_jfr_path": "/stale.jfr", "target_jfr_path": "/stale.jfr"})),
        );
        assert_eq!(args["baseline_jfr_path"], "/tmp/base.jfr");
        assert_eq!(args["target_jfr_path"], "/tmp/target.jfr");
    }

    #[test]
    fn test_upstream_name_table_complete() {
        let kinds = [
            JfrProxyKind::Overview,
            JfrProxyKind::Rules,
            JfrProxyKind::QuickAnalysis,
            JfrProxyKind::GcDetail,
            JfrProxyKind::MemoryLeaks,
            JfrProxyKind::PredictiveLeak,
            JfrProxyKind::AllocationHotspots,
            JfrProxyKind::HotMethods,
            JfrProxyKind::ThreadCpu,
            JfrProxyKind::CpuFlame,
            JfrProxyKind::ThreadContention,
            JfrProxyKind::DeadlockDetection,
            JfrProxyKind::IoHotspots,
            JfrProxyKind::Exceptions,
            JfrProxyKind::Errors,
            JfrProxyKind::Safepoints,
            JfrProxyKind::VirtualThreads,
            JfrProxyKind::StackTraceSearch,
            JfrProxyKind::Correlate,
            JfrProxyKind::RequestWaterfall,
        ];
        assert_eq!(kinds.len(), 20);
        let names: Vec<&str> = kinds.iter().map(|k| k.upstream_name()).collect();
        assert!(names.iter().all(|n| !n.is_empty()));
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len(), "upstream names must be unique");
    }
}
