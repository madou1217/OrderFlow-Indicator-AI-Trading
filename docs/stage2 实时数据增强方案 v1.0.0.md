# Stage 2 实时数据增强方案 v1.0.0

## 0. 问题定义

Stage 2（entry / pending / management）的交易决策**落后于当前行情**。

根因是：Stage 2 使用的 indicator bundle 虽然比 Stage 1 新，
但 `cvd_pack.by_window` 和 `footprint.by_window` 里的**多时间框架 series 只包含已闭合 bar**，
当前未闭合的 15m / 4h / 1d 窗口内的分钟级演变对 Stage 2 不可见。

### 实测时间线（来自日志 2026-03-26）

```
03:30:00  Stage 1 开始，使用 bundle ts_bucket=03:30
03:41:16  Stage 1 完成（scan_latency=533s ≈ 8.9 min）
03:41:21  Stage 2 开始，load_latest_temp_indicator_bundle → ts_bucket=03:39
          stage1_to_stage2_gap = 9 分钟
03:45:42  Stage 2 finalize 完成（finalize_latency=256s ≈ 4.3 min）
          总延迟：03:30 → 03:45 = 15 分钟
```

**Stage 2 在 03:41 做决策时：**

| 数据类型 | Stage 2 看到的 | 实际已经发生的 |
|---------|--------------|-------------|
| 15m bar series | 最新闭合 bar = 03:30→03:45 的上一个（03:30 bar） | 03:30→03:39 已有 9 分钟新数据 |
| 4h bar series | 最新闭合 bar = 00:00→04:00 的上一个 | 当前 4h bar 已走了 3h41m |
| 1d bar series | 最新闭合 bar = 前一天 | 当前日 bar 已走了 3h41m |
| cvd_pack 顶层 | 03:39 这 1 分钟的 delta | ✓ 是最新的 |
| orderbook OBI/OFI | 03:39 分钟末快照 | ✓ 接近实时 |

**核心缺失：Stage 2 无法看到 03:30→03:39 这 9 分钟内 delta/CVD 的演变趋势。**

如果这 9 分钟内发生了 delta 突然翻转、CVD 斜率剧变、或者 whale 大单涌入，
Stage 2 完全不知道。它只能看到 03:39 这一分钟的快照和 03:30 之前的闭合 bar。

---

## 1. 最高原则

本方案完全服从：

- **不降模型输出质量是第一需求**
- **只增加信息，不删除任何现有信息**
- **不改变现有 indicator 的闭合 bar series 语义**

做法是：**在现有数据旁边新增一层 `partial_window` 数据**，而不是修改现有字段。

---

## 2. 方案总览

在 indicator_engine 的 `cvd_pack` indicator 内，新增一个 `partial_window` 层，
包含当前未闭合的 15m / 4h / 1d 窗口内的**分钟级累计序列**。

同时在 LLM 侧的 Stage 2 filter 中，新增一层 `realtime_flow_context`，
从 bundle 中提取并压缩 partial window 数据，直接注入 Stage 2 的模型输入。

### 数据流

```
indicator_engine
  └─ cvd_pack.evaluate()
       └─ 新增: partial_window.{15m,4h,1d}
            └─ 当前窗口内每分钟的 cum_delta_fut, cum_delta_spot, cum_volume

LLM service (Stage 2 filter)
  └─ 新增: build_realtime_flow_context()
       └─ 从 cvd_pack.partial_window + footprint.by_window + orderbook_depth
       └─ 压缩成 realtime_flow_context 注入模型输入
```

---

## 3. indicator_engine 侧改动

### 3.1 `cvd_pack` 新增 `partial_window`

在 `I14CvdPack::evaluate()` 中，除了现有的 `by_window`（闭合 bar series），
新增 `partial_window` 层：

```rust
// i14_cvd_pack.rs evaluate() 中新增
let mut partial_window = serde_json::Map::new();
for (label, mins) in WINDOWS {
    // 只对 15m, 4h, 1d 生成 partial window
    if !matches!(label, "15m" | "4h" | "1d") {
        continue;
    }
    let pw = build_partial_window(
        &ctx.history_futures,
        &ctx.history_spot,
        ctx.ts_bucket,
        mins,
    );
    if !pw.is_null() {
        partial_window.insert(label.to_string(), pw);
    }
}
```

### 3.2 `build_partial_window` 函数定义

```rust
fn build_partial_window(
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
    ts_bucket: DateTime<Utc>,
    interval_mins: i64,
) -> Value {
    // 计算当前窗口的起始时间
    let window_end = align_bar_end(ts_bucket + Duration::minutes(1), interval_mins);
    let window_start = window_end - Duration::minutes(interval_mins);

    // 收集当前窗口内的所有 1m bar
    let fut_minutes: Vec<_> = history_futures.iter()
        .filter(|h| h.ts_bucket >= window_start && h.ts_bucket <= ts_bucket)
        .collect();
    let spot_minutes: Vec<_> = history_spot.iter()
        .filter(|h| h.ts_bucket >= window_start && h.ts_bucket <= ts_bucket)
        .collect();

    if fut_minutes.is_empty() {
        return Value::Null;
    }

    let minutes_elapsed = fut_minutes.len();
    let minutes_total = interval_mins as usize;

    // 累计 delta 序列
    let mut cum_delta_fut = 0.0;
    let mut cum_delta_spot = 0.0;
    let mut cum_volume_fut = 0.0;
    let mut series = Vec::new();

    // 用于计算斜率变化
    let mut delta_fut_first_half = 0.0;
    let mut delta_fut_second_half = 0.0;
    let half_point = minutes_elapsed / 2;

    for (i, h) in fut_minutes.iter().enumerate() {
        cum_delta_fut += h.delta;
        cum_volume_fut += h.volume;
        // 找对应 spot
        let spot_delta = spot_minutes.iter()
            .find(|s| s.ts_bucket == h.ts_bucket)
            .map(|s| s.delta)
            .unwrap_or(0.0);
        cum_delta_spot += spot_delta;

        if i < half_point {
            delta_fut_first_half += h.delta;
        } else {
            delta_fut_second_half += h.delta;
        }

        series.push(json!({
            "minute": i + 1,
            "cum_delta_fut": round2(cum_delta_fut),
            "cum_delta_spot": round2(cum_delta_spot),
        }));
    }

    // 斜率变化检测
    let slope_regime = if half_point < 2 {
        "insufficient_data"
    } else if delta_fut_first_half > 0.0 && delta_fut_second_half < 0.0 {
        "reversal_to_selling"
    } else if delta_fut_first_half < 0.0 && delta_fut_second_half > 0.0 {
        "reversal_to_buying"
    } else if delta_fut_second_half.abs() > delta_fut_first_half.abs() * 1.5 {
        "accelerating"
    } else if delta_fut_second_half.abs() < delta_fut_first_half.abs() * 0.5 {
        "decelerating"
    } else {
        "steady"
    };

    json!({
        "window_start": window_start.to_rfc3339(),
        "minutes_elapsed": minutes_elapsed,
        "minutes_total": minutes_total,
        "progress_pct": round2(minutes_elapsed as f64 / minutes_total as f64 * 100.0),
        "cum_delta_fut": round2(cum_delta_fut),
        "cum_delta_spot": round2(cum_delta_spot),
        "cum_volume_fut": round2(cum_volume_fut),
        "delta_fut_first_half": round2(delta_fut_first_half),
        "delta_fut_second_half": round2(delta_fut_second_half),
        "slope_regime": slope_regime,
        "series": series,
    })
}
```

### 3.3 输出示例

在 bundle ts_bucket=03:39 时，`cvd_pack.partial_window.15m` 的内容：

```json
{
  "window_start": "2026-03-26T03:30:00Z",
  "minutes_elapsed": 9,
  "minutes_total": 15,
  "progress_pct": 60.0,
  "cum_delta_fut": 3420.5,
  "cum_delta_spot": -180.3,
  "cum_volume_fut": 28500.0,
  "delta_fut_first_half": 4200.1,
  "delta_fut_second_half": -779.6,
  "slope_regime": "reversal_to_selling",
  "series": [
    {"minute": 1, "cum_delta_fut": 500.2, "cum_delta_spot": 20.1},
    {"minute": 2, "cum_delta_fut": 1200.5, "cum_delta_spot": 45.3},
    {"minute": 3, "cum_delta_fut": 2100.8, "cum_delta_spot": 60.2},
    {"minute": 4, "cum_delta_fut": 3000.3, "cum_delta_spot": 10.5},
    {"minute": 5, "cum_delta_fut": 4200.1, "cum_delta_spot": -30.2},
    {"minute": 6, "cum_delta_fut": 4050.3, "cum_delta_spot": -80.1},
    {"minute": 7, "cum_delta_fut": 3800.1, "cum_delta_spot": -120.5},
    {"minute": 8, "cum_delta_fut": 3600.8, "cum_delta_spot": -160.8},
    {"minute": 9, "cum_delta_fut": 3420.5, "cum_delta_spot": -180.3}
  ]
}
```

这个例子展示了一个典型的"delta 翻转"：前 5 分钟 futures delta 持续买入（+4200），
后 4 分钟开始回落（-780），`slope_regime = "reversal_to_selling"`。

**这是当前 Stage 2 完全看不到的信息。**

---

## 4. LLM 侧改动：Stage 2 filter 新增 `realtime_flow_context`

### 4.1 适用范围

`realtime_flow_context` 只在 **Stage 2**（entry / pending / management）中注入。
Stage 1 Scan 不需要，因为 Scan 本身就在 bar 闭合时运行。

### 4.2 数据结构

```json
"realtime_flow_context": {
  "bundle_ts": "2026-03-26T03:39:00Z",
  "stage1_scan_ts": "2026-03-26T03:30:00Z",
  "data_lag_minutes": 9,

  "partial_windows": {
    "15m": {
      "window_start": "2026-03-26T03:30:00Z",
      "progress_pct": 60.0,
      "cum_delta_fut": 3420.5,
      "cum_delta_spot": -180.3,
      "delta_fut_first_half": 4200.1,
      "delta_fut_second_half": -779.6,
      "slope_regime": "reversal_to_selling",
      "vs_last_closed_bar": {
        "last_closed_delta_fut": 2047.8,
        "current_partial_delta_fut": 3420.5,
        "direction_consistent": true,
        "magnitude_ratio": 1.67
      }
    },
    "4h": {
      "window_start": "2026-03-26T00:00:00Z",
      "progress_pct": 65.0,
      "cum_delta_fut": -1184.2,
      "cum_delta_spot": -520.3,
      "delta_fut_first_half": 2800.5,
      "delta_fut_second_half": -3984.7,
      "slope_regime": "reversal_to_selling",
      "vs_last_closed_bar": {
        "last_closed_delta_fut": 8908.1,
        "current_partial_delta_fut": -1184.2,
        "direction_consistent": false,
        "magnitude_ratio": -0.13
      }
    },
    "1d": {
      "window_start": "2026-03-26T00:00:00Z",
      "progress_pct": 16.3,
      "cum_delta_fut": 24405.2,
      "cum_delta_spot": 1200.5,
      "slope_regime": "steady",
      "vs_last_closed_bar": {
        "last_closed_delta_fut": 12585.6,
        "current_partial_delta_fut": 24405.2,
        "direction_consistent": true,
        "magnitude_ratio": 1.94
      }
    }
  },

  "orderbook_now": {
    "obi_k_dw_close_fut": -0.863,
    "obi_k_dw_slope_fut": -0.00000099,
    "ofi_norm_fut": -1.29,
    "ofi_norm_spot": 0.90,
    "exec_confirm_fut": false,
    "spot_confirm": true,
    "spread_twa_fut": 0.004
  },

  "cvd_slope_now": {
    "cvd_slope_fut_30bar": 66.09,
    "cvd_slope_spot_30bar": -36.60,
    "fut_spot_slope_divergent": true
  }
}
```

### 4.3 各字段的来源

| 字段 | 来源 | 说明 |
|------|------|------|
| `partial_windows.{tf}` | `cvd_pack.partial_window.{tf}` | 新增的 partial window 数据 |
| `vs_last_closed_bar` | `cvd_pack.by_window.{tf}.series[-1]` | 用最近闭合 bar 做对比 |
| `orderbook_now` | `orderbook_depth` 顶层字段 | 当前分钟的 orderbook 快照 |
| `cvd_slope_now` | `cvd_pack` 顶层 `cvd_slope_fut/spot` | 30-bar 滚动斜率 |

### 4.4 `vs_last_closed_bar` 的价值

这个子结构让模型能一眼看到：

- **当前未闭合窗口的 delta 方向是否与上一个闭合 bar 一致**
- **magnitude_ratio**：当前 partial delta 已经是上一 bar 的几倍
  - `> 1.5`：当前窗口比上一 bar 更强
  - `0 ~ 1.0`：正常延续
  - `< 0`：方向翻转

这些在当前系统中完全不可见。

### 4.5 `slope_regime` 的价值

告诉模型当前窗口内部 delta 的演变趋势：

| slope_regime | 含义 | 交易意义 |
|-------------|------|---------|
| `reversal_to_selling` | 前半窗口买入，后半转卖出 | 入场做多可能被 trap |
| `reversal_to_buying` | 前半窗口卖出，后半转买入 | 底部可能在形成 |
| `accelerating` | 后半 > 前半 1.5x | 动能加速，趋势增强 |
| `decelerating` | 后半 < 前半 0.5x | 动能衰减，可能即将反转 |
| `steady` | 均匀分布 | 无明显变化 |
| `insufficient_data` | 窗口内不足 4 分钟 | 数据不够判断 |

---

## 5. `partial_window.series` 的裁剪策略

### 5.1 15m 窗口

15m 最多 15 个 entry。每个 entry 3 个数字（minute, cum_delta_fut, cum_delta_spot）。
**全量保留**，最多 ~0.6 KB。

### 5.2 4h 窗口

4h = 240 分钟，全量保留 series 会有 240 个 entry（~7 KB），太大。

策略：**只保留最近 15 分钟的分钟级 series + 汇总统计**。

```json
"4h": {
  "window_start": "2026-03-26T00:00:00Z",
  "progress_pct": 65.0,
  "cum_delta_fut": -1184.2,
  "cum_delta_spot": -520.3,
  "delta_fut_first_half": 2800.5,
  "delta_fut_second_half": -3984.7,
  "slope_regime": "reversal_to_selling",
  "recent_15m_series": [
    {"minute": 1, "cum_delta_fut": -1050.3, "cum_delta_spot": -500.1},
    ...
    {"minute": 9, "cum_delta_fut": -1184.2, "cum_delta_spot": -520.3}
  ]
}
```

这里 `recent_15m_series` 只保留最近 15 个 1m bar 的累积值，
相当于"4h bar 内最后 15 分钟的微观演变"。

### 5.3 1d 窗口

同 4h，只保留最近 15 分钟的 `recent_15m_series` + 汇总统计。

### 5.4 总 size 预算

| 组件 | 大小 |
|------|------|
| 15m partial_window（含 series） | ~0.6 KB |
| 4h partial_window（含 recent_15m_series） | ~0.8 KB |
| 1d partial_window（含 recent_15m_series） | ~0.8 KB |
| orderbook_now | ~0.2 KB |
| cvd_slope_now | ~0.1 KB |
| **总计 realtime_flow_context** | **~2.5 KB** |

对比当前 Stage 2 entry 输入总大小（~107 KB），这只增加 ~2.3%。

---

## 6. Prompt 改动

### 6.1 Stage 2 entry / pending / management prompt 新增

在所有 Stage 2 prompt 中新增一段：

```text
REALTIME FLOW CONTEXT

`realtime_flow_context` contains data from AFTER the Stage 1 scan was produced.
It shows what happened in the minutes between the scan and now.

Key fields:
- `data_lag_minutes`: how many minutes newer this data is versus the scan
- `partial_windows.{tf}.slope_regime`: whether delta is accelerating, decelerating, or reversing within the current unfinished bar
- `partial_windows.{tf}.vs_last_closed_bar`: how the current partial bar compares to the last closed bar
- `orderbook_now`: current orderbook state (OBI, OFI, spread)
- `cvd_slope_now`: current 30-bar CVD slope for futures and spot

Use this to:
1. Confirm or invalidate the scan's thesis with newer evidence
2. Detect delta reversals or slope changes that occurred after the scan
3. Improve entry timing: if slope_regime shows reversal, consider waiting or adjusting entry
4. Improve SL placement: if partial delta shows stronger flow than scan expected, tighten SL accordingly

Do not ignore this data. It is MORE RECENT than the scan and may override scan conclusions about short-term flow direction.
```

### 6.2 自检新增 1 条

对于所有 Stage 2 自检，新增：

```text
Have I checked `realtime_flow_context.partial_windows` for delta reversals or slope changes
that occurred after the Stage 1 scan?
```

---

## 7. 三个 Stage 2 模式的差异化处理

### 7.1 Entry

**完整注入 `realtime_flow_context`**，包含所有 3 个 timeframe 的 partial_window。

Entry 需要精确的入场时机，partial window delta 对于判断"现在入场是否太晚"至关重要。

### 7.2 Pending

**完整注入 `realtime_flow_context`**。

Pending 需要判断"挂单价位是否还有意义"，如果 partial window 显示 delta 已经翻转，
挂单可能需要撤销或调价。

### 7.3 Management

**注入 `realtime_flow_context`，但不包含 15m 的 series 明细**。

Management 更关注 4h/1d 级别的趋势是否延续，不需要 15m 内的逐分钟序列。
只需要 `slope_regime` 和 `vs_last_closed_bar`。

```json
// management 模式下的 15m partial_window
"15m": {
  "progress_pct": 60.0,
  "cum_delta_fut": 3420.5,
  "cum_delta_spot": -180.3,
  "slope_regime": "reversal_to_selling",
  "vs_last_closed_bar": { ... }
  // 不含 series
}
```

---

## 8. 实现路径

### Phase 1: indicator_engine（无风险，纯新增）

1. 在 `i14_cvd_pack.rs` 新增 `build_partial_window` 函数
2. 在 `evaluate()` 中调用，输出到 `partial_window` 字段
3. **不改变现有任何字段的语义**
4. `partial_window` 作为新增字段，即使下游不读也无害

### Phase 2: LLM filter（无风险，纯新增）

1. 在 `core_entry.rs` / `core_pending.rs` / `core_management.rs` 中新增 `build_realtime_flow_context()`
2. 从 `cvd_pack.partial_window` + `orderbook_depth` + `cvd_pack` 顶层提取数据
3. 注入到 Stage 2 的模型输入中，作为新的顶层字段
4. **不改变现有任何 indicator 的 filter 逻辑**

### Phase 3: Prompt 更新

1. 在 entry / pending / management prompt 中新增 `REALTIME FLOW CONTEXT` 段
2. 新增 1 条自检
3. **不删除现有任何 prompt 内容**

---

## 9. 质量验证

### 9.1 上线前对照

至少对照 10 个 Stage 2 cycle：

1. 有 `realtime_flow_context` vs 无 `realtime_flow_context` 的 entry 决策质量
2. 特别关注：slope_regime = "reversal_*" 时，模型是否做出了不同的决策
3. SL 精度：有 partial window 数据后，SL 是否更贴近实际波动

### 9.2 rollout 指标

建议记录：

1. `realtime_flow_context_present`
2. `partial_window_15m_slope_regime`
3. `partial_window_15m_vs_closed_direction_consistent`
4. `data_lag_minutes`

### 9.3 回退方案

如果发现质量问题：
- 直接从 Stage 2 filter 中移除 `realtime_flow_context` 注入
- indicator_engine 的 `partial_window` 字段可以保留（不会被读取）
- **无需回滚任何现有逻辑**

---

## 10. 预计收益

### 10.1 解决的核心问题

Stage 2 不再是"基于 9 分钟前的快照做决策"：

| 场景 | 当前行为 | 改后行为 |
|------|---------|---------|
| Delta 在 scan 后翻转 | Stage 2 不知道，按旧方向入场 | `slope_regime=reversal_*`，模型可以等待或反向 |
| CVD 加速 | Stage 2 只看到闭合 bar | `accelerating` + `magnitude_ratio > 1.5`，模型可以加仓 |
| Whale 大单在 scan 后出现 | Stage 2 完全不知道 | `cum_delta_fut` 突然跳变可见 |
| 4h bar 内趋势反转 | Stage 2 只看到上一个 4h 闭合 bar | `4h.slope_regime=reversal_*` 可见 |

### 10.2 不引入的风险

- 不删除任何现有数据
- 不改变任何现有字段语义
- 不修改 indicator_engine 的闭合 bar 逻辑
- 输入增量 ~2.5 KB（占比 2.3%）
- 如果模型不理解新数据，最差情况是忽略它，不会比现在更差

---

## 11. 最终结论

Stage 2 的交易决策落后于行情，
根因是多时间框架 series 只包含闭合 bar，当前窗口内的分钟级演变不可见。

本方案通过：

1. **indicator_engine 新增 `partial_window`**：让每个 bundle 携带当前未闭合窗口的逐分钟 delta 序列
2. **LLM filter 新增 `realtime_flow_context`**：压缩后注入 Stage 2 输入
3. **Prompt 新增引导**：告诉模型如何使用这层更新数据

做到**不改变任何现有逻辑、不删除任何现有数据、不引入质量风险**的前提下，
让 Stage 2 能看到 Stage 1 scan 之后发生的市场变化。
