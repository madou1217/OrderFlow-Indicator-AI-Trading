# Stage 2 实时数据增强方案 v1.1.0

## 0. v1.1 修订摘要

v1.1 相对 v1.0 的核心修正有 3 点：

1. **修正问题定义**
   `Stage 2` 不是“完全看不到实时数据”。
   当前实现里：
   - `Stage 2` 会加载最新的 `temp_indicator bundle`
   - `orderbook_depth` 顶层和 `by_window["15m"/"1h"]` 已经是接近实时的滚动聚合
   - `footprint.by_window` 也已经把当前分钟并入滚动窗口

   真正的主盲区是：
   **`cvd_pack.by_window` 只包含已闭合窗口的 series，导致 Stage 2 看不到当前未闭合 `15m / 4h / 1d` bar 内部的 Delta / CVD 演变。**

2. **补上 Stage 2 自身延迟造成的再次过期**
   v1.0 只解决“`Stage 1 -> Stage 2` 中间 5~15 分钟的盲区”，
   但没有解决 “`Stage 2` 发 prompt 后，到模型返回和执行时，数据又过期了” 的问题。
   v1.1 增加 `pre_execution_freshness_recheck`。

3. **减少重复搬运已有 live 信息**
   v1.0 倾向于把 `footprint` / `orderbook_depth` 再包装一遍。
   v1.1 改成：
   - 重点新增 `cvd_pack.partial_window`
   - 在 `realtime_flow_context` 里只引用少量必要的 live refs
   - 不重复复制大块已存在的 `footprint` / `orderbook_depth` 数据

---

## 1. 现状校准

### 1.1 当前系统已经做到的事

- `Stage 2` 会重新读取同 symbol 的最新 `temp_indicator bundle`
- `orderbook_depth` 顶层字段已经提供当前分钟末的 OBI / OFI / microprice / spread 等快照
- `orderbook_depth.by_window["15m"]` 已经提供滚动窗口聚合
- `footprint.by_window["15m"/"4h"/"1d"]` 不是纯闭合 bar，而是包含当前分钟的滚动聚合

### 1.2 当前系统真正缺失的事

`cvd_pack` 目前只有两层信息：

- 顶层：当前 1m 的 `delta_fut / delta_spot / cvd_slope_*`
- `by_window`：只到最近一个**已闭合**的 `15m / 4h / 1d` bar

这会导致：

- `Stage 2` 知道“最新 1 分钟发生了什么”
- `Stage 2` 也知道“上一个闭合 15m / 4h / 1d bar 长什么样”
- **但它不知道当前未闭合 bar 在最近几分钟里是怎样逐步翻转、加速、衰减的**

这正是 `Delta / Volume Delta / CVD 斜率突变` 最容易发生、同时对 `entry / pending / management` 最有交易价值的区间。

### 1.3 第二个问题：末端再次过期

真实时间线通常是：

```text
Stage 1 scan bundle ts      -> T0
Stage 2 core bundle ts      -> T1 = T0 + 5~15m
模型 finalize 返回 / 执行时点 -> T2 = T1 + 1~5m
```

即使 `T1` 时刻的数据已经比 `Stage 1` 新，
`T2` 时真正要执行交易动作时，这份数据仍可能再次过期。

当前系统已有“过 stale 则直接跳过执行”的保护，
但没有“用更新数据再确认一次”的保护。

---

## 2. 目标与非目标

### 2.1 v1.1 目标

v1.1 要实现 3 个目标：

1. **让 Stage 2 看到未闭合 `15m / 4h / 1d` bar 内部的最新 Delta / CVD 演变**
2. **让模型明确知道哪些数据是 `Stage 1` 之后新增的**
3. **在真正执行前，再做一次轻量 freshness recheck，降低末端过期执行**

### 2.2 v1.1 非目标

v1.1 不做这些事：

- 不改变任何现有 `by_window.series` 的闭合 bar 语义
- 不重写 `footprint` 的聚合方式
- 不复制完整 `orderbook_depth` / `footprint` 到一个新字段里
- 不引入秒级 tick replay
- 不在 v1.1 里再开一个完整的第二轮大 prompt

---

## 3. v1.1 总体方案

v1.1 分成 3 层：

### Layer A: indicator_engine 新增 `cvd_pack.partial_window`

只补 `cvd_pack` 的真实盲区：

- `partial_window.15m`
- `partial_window.4h`
- `partial_window.1d`

它们描述当前未闭合窗口内，分钟级 Delta / CVD 如何演变。

### Layer B: Stage 2 prompt input 新增 `realtime_flow_context`

在 `Stage 2` 的 prompt input 顶层新增：

- `realtime_flow_context.cvd_partial_windows`
- `realtime_flow_context.since_stage1_increment`
- `realtime_flow_context.live_refs`
- `realtime_flow_context.freshness`

其中：

- `cvd_partial_windows` 负责回答“当前未闭合 bar 正在怎样变化”
- `since_stage1_increment` 负责回答“从 scan 到现在新增了什么”
- `live_refs` 只放少量最关键的实时引用值，不复制完整大对象
- `freshness` 告诉模型这份数据相对 `Stage 1` 新了多久

其中 `since_stage1_increment` 除了 flow 增量外，还要显式表达：

- **scan 后新出现的 event indicators**

否则会出现一个盲区：

- `Stage 1 scan` 当时没有 absorption / initiation / exhaustion
- 但 `Stage 2 bundle` 已经包含了新确认的 event
- 模型虽然能看到这个 event 存在，却不知道它是 **scan 之后才出现的新增信息**

### Layer C: runtime 新增 `pre_execution_freshness_recheck`

在模型已经给出可执行决策，但真正落交易动作之前：

- 再检查是否已经出现更新的 `temp_indicator bundle`
- 如果更新幅度足够大，则做一次轻量 recheck
- recheck 不重跑完整大 prompt，只做 veto / confirm

---

## 4. indicator_engine 侧设计

### 4.1 只改 `cvd_pack`

v1.1 不修改：

- `footprint`
- `orderbook_depth`
- `kline_history`

原因是这些数据源当前已经提供了部分 live 或 rolling 视角。
最缺的是 `cvd_pack` 对未闭合 HTF bar 的表达。

### 4.2 `cvd_pack.partial_window` 结构

新增字段：

```json
"partial_window": {
  "15m": { ... },
  "4h": { ... },
  "1d": { ... }
}
```

每个 timeframe 的公共结构：

```json
{
  "window_start": "2026-03-26T03:30:00Z",
  "window_end": "2026-03-26T03:45:00Z",
  "last_minute_ts": "2026-03-26T03:39:00Z",
  "minutes_elapsed": 10,
  "minutes_total": 15,
  "progress_pct": 66.67,

  "cum_delta_fut": 3420.5,
  "cum_delta_spot": -180.3,
  "cum_volume_fut": 28500.0,

  "recent_3m_delta_fut": -420.8,
  "recent_5m_delta_fut": -779.6,
  "recent_15m_delta_fut": 3420.5,

  "slope_recent_5m_fut": -132.4,
  "slope_prev_5m_fut": 580.1,
  "slope_change_ratio": -0.23,
  "regime": "reversal_to_selling",

  "recent_series": [
    {
      "ts": "2026-03-26T03:35:00Z",
      "delta_fut": -150.2,
      "delta_spot": -49.9,
      "cum_delta_fut": 4050.3,
      "cum_delta_spot": -80.1
    }
  ]
}
```

### 4.3 为什么不用 v1.0 的 first_half / second_half

v1.0 用前半段 vs 后半段累计值来判断 `slope_regime`，
实现简单，但对 `4h / 1d` 太钝，也不够接近“最近几分钟的斜率突变”。

v1.1 改成：

- `slope_recent_5m_fut`
- `slope_prev_5m_fut`
- `slope_change_ratio`
- `regime`

判断逻辑：

- `slope_prev_5m_fut > 0` 且 `slope_recent_5m_fut < 0`
  -> `reversal_to_selling`
- `slope_prev_5m_fut < 0` 且 `slope_recent_5m_fut > 0`
  -> `reversal_to_buying`
- `abs(recent) > abs(prev) * 1.5`
  -> `accelerating`
- `abs(recent) < abs(prev) * 0.5`
  -> `decelerating`
- 否则
  -> `steady`

样本不足时：

- `minutes_elapsed < 6`
  -> `insufficient_data`

### 4.4 各 timeframe 的序列保留策略

#### 15m

- 保留完整 `recent_series`
- 最多 15 个点

#### 4h / 1d

- 不保留完整窗口全部分钟
- 只保留最近 15 分钟的 `recent_series`
- 其余只保留汇总统计

这样既保留最近微观结构，
又不把 raw bundle 继续放大太多。

### 4.5 `vs_last_closed_bar`

`vs_last_closed_bar` 继续保留，但移动到 `realtime_flow_context` 里构建，
而不是直接写死进 indicator。

原因：

- 它本质上是 Stage 2 消费层语义
- 它需要和 prompt 里的 “当前 partial vs 上个闭合 bar” 直接对齐
- 放在 filter 层更容易按不同 mode 做裁剪

---

## 5. Stage 2 侧设计：`realtime_flow_context`

### 5.1 位置

`realtime_flow_context` 放在 Stage 2 prompt input 的**顶层**，
而不是塞进某个 indicator 的 payload 里面。

理由：

- 这是跨 indicator 的消费层结构
- 它属于“给模型看的执行时上下文”，不是单一指标定义
- 顶层更容易在 prompt 中被模型显式关注

### 5.2 结构

```json
"realtime_flow_context": {
  "bundle_ts": "2026-03-26T03:39:00Z",
  "stage1_scan_ts_bucket": "2026-03-26T03:30:00Z",
  "data_lag_minutes": 9.0,

  "cvd_partial_windows": {
    "15m": {
      "progress_pct": 60.0,
      "cum_delta_fut": 3420.5,
      "cum_delta_spot": -180.3,
      "recent_3m_delta_fut": -420.8,
      "recent_5m_delta_fut": -779.6,
      "slope_recent_5m_fut": -132.4,
      "slope_prev_5m_fut": 580.1,
      "slope_change_ratio": -0.23,
      "regime": "reversal_to_selling",
      "vs_last_closed_bar": {
        "last_closed_delta_fut": 2047.8,
        "direction_consistent": true,
        "magnitude_ratio": 1.67
      },
      "recent_series": [ ... ]
    },
    "4h": {
      "progress_pct": 65.0,
      "cum_delta_fut": -1184.2,
      "cum_delta_spot": -520.3,
      "recent_5m_delta_fut": -645.0,
      "slope_recent_5m_fut": -140.2,
      "slope_prev_5m_fut": 98.3,
      "regime": "reversal_to_selling",
      "vs_last_closed_bar": {
        "last_closed_delta_fut": 8908.1,
        "direction_consistent": false,
        "magnitude_ratio": -0.13
      }
    }
  },

  "since_stage1_increment": {
    "minutes": 9,
    "coverage_limited": false,
    "delta_fut_sum": -620.3,
    "delta_spot_sum": -180.3,
    "volume_fut_sum": 8200.0,
    "max_abs_1m_delta_fut": 540.1,
    "dominant_flow": "selling",
    "new_events_since_scan": [
      {
        "type": "absorption",
        "direction": "bullish",
        "price": 2159.18,
        "minutes_ago": 4,
        "confirm_ts": "2026-03-26T03:35:00Z"
      },
      {
        "type": "initiation",
        "direction": "bearish",
        "price": 2171.05,
        "minutes_ago": 2,
        "confirm_ts": "2026-03-26T03:37:00Z"
      }
    ]
  },

  "live_refs": {
    "orderbook_ofi_norm_fut": -1.29,
    "orderbook_obi_k_dw_close_fut": -0.863,
    "orderbook_exec_confirm_fut": false,
    "orderbook_spot_confirm": true,
    "footprint_15m_window_delta": -57.3
  },

  "freshness": {
    "stage1_to_stage2_gap_minutes": 9.0,
    "prompt_bundle_age_seconds": 14
  }
}
```

### 5.3 `since_stage1_increment` 的意义

它回答的是：

**“从 `Stage 1 scan` 产出之后，到这次 `Stage 2 bundle` 为止，新增了什么？”**

这是 v1.0 没有显式表达的一层。

当 `stage1_to_stage2_gap_minutes` 超过当前保留的分钟 tail 时，
设置 `coverage_limited = true`，
并显式告诉模型“这里只覆盖了最近可用的新增分钟，不代表 scan 之后的完整全部增量”。

模型看到：

- 当前 partial bar 很强

和看到：

- **scan 之后新增的 9 分钟里 actually 是净卖出 / 净买入**

在交易决策上不是一回事。

同理，模型看到：

- 当前 prompt 里有一个 `absorption`

和看到：

- **这个 absorption 是 `Stage 1 scan` 之后新确认的**

也不是一回事。

对于 `entry / pending`，这类“scan 后新出现的 absorption / initiation / exhaustion”
往往有非常高的交易价值。

### 5.4 `new_events_since_scan` 的构建方式

v1.1 的最小实现建议直接读取：

- `indicators.events_summary.payload.most_recent_absorption`
- `indicators.events_summary.payload.most_recent_initiation`
- `indicators.events_summary.payload.most_recent_buying_exhaustion`
- `indicators.events_summary.payload.most_recent_selling_exhaustion`

这些 summary 当前已经包含：

- `confirm_ts`
- `direction`
- `type`
- `price`
- `minutes_ago`

构建规则：

1. 取上述每个 summary
2. 若 `confirm_ts > stage1_scan_ts_bucket`，则视为 `scan` 后新出现
3. 只保留满足条件的 event
4. 统一写入 `since_stage1_increment.new_events_since_scan`

这版实现的优点是：

- 不需要再扫描完整 event arrays
- 复用现有 `events_summary`
- 增量极小，通常 0~3 个事件

这版实现的边界是：

- 它最多只能捕捉“每个事件家族当前最新的那个 post-scan event”
- 如果 scan 后同一事件家族连续确认了多个事件，v1.1 只会保留最新一个

如果后续需要完整覆盖“同一类型 scan 后连续出现多个事件”的情况，
再在 v1.2 升级为直接读取原始 event indicator 的 `recent_7d.events`。

### 5.5 `live_refs` 的意义

`live_refs` 不是新指标，
只是从已有的 live/rolling 指标里抽取最关键的几个字段，供模型快速定位：

- `orderbook_ofi_norm_fut`
- `orderbook_obi_k_dw_close_fut`
- `orderbook_exec_confirm_fut`
- `orderbook_spot_confirm`
- `footprint_15m_window_delta`

这样做的目的是：

- 不重复复制整个 `orderbook_depth` payload
- 不让 prompt 因为重复大对象变得更噪
- 让模型更容易把“新增的 CVD partial 视角”和当前 orderbook / footprint 合起来看

### 5.6 不同 mode 的保留策略

#### Entry

- 完整保留 `15m.recent_series`
- 保留 `4h / 1d` 汇总
- 完整保留 `since_stage1_increment`
- 完整保留 `new_events_since_scan`

#### Pending

- 与 `Entry` 基本一致
- 因为 pending 是否撤单 / 改价，非常依赖最近几分钟是否已经翻向
- 也非常依赖 scan 后是否新出现 `absorption / initiation / exhaustion`

#### Management

- 不保留 `15m.recent_series`
- 只保留 `15m` 摘要
- `4h / 1d` 只保留汇总和 `vs_last_closed_bar`
- `new_events_since_scan` 只保留最近 1~2 个最相关事件即可

这样可以降低 management prompt 的 token 噪音，
同时仍然保留 “4h / 1d 延续是否被破坏，15m 是否出现近端风险”。

---

## 6. runtime 侧设计：`pre_execution_freshness_recheck`

### 6.1 为什么 v1.1 必须加这一层

当前系统已经有：

- `post_invoke_data_age_secs > max_exec_stale_secs` 时跳过执行

但这个保护只能做到：

- **太旧就不下单**

它做不到：

- **如果出现了更新数据，先看一眼是否推翻刚才的结论**

v1.1 要把“直接跳过”升级为“先 recheck，再决定是否执行”。

### 6.2 v1.1 的最小实现

对 `Stage 2 entry` 的 `LONG / SHORT` 可执行决策，
在真正执行前做如下流程：

1. 再读一次最新 `temp_indicator bundle`
2. 如果没有更新，直接执行
3. 如果更新了，但只新 0~1 分钟，直接执行
4. 如果更新 >= 2 分钟：
   - 构建一个**轻量 recheck context**
   - 只看 `realtime_flow_context`
   - 执行 deterministic veto 规则

### 6.3 v1.1 的 deterministic veto 规则

#### LONG veto

满足以下任意 2 条，则 veto：

- `cvd_partial_windows.15m.regime == "reversal_to_selling"`
- `since_stage1_increment.delta_fut_sum < 0`
- `live_refs.orderbook_ofi_norm_fut < 0`
- `live_refs.orderbook_exec_confirm_fut == false` 且 `live_refs.orderbook_spot_confirm == false`

#### SHORT veto

对称：

- `cvd_partial_windows.15m.regime == "reversal_to_buying"`
- `since_stage1_increment.delta_fut_sum > 0`
- `live_refs.orderbook_ofi_norm_fut > 0`
- `live_refs.orderbook_exec_confirm_fut == false` 且 `live_refs.orderbook_spot_confirm == false`

### 6.4 veto 后的行为

v1.1 中的 `veto` 含义不是：

- 永久放弃这笔交易
- 推翻 `Stage 1` 的结构判断

而是：

- **本次执行尝试降级为 `SKIP`**
- **不下单 / 不改单**
- **记录结构化 veto reason，等待下一次正常 Stage 2 cycle 重新判断**

也就是说，`veto` 只阻止“当前这一次已经过了若干分钟、且最新数据与原决策冲突”的执行动作，
它本质上是 execution guard，不是 thesis engine。

建议 runtime 侧记录：

- `recheck_result = "confirm" | "veto" | "not_needed"`
- `recheck_veto_rules_hit = ["rule_a", "rule_c"]`
- `recheck_veto_snapshot`

其中 `recheck_veto_snapshot` 至少包含：

- `cvd_partial_windows.15m.regime`
- `since_stage1_increment.delta_fut_sum`
- `live_refs.orderbook_ofi_norm_fut`
- `live_refs.orderbook_exec_confirm_fut`
- `live_refs.orderbook_spot_confirm`

这样 rollout 时可以回溯：

- 哪些 veto 是正确挡掉了追晚 / 逆势执行
- 哪些 veto 只是短噪音导致的误杀

### 6.5 为什么 v1.1 先不用第二轮 LLM recheck

因为 v1.1 的目标是：

- 先把最有价值的 blind spot 补上
- 先把“过期执行”从 hard skip 升级到 cheap veto

完整的第二轮 mini-LLM recheck 可以留到 v1.2，
避免 v1.1 把链路复杂度一下拉太高。

---

## 7. Prompt 改动

### 7.1 Prompt 要新增的显式指令

在 `entry / pending / management` 的系统 prompt 中新增一段：

```text
REALTIME FLOW CONTEXT

`realtime_flow_context` is newer than the Stage 1 scan.
It summarizes what changed between the scan and now, especially inside the current unfinished 15m / 4h / 1d bar.

Prioritize:
- `cvd_partial_windows.{tf}.regime`
- `cvd_partial_windows.{tf}.vs_last_closed_bar`
- `since_stage1_increment`
- `live_refs`

Use this context to confirm or invalidate the earlier scan thesis.
If recent flow has flipped against the thesis, prefer waiting, cancelling, reducing, or tightening risk rather than blindly following the older scan.
```

### 7.2 自检新增

Stage 2 自检新增一条：

```text
Have I checked whether realtime_flow_context shows a post-scan flow reversal,
acceleration, or invalidation inside the unfinished 15m / 4h / 1d bar?
```

---

## 8. 代码落点

### 8.1 indicator_engine

文件：

- `systems/indicator_engine/src/indicators/i14_cvd_pack.rs`

改动：

- 新增 `build_partial_window_*` helper
- 在 `evaluate()` 中输出 `partial_window`
- 不改现有 `by_window`

### 8.2 llm filter

文件：

- `systems/llm/src/llm/filter/core.rs`
- `systems/llm/src/llm/filter/core_shared.rs`
- `systems/llm/src/llm/filter/core_entry.rs`
- `systems/llm/src/llm/filter/core_pending.rs`
- `systems/llm/src/llm/filter/core_management.rs`

改动：

- 在 `core.rs` 的 Stage 2 root builder 中插入顶层 `realtime_flow_context`
- `core_shared.rs` 新增 `build_realtime_flow_context()`
- `build_realtime_flow_context()` 直接复用 `filtered_indicators["events_summary"]` 构建 `new_events_since_scan`
- `core_entry / pending / management` 只负责 mode-specific 裁剪

### 8.3 provider

文件：

- `systems/llm/src/llm/provider.rs`

改动：

- 目前 `build_finalize_value()` / `serialize_prompt_input_minified()` 对 finalize 附加逻辑偏向 `Entry`
- v1.1 需要把 Stage 2 finalize 的附加构建能力扩展到：
  - `Entry`
  - `Pending`
  - `Management`

目标是：

- 只要是 `EntryPromptStage::Finalize`
- 都能拿到 `prior_scan`
- 都能在 prompt input 顶层构建 `realtime_flow_context`

### 8.4 prompt assets

文件：

- `systems/llm/src/llm/prompt/entry/*.txt`
- `systems/llm/src/llm/prompt/pending_order/*.txt`
- `systems/llm/src/llm/prompt/management/*.txt`

改动：

- 新增 `REALTIME FLOW CONTEXT` 指令段
- 新增自检 1 条

### 8.5 runtime

文件：

- `systems/llm/src/app/runtime.rs`

改动：

- 在模型返回、真正执行交易前新增 `pre_execution_freshness_recheck`
- 保留现有 stale skip 逻辑
- 但在 skip 之前先尝试：
  - `reload latest bundle`
  - `build recheck context`
  - `confirm / veto`

---

## 9. 实施顺序

### Phase 1

只做：

- `cvd_pack.partial_window`
- `realtime_flow_context`
- prompt 更新

目标：

- 先让模型能看见 post-scan intrabar flow

### Phase 2

再做：

- `pre_execution_freshness_recheck`

目标：

- 把“末端过期只会跳过执行”升级为“先 recheck 再执行”

---

## 10. 验证方案

### 10.1 离线对照

至少抽 10~20 个 Stage 2 cycle：

- 对照无 v1.1 vs 有 v1.1
- 特别筛选：
  - `15m.regime = reversal_*`
  - `since_stage1_increment` 与原 scan 方向相反
  - `4h / 1d partial` 与上一闭合 bar 方向不一致

观察：

- `entry` 是否减少追晚
- `pending` 是否更及时撤单 / 改价
- `management` 是否更能识别 15m 近端风险与 4h / 1d 延续破坏

### 10.2 rollout 指标

建议新增：

- `realtime_flow_context_present`
- `rtf_stage1_to_stage2_gap_minutes`
- `rtf_15m_regime`
- `rtf_15m_direction_consistent`
- `rtf_since_stage1_delta_fut_sum`
- `rtf_new_events_since_scan_count`
- `rtf_new_events_since_scan_types`
- `rtf_recheck_triggered`
- `rtf_recheck_vetoed`
- `rtf_recheck_result`
- `rtf_recheck_veto_rules_hit`
- `rtf_recheck_veto_snapshot`

### 10.3 重点看什么

- `LONG / SHORT` 的执行后 5~15 分钟 adverse move 是否下降
- `pending` 被动挂单是否更少挂在已经失效的方向上
- `management` 是否更少在 15m 已经翻向时继续僵硬 HOLD

---

## 11. 最终判断

v1.1 的定位不是“让 Stage 2 拥有全量实时市场重建能力”，
而是：

- **准确补上当前最大的实时盲区：未闭合 HTF bar 内的 Delta / CVD 演变**
- **显式告诉模型哪些信息是 scan 之后新增的**
- **在执行前加一道便宜但有效的新鲜度确认**

如果 v1.1 做完，预期能明显改善：

- `entry` 追晚
- `pending` 撤单 / 改价滞后
- `management` 对 15m 近端风险反应慢

但它仍不是终局。
如果后续还要继续提升，可以在 v1.2 再考虑：

- 二次 mini-LLM recheck
- 更强的 intrabar regime classifier
- 基于秒级 microstructure 的 execution guard
