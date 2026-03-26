# Scan Prompt & Schema 修改方案 v3.3.0

## 0. 版本定位

`v3.3.0` 是当前这条优化线的**单一 canonical 实施稿**。

它把此前两版已经确认的方向合并成一个版本：

- `v3.2.0`：Rust 完整接管 deterministic flow
- `v3.3.0`：在 `v3.2.0` 基础上，再把 `value_read` 和 `cross_market_parse` 收回 Rust

这意味着：

- 后续如果按这条线落代码，实现者应只看 `v3.3.0`
- `v3.2.0` 可以保留作为讨论记录，但不再作为单独实施文档

这版的目标很单一：

- **在不降低 Stage1 输出质量的前提下，尽可能减少模型输入、输出和无价值推理负担**

---

## 1. 最高原则

本版完全服从下面这个最高原则：

- 不降模型输出质量是第一需求
- 不为了减少模型思考时长而降低输出质量
- 不用考虑工作量

因此，只有满足下面条件的字段，才允许从模型输出层收回 Rust：

1. Rust 已经持有同源 truth
2. 当前模型输出只是把该 truth 重映射成 schema
3. 把该字段收回 Rust 后，不会拿走模型真正的 contextual judgement

只要有一条不满足，就不进这版。

---

## 2. v3.3.0 的最终范围

`v3.3.0` 只做三类 ownership 下沉：

1. `flow_parse` 的 deterministic 部分
2. `timeframes.{tf}.state_parse.value_read`
3. `cross_timeframe_parse.cross_market_parse`

`v3.3.0` 明确**不做**：

- 不动 `structure_parse`
- 不动 `participant_parse`
- 不动 `evidence_trace`
- 不动 `main_tension`
- 不动 `unresolved_factors`
- 不裁 `raw_overflow`
- 不裁 `latest_24h_detail`
- 不新增 structure candidate-layer

原因很明确：

- 这几个层要么仍然依赖模型的 contextual selection
- 要么还没有被证明“零质量风险”

---

## 3. 为什么这三块可以安全收回 Rust

## 3.1 deterministic flow fields

这些字段本质上是规则归纳，不是 contextual reasoning：

- `delta_read.futures`
- `delta_read.spot`
- `delta_read.relation`
- `cvd_read.state`
- `whale_read.state`
- `whale_read.spot_vs_futures_relation`

它们来自：

- latest closed delta
- CVD slope
- whale delta
- spot/futures 同向或分歧关系

这些字段由 Rust 预计算，不会拿走模型真正的判断力。

## 3.2 `value_read`

每个 timeframe 的：

- `pvs`
- `tpo`
- `combined`

当前本质上也是 deterministic mapping。

原因：

- Rust 已经在 `value_state_board` 中计算出 source-native truth
- 当前 provider 对 `combined` 逻辑已经有硬约束
  - `pvs == tpo` 时，`combined = pvs`
  - 否则，`combined = conflicted`

所以把 `value_read` 从模型输出中移走，并不会让 persisted schema 比现在更差。

这里必须写清楚：

- 这不是说当前 `value_read` 完美表达了全部 source-native truth
- 而是说：**在当前 persisted schema 合同下，它已经是 deterministic mapping**

也就是说：

- `v3.3.0` 不会顺手修复当前 schema 对 `reentered_value` 的压缩
- 但也不会把它变得更差

## 3.3 `cross_market_parse`

当前 `cross_market_parse` 只有 3 个字段：

- `spot_vs_futures_gap_pct`
- `flow_driver`
- `latest_4h_delta_relation`

这 3 个字段同样属于 deterministic remapping：

- `spot_vs_futures_gap_pct`
  - 来源：`now.price_anchor.futures_last_price` 与 `now.price_anchor.spot_proxy_price`
- `flow_driver`
  - 来源：`cvd_pack.likely_driver`
- `latest_4h_delta_relation`
  - 来源：最新闭合 4h futures/spot delta 的关系

所以它们也适合从模型输出层收回 Rust。

注意：

- `spot_vs_futures_gap_pct` **不是**来自 `cvd.window_latest`
- 这一点在实现中必须显式纠正

---

## 4. ownership 的最终划分

## 4.1 Rust-owned

### A. deterministic flow base

- `delta_read.futures`
- `delta_read.spot`
- `delta_read.relation`
- `cvd_read.state`
- `whale_read.state`
- `whale_read.spot_vs_futures_relation`

### B. `value_read`

每个 timeframe：

- `state_parse.value_read.pvs`
- `state_parse.value_read.tpo`
- `state_parse.value_read.combined`

### C. `cross_market_parse`

- `cross_market_parse.spot_vs_futures_gap_pct`
- `cross_market_parse.flow_driver`
- `cross_market_parse.latest_4h_delta_relation`

## 4.2 Model-owned

模型仍然负责：

- `structure_parse`
- `state_parse.control_read`
- `state_parse.range_state`
- `state_parse.auction_state`
- `state_parse.sponsorship_state`
- `flow_override`
- `participant_parse`
- `evidence_trace`
- `shared_levels`
- `main_tension`
- `unresolved_factors`

这条边界是本版最重要的设计结论：

- Rust 只接管 deterministic remapping
- 模型继续承担当前市场的 contextual interpretation

---

## 5. 输入层怎么改

## 5.1 删除

删除：

- `now.current_flow_snapshot`

## 5.2 新增

新增：

- `now.precomputed_flow_base`
- `now.flow_supporting_evidence`
- `now.precomputed_value_read`
- `now.precomputed_cross_market`

## 5.3 保留

继续保留：

- `now.value_state_board`
- `now.bracket_board`
- `now.structure_nodes_near_current`
- `raw_overflow`
- `events_newest_to_oldest.latest_24h_detail`

同时保留 `cvd_path_snapshot.by_window`，但每个 timeframe 只保留 latest `3` entries。

原因：

- `raw_overflow` 和完整事件层当前还没有足够证据证明可以“零风险裁剪”
- 这版的主收益已经来自 flow/value/cross-market ownership 下沉，不需要为了再挤一点输入而冒质量风险

---

## 6. 新增输入合同的最终定义

## 6.1 `precomputed_flow_base`

每个 timeframe：

```json
"precomputed_flow_base": {
  "15m": {
    "delta_read": {
      "futures": "buying|selling|mixed|unclear",
      "spot": "buying|selling|mixed|unclear",
      "relation": "aligned|divergent|unclear"
    },
    "cvd_read": {
      "state": "rising|falling|flat|unclear"
    },
    "whale_read": {
      "state": "buyers|sellers|mixed|unclear",
      "spot_vs_futures_relation": "aligned|divergent|unclear"
    }
  },
  "4h": { "...": "..." },
  "1d": { "...": "..." }
}
```

## 6.2 `flow_supporting_evidence`

每个 timeframe：

```json
"flow_supporting_evidence_per_tf": {
  "latest_closed_delta_fut": -10679.7,
  "latest_closed_delta_spot": 1755.07,
  "partial_window_delta_fut": -12420.85,
  "partial_window_delta_spot": -824.31,
  "partial_vs_closed_divergent": true,
  "cvd_slope": -13720.48,
  "whale_delta_fut": -17909279.31,
  "whale_delta_spot": 733486.03,
  "obi_close_fut": -0.489,
  "obi_direction": "sell|buy|mixed|unclear",
  "ofi_direction": "sell|buy|mixed|unclear",
  "nearest_ask_wall_price": 2192.93,
  "nearest_bid_wall_price": 2184.03,
  "nearest_sell_imb_prices": [2189.01, 2192.36],
  "nearest_buy_imb_prices": [2180.10],
  "stacked_sell_near_price": true,
  "stacked_buy_near_price": true
}
```

这层的职责不是让模型重建 deterministic flow，而是支撑：

- `participant_parse.constraints`
- `participant_parse.evidence`
- `evidence_trace.supporting_facts`
- `evidence_trace.conflicting_facts`
- `orderbook_pressure_side`
- `orderbook_near_price_constraint`
- `combined_flow_state`

注意：

- `last_closed_bar_close_location_pct` 继续留在 `momentum_snapshot`
- 不并入 `flow_supporting_evidence`

## 6.3 `precomputed_value_read`

```json
"precomputed_value_read": {
  "15m": {
    "pvs": "inside_value|above_value|below_value|accepted_above|accepted_below|rejected_from_above|rejected_from_below",
    "tpo": "inside_value|above_value|below_value|accepted_above|accepted_below|rejected_from_above|rejected_from_below",
    "combined": "inside_value|above_value|below_value|accepted_above|accepted_below|rejected_from_above|rejected_from_below|conflicted"
  },
  "4h": { "...": "..." },
  "1d": { "...": "..." }
}
```

规则：

- `pvs` 直接取 `value_state_board.{tf}.pvs`
- `tpo` 直接取 `value_state_board.{tf}.tpo`
- `combined`
  - `pvs == tpo` -> `combined = pvs`
  - `pvs != tpo` -> `combined = conflicted`

## 6.4 `precomputed_cross_market`

```json
"precomputed_cross_market": {
  "spot_vs_futures_gap_pct": 0.05,
  "flow_driver": "futures_led|spot_led|balanced|unclear",
  "latest_4h_delta_relation": "aligned|divergent|flat_or_unclear"
}
```

规则：

- `spot_vs_futures_gap_pct`
  - 来自 `price_anchor`
- `flow_driver`
  - 来自 `cvd_pack.likely_driver`
  - 上游若给出 `mixed` 或 `balanced`，统一归一为 `balanced`
- `latest_4h_delta_relation`
  - 来自最新闭合 4h futures/spot delta 关系

---

## 7. 模型 response schema 怎么改

## 7.1 从模型输出中删除

删除：

- 完整 `flow_parse`
- `timeframes.{tf}.state_parse.value_read`
- `cross_timeframe_parse.cross_market_parse`

也就是说，模型不再输出：

- 6 个 deterministic flow enums × 3 TF
- `value_read` 9 个 enums
- `cross_market_parse` 3 个字段

## 7.2 模型新增/保留的 flow 输出

模型不输出完整 `flow_parse`，而只输出 `flow_override`：

```json
"flow_override_per_tf": {
  "type": "object",
  "additionalProperties": false,
  "required": [
    "cvd_alignment_vs_price",
    "orderbook_pressure_side",
    "orderbook_near_price_constraint",
    "combined_flow_state"
  ],
  "properties": {
    "cvd_alignment_vs_price": {
      "type": "string",
      "enum": ["supports", "lags", "opposes", "unclear"]
    },
    "orderbook_pressure_side": {
      "type": "string",
      "enum": ["buy", "sell", "mixed", "unclear"]
    },
    "orderbook_near_price_constraint": {
      "type": "string",
      "enum": ["offers_above", "bids_below", "two_sided", "none", "unclear"]
    },
    "combined_flow_state": {
      "type": "string",
      "enum": ["supportive", "constraining", "conflicted", "neutral", "unclear"]
    }
  }
}
```

## 7.3 persisted full scan schema 不变

最终 persisted full scan 仍包含：

- 完整 `flow_parse`
- `state_parse.value_read`
- `cross_timeframe_parse.cross_market_parse`

所以这版是：

- 对模型 response schema 做 breaking change
- 对最终 persisted schema 不做 breaking change

---

## 8. parser / merge 的最终流程

`v3.3.0` 必须拆成两阶段：

1. 校验模型返回的 reduced response schema
2. Rust 注入：
   - `precomputed_flow_base`
   - `precomputed_value_read`
   - `precomputed_cross_market`
3. Rust 用 `precomputed_flow_base + flow_override` 合成完整 `flow_parse`
4. Rust 合成最终 persisted full scan
5. 再校验最终 persisted full scan schema

### 为什么必须拆两阶段

因为当前 provider 还把这些字段当成“模型必须输出”：

- `flow_parse`
- `value_read`
- `cross_market_parse`

如果不拆两阶段，reduced response 会被直接判缺字段。

---

## 9. 最终 persisted scan 的合成方式

最终完整 `flow_parse`：

```json
"flow_parse": {
  "delta_read": "<from precomputed_flow_base>",
  "cvd_read": {
    "state": "<from precomputed_flow_base>",
    "alignment_vs_price": "<from flow_override>"
  },
  "whale_read": "<from precomputed_flow_base>",
  "orderbook_read": {
    "pressure_side": "<from flow_override>",
    "near_price_constraint": "<from flow_override>"
  },
  "combined_flow_state": "<from flow_override>"
}
```

最终完整 `state_parse.value_read`：

```json
"value_read": "<from precomputed_value_read>"
```

最终完整 `cross_market_parse`：

```json
"cross_market_parse": "<from precomputed_cross_market>"
```

这意味着：

- Stage 2 仍然看到完整 scan
- 下游字段语义不变
- ownership 更清晰

---

## 10. prompt 要怎么改

## 10.1 删除两整块旧 flow 要求

从 scan prompt 中删除：

- `FLOW FIELD SEMANTICS`
- `FLOW REDUCTION IS REQUIRED`

因为模型不再承担 raw flow reduction。

## 10.2 新增：`USE PRECOMPUTED FLOW BASE`

推荐写成：

```text
PRECOMPUTED FLOW BASE

`precomputed_flow_base` already contains the deterministic flow read for each timeframe.
Use it as the fixed deterministic flow base.

Do not rebuild deterministic flow fields from raw indicators.

`flow_override` is where you provide only the flow judgements that require auction context:
- `cvd_alignment_vs_price`
- `orderbook_pressure_side`
- `orderbook_near_price_constraint`
- `combined_flow_state`
```

## 10.3 新增：`USE FLOW SUPPORTING EVIDENCE`

```text
FLOW SUPPORTING EVIDENCE

`flow_supporting_evidence` is the compact evidence layer for:
- participant constraints
- supporting facts
- conflicting facts
- partial-vs-closed flow divergence
- near-price orderbook pressure

Use it to explain how current flow constrains or supports the current auction.
Do not rebuild deterministic flow fields from it.
```

## 10.4 新增：`USE PRECOMPUTED VALUE READ`

```text
PRECOMPUTED VALUE READ

`precomputed_value_read` already contains the value-state truth for each timeframe
under the current persisted scan contract.

Use it as the fixed value-state base.
Do not restate or recalculate it.

Your task is to interpret the market around that value-state truth through:
- control
- range state
- auction state
- sponsorship
- participant tasks
```

## 10.5 新增：`USE PRECOMPUTED CROSS-MARKET`

```text
PRECOMPUTED CROSS-MARKET

`precomputed_cross_market` already contains the cross-market truth.
Use it as fixed context.

Do not restate or recalculate:
- `spot_vs_futures_gap_pct`
- `flow_driver`
- `latest_4h_delta_relation`

Your task is to use that truth when writing:
- participant constraints
- main tension
- unresolved factors
```

## 10.6 自检

保留最小 7 条：

1. Have I used `precomputed_flow_base` as the fixed deterministic flow base?
2. Have I limited `flow_override` to the 4 context-sensitive flow judgements?
3. Have I used `flow_supporting_evidence` to explain constraints and evidence rather than rebuild deterministic flow?
4. Have I used `precomputed_value_read` as the fixed value-state base?
5. Have I used `precomputed_cross_market` as fixed cross-market truth rather than restating it?
6. Have I kept `combined_flow_state` relative to `state_parse.control_read.side`?
7. Could a downstream model understand the current market without reconstructing the raw input?

---

## 11. 哪些裁剪这版明确做，哪些不做

## 11.1 这版明确建议做

- 删除 `current_flow_snapshot`
- 引入 `precomputed_flow_base`
- 引入固定 `flow_supporting_evidence`
- 引入 `precomputed_value_read`
- 引入 `precomputed_cross_market`
- response schema 改成 `flow_override`
- 从模型 response schema 中移除 `value_read`
- 从模型 response schema 中移除 `cross_market_parse`
- `cvd_path_snapshot.by_window` 每个 timeframe 裁到 latest `3` entries

## 11.2 这版明确不建议做

- 删除 `raw_overflow`
- 把 `events_newest_to_oldest.latest_24h_detail` 裁成 top `25`
- 动 `structure_parse`
- 改 persisted schema 的语义合同

原因：

- 这些改动还没有足够证据证明“零质量风险”
- 它们会把这版从 deterministic remapping ownership 下沉，扩大成高风险上下文合同改写

---

## 12. 实现验证要求

## 12.1 merge 单测

至少覆盖：

1. `precomputed_flow_base + flow_override` 正常合成完整 `flow_parse`
2. `precomputed_value_read` 正常注入 3 个 timeframe 的 `value_read`
3. `precomputed_cross_market` 正常注入 `cross_market_parse`
4. `combined_flow_state` 仍正确相对于 `state_parse.control_read.side`
5. reduced response 缺失任一 required `flow_override` 字段时，parser 正确拒绝

## 12.2 rollout 指标

建议至少记录：

1. `precomputed_flow_base_present`
2. `flow_supporting_evidence_present`
3. `precomputed_value_read_present`
4. `precomputed_cross_market_present`
5. `flow_override_present`

## 12.3 质量验证

上线前后至少对照：

- `participant_parse`
- `evidence_trace`
- `main_tension`
- `unresolved_factors`
- 15m / 4h / 1d 的 `structure_parse`

确认删除 `current_flow_snapshot` 并收回 `value_read / cross_market_parse` 后，
这些高价值输出没有明显退化。

---

## 13. 预计收益

### 13.1 输入侧

主要收益仍来自：

- 删除 `current_flow_snapshot`，约节省 `~15-16 KB`
- `flow_supporting_evidence` 控制在 `~1.1KB - 1.5KB`
- `precomputed_value_read + precomputed_cross_market` 总体很小
- `cvd_path_snapshot.by_window` 裁到 latest `3`

总体仍然是明显净减。

### 13.2 prompt 侧

prompt 少掉一整类任务：

- 不再要求模型做 raw flow reduction
- 不再要求模型重复输出 `value_read`
- 不再要求模型重复输出 `cross_market_parse`

### 13.3 输出侧

模型不再输出：

- 完整 `flow_parse`
- `value_read`
- `cross_market_parse`

只输出真正需要上下文判断的剩余层。

### 13.4 保守预期

保守预期仍然是：

- `stage1 scan` 有机会减少 `1-3 分钟`

这不是因为少了几个 enum，
而是因为：

- 删除了大块 raw flow 输入
- 减少了一整类 deterministic remapping
- 缩短了模型 response

---

## 14. 为什么这版仍符合“绝不降质量”

### 14.1 不动模型真正的高价值判断层

这版没有碰：

- `structure_parse`
- `participant_parse`
- `evidence_trace`
- `main_tension`
- `unresolved_factors`

也就是没有碰模型最核心的 contextual explanation。

### 14.2 只拿走 deterministic remapping

这版拿走的，是模型当前只是在做“重复搬运”的部分：

- deterministic flow fields
- `value_read`
- `cross_market_parse`

这些字段在当前 persisted schema 合同下，本来就不应消耗高成本 reasoning。

### 14.3 最终 persisted scan 仍然完整

Stage 2 和日志系统看到的仍然是完整 scan：

- `flow_parse`
- `value_read`
- `cross_market_parse`

因此，这版不是“删字段换速度”，而是：

- 把 deterministic ownership 收回到更合适的层
- 同时保持最终 scan 合同不变

---

## 15. 最终结论

如果目标是：

- 哪怕只快 1 分钟也值得
- 但绝不能因为加速而降低 Stage1 输出质量

那么 `v3.3.0` 的最终结论是：

- 删除 `current_flow_snapshot`
- Rust 直接拥有 deterministic flow base
- Rust 直接拥有 `value_read`
- Rust 直接拥有 `cross_market_parse`
- 模型只输出 contextual `flow_override`
- 模型继续负责 `structure_parse / participant_parse / evidence_trace / main_tension / unresolved_factors`
- Rust 合成最终完整 persisted scan
- 保留 `raw_overflow` 与完整事件层，不做高风险上下文裁剪

一句话总结：

- `v3.3.0` 不是更激进的压缩版
- `v3.3.0` 是把当前已被证明 deterministic 的 remapping 全部从模型输出层拿走的**单一最终实施稿**
