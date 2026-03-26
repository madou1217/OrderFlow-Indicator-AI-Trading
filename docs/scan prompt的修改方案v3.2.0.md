# Scan Prompt & Schema 修改方案 v3.2.0

## 0. 这版的定位

`v3.2.0` 是对 [scan prompt的修改方案v3.1.0.md](/data/docs/scan%20prompt的修改方案v3.1.0.md) 的正式收口版。

这版只做一件事：

- **撤掉 `exception_*` 设计，明确 deterministic flow 完全归 Rust 所有**

原因不是为了简化实现，
而是为了满足你的最高优先级要求：

- **绝不能为了减少模型时间输出而降低 Stage1 输出质量**

---

## 1. 为什么 `exception_*` 设计要撤掉

在 `v3.1.0` 里，`flow_override` 仍保留了若干 optional `exception_*` 字段，
试图给模型一个“极小的 deterministic 纠偏口”。

这个思路看起来保守，
但从第一性原理看，它其实是不成立的。

根本原因是：

- `precomputed_flow_base` 和 `flow_supporting_evidence` 都来自同一个 source snapshot
- 它们都由 Rust 在同一个 cycle 内构建
- 如果 Rust 用的是同一套 deterministic 逻辑，
  那么所谓的“evidence-verified exception gate”本质上永远只能重算出与 base 相同的结论

也就是说：

1. Rust 先算出 `precomputed_flow_base`
2. 模型给出某个 deterministic `exception_*`
3. Rust 再从同源 `flow_supporting_evidence` 验证
4. 如果验证逻辑与 base 逻辑同源同义，
   那它只会重复 base，而不会提供新的真相来源

结果就是：

- 如果 exception 与 base 相同，等于没意义
- 如果 exception 与 base 不同，Rust 会把它挡回去

这说明：

- `exception_*` 不是一个真实有用的安全阀
- 它只是把 ownership 搞得更复杂
- 它增加 schema 复杂度，却不增加真实能力

所以 `v3.2.0` 的正确做法不是继续加强 gate，
而是直接承认：

- **deterministic truth 就应完全归 deterministic system 所有**

---

## 2. v3.2.0 的核心决策

与 `v3.1.0` 相比，`v3.2.0` 的唯一核心变化是：

- **删除全部 `exception_*` 字段**

从这版开始，ownership 清晰固定为：

### 2.1 Rust 负责

- `delta_read.futures`
- `delta_read.spot`
- `delta_read.relation`
- `cvd_read.state`
- `whale_read.state`
- `whale_read.spot_vs_futures_relation`

也就是说：

- **所有 deterministic flow fields 完全由 Rust 负责**
- **模型不再拥有这些字段的 override 权**

### 2.2 模型负责

模型只负责 4 个真正需要上下文判断的字段：

- `cvd_alignment_vs_price`
- `orderbook_pressure_side`
- `orderbook_near_price_constraint`
- `combined_flow_state`

这 4 个字段之所以仍交给模型，
是因为它们依赖：

- 当前 auction state
- 当前 `state_parse.control_read.side`
- near-price constraint 的组合判断
- structure / state / flow 的交叉解释

---

## 3. 这版为什么更符合第一性原理

第一性原理下，
系统设计的关键不是“尽量给模型留一点点兜底空间”，
而是：

- **让每一类真相由最合适的系统拥有**

对于 deterministic flow truth 来说：

- 如果它真的 deterministic，
  那就不应该让模型参与最终裁决
- 如果它并不 deterministic，
  那它本来就不该放在 Rust-owned base 里

所以最清晰的边界就是：

- Rust 完整拥有 deterministic fields
- 模型完整拥有 contextual fields

而不是中间态：

- Rust 先算 deterministic truth
- 模型再给 deterministic override
- Rust 再用同源 evidence 去验证 override

这种中间态没有增加质量保障，
只是在增加复杂度。

---

## 4. v3.2.0 的最终架构

## 4.1 输入侧

输入仍然按 `v3.1.0` 的方向改：

删除：

- `now.current_flow_snapshot`

新增：

- `now.precomputed_flow_base`
- `now.flow_supporting_evidence`

### `precomputed_flow_base`

只包含 deterministic flow truth：

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
  }
}
```

### `flow_supporting_evidence`

继续保留固定、紧凑、严格的合同，
用于支撑：

- `participant_parse.constraints`
- `participant_parse.evidence`
- `evidence_trace.supporting_facts`
- `evidence_trace.conflicting_facts`
- 以及模型对 4 个 contextual flow fields 的判断

---

## 4.2 输出侧

模型不再输出完整 `flow_parse`。

模型输出：

- `structure_parse`
- `state_parse`
- `flow_override`
- `participant_parse`
- `evidence_trace`
- `cross_timeframe_parse`

其中 `flow_override` 只包含 4 个 required 字段：

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

注意：

- **`exception_*` 已完全删除**
- 不再有 optional deterministic override
- 不再依赖 provider 对 optional 字段的 structured output 行为

---

## 4.3 最终 persisted scan

最终 persisted scan 继续保留完整 `flow_parse`，
但它完全由 Rust 合成：

1. 读取 `precomputed_flow_base`
2. 读取模型输出的 `flow_override`
3. 合成最终 `flow_parse`

也就是：

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

这意味着：

- final persisted schema 不变
- 下游消费路径基本不变
- 模型输出更小
- ownership 更清晰

---

## 5. `flow_supporting_evidence` 的最终要求

`v3.2.0` 继续沿用 `v3.1.0` 的 fixed schema 思路，
并明确它不是 optional context，而是正式合同。

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

这层证据存在的意义不是让模型重建 deterministic flow，
而是让模型仍能高质量完成：

- `participant_parse`
- `evidence_trace`
- `orderbook_pressure_side`
- `orderbook_near_price_constraint`
- `combined_flow_state`

同时：

- `last_closed_bar_close_location_pct` 继续留在 `momentum_snapshot`
- 不并入 `flow_supporting_evidence`

---

## 6. Prompt 需要怎么改

## 6.1 删除两整块

从 scan prompt 中删除：

- `FLOW FIELD SEMANTICS`
- `FLOW REDUCTION IS REQUIRED`

因为模型不再承担 raw flow reduction。

## 6.2 新增一块：`USE PRECOMPUTED FLOW BASE`

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

## 6.3 新增一块：`USE FLOW SUPPORTING EVIDENCE`

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

## 6.4 自检

保留最小 4 条：

1. Have I used `precomputed_flow_base` as the fixed deterministic flow base?
2. Have I limited `flow_override` to the 4 context-sensitive flow judgements?
3. Have I used `flow_supporting_evidence` to explain participant constraints and evidence rather than rebuild deterministic flow?
4. Have I kept `combined_flow_state` relative to `state_parse.control_read.side`?

---

## 7. Schema 层怎么改

### 7.1 输入 schema

新增：

- `now.precomputed_flow_base`
- `now.flow_supporting_evidence`

删除：

- `now.current_flow_snapshot`

### 7.2 LLM response schema

删除：

- `flow_parse`
- `exception_*`

新增：

- `flow_override`

### 7.3 final persisted scan schema

继续保留：

- `flow_parse`

所以这版是：

- **对模型 response schema 做 breaking change**
- **对最终 persisted schema 不做 breaking change**

---

## 8. 哪些裁剪这版能做，哪些不能做

### 8.1 这版明确建议做

- 删除 `current_flow_snapshot`
- 引入 `precomputed_flow_base`
- 引入固定 `flow_supporting_evidence`
- response schema 改成 `flow_override`
- `cvd_path_snapshot.by_window` 每个 timeframe 裁到 latest `3` entries

### 8.2 这版明确不建议做

- 删除 `raw_overflow`
- 把 `events_newest_to_oldest.latest_24h_detail` 裁成 top `25`
- 在 persisted schema 中改名 `combined_flow_state`

原因不变：

- 这些改动还没有足够证据证明“零质量风险”
- 它们会把 v3.2.0 从 flow ownership 重构扩大成多处上下文合同同时变动

---

## 9. 为什么这版比 v3.1.0 更稳

`v3.2.0` 比 `v3.1.0` 更稳，不是因为它更保守，
而是因为它把一个伪能力删掉了。

`v3.1.0` 的 `exception_*` 看起来给模型保留了 deterministic override 权，
但实际上：

- 同源 deterministic gate 不会产生新的真相来源
- 它只会增加复杂度
- 还会引入 provider optional field 的实现噪音

`v3.2.0` 删掉这层后，边界反而更清楚：

1. deterministic truth 完全归 Rust
2. contextual flow judgement 完全归模型
3. compact evidence 继续服务 participant/evidence
4. risky context cuts 暂不做

这比“名义上允许 override，实际上永远过不了 gate”更接近真正的质量优先。

---

## 10. 建议补的实现验证

虽然 `exception_*` 已经删除，
但仍建议补 3 类验证：

### 10.1 merge 单测

至少覆盖：

1. `precomputed_flow_base + flow_override` 正常合成完整 `flow_parse`
2. `combined_flow_state` 仍正确相对于 `state_parse.control_read.side`
3. 缺失 `flow_override` 任一 required 字段时，parser 正确拒绝

### 10.2 rollout 指标

建议至少记录：

1. `precomputed_flow_base_present`
2. `flow_supporting_evidence_present`
3. `flow_override_present`

如果后面还要继续做时延优化 A/B，
这几组指标可以作为基线。

### 10.3 质量验证

上线前后至少对照：

- 15m / 4h / 1d 的 `participant_parse`
- `evidence_trace`
- `main_tension`
- `unresolved_factors`

确认删除 `current_flow_snapshot` 后，
这些高价值输出没有明显退化。

---

## 11. 预计收益

这版的主要收益仍来自：

### 11.1 输入缩减

- 删除 `current_flow_snapshot`，约节省 `~16 KB`
- `flow_supporting_evidence` 控制在 `~1.1KB - 1.5KB`

### 11.2 Prompt 简化

- 删除 `FLOW FIELD SEMANTICS`
- 删除 `FLOW REDUCTION IS REQUIRED`

### 11.3 输出缩减

模型不再输出完整 `flow_parse`，
也不再输出任何 `exception_*`。

相比 `v3.1.0`，
这版进一步去掉了那层 provider-dependent optional noise。

保守预期仍然是：

- `stage1 scan` 有机会减少 `1-3 分钟`

这不是因为减少了某几个 enum，
而是因为：

- 输入大块 flow 被移出
- prompt 少了一整类 reduction 任务
- 模型输出也只保留真正需要上下文判断的部分

---

## 12. 最终结论

如果目标是：

- **哪怕只快 1 分钟也值得**
- **但绝不能因为加速而降低 Stage1 输出质量**

那么 `v3.2.0` 的最终结论是：

- **删除 `current_flow_snapshot`**
- **Rust 直接拥有 deterministic flow base**
- **模型只输出 4 个 context-sensitive flow override 字段**
- **Rust 合成最终完整 `flow_parse`**
- **删除 `exception_*`，不再给模型保留伪 deterministic override 权**
- **保留 `raw_overflow` 与 24h events，避免高风险上下文裁剪**

一句话总结：

- `v3.1.0` 是 “Rust owns deterministic flow, model still has an exception path”
- `v3.2.0` 应收口成 “Rust owns deterministic flow completely, model owns only contextual flow judgement”

这才是目前最接近“在不降质量的前提下，真正减少模型时间输出”的方案。
