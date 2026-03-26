# Scan Prompt & Schema 修改方案 v3.1.0

## 0. 这版的目标

`v3.1.0` 只围绕一个目标展开：

- **在不降低 Stage1 输出质量的前提下，尽可能减少 `stage1 scan` 的输入体积、推理负担和返回时间**

这版仍然坚持：

- **Rust 预计算 flow**

但相比 `v3.0.0`，这版把边界再收紧了一步：

- 不再把“Rust 预计算 flow”理解成“LLM 继续输出完整 `flow_parse`”
- 也不把它理解成“LLM 完全失去 flow 纠错能力”

`v3.1.0` 的核心是：

- **Rust 直接拥有确定性 flow truth**
- **模型只输出 context-sensitive flow judgement**
- **同时保留一个极小的异常纠偏口，避免质量回退**

---

## 1. 第一性原理下，当前真正慢在哪里

当前 `stage1 scan` 慢，不是因为模型不会解析市场，而是因为我们把一整类本可确定性完成的 reduction 工作交给了模型。

Stage1 真正高价值的工作是：

- 解析结构
- 解析状态
- 解析主要参与者当前的市场任务
- 解析跨周期主张力与未解决冲突

这些任务需要模型做综合推理。

但 `flow_parse` 中有一部分字段，本质上只是：

- 从 latest closed delta 读方向
- 从 CVD slope 读方向
- 从 whale delta 读方向
- 从 spot/futures 是否同向读关系

这些不是高价值推理，而是 deterministic reduction。

当前代码里，`scan` 输入会把 `cvd / orderbook / footprint / whales` 一起塞进 `now.current_flow_snapshot`，[scan.rs](/data/systems/llm/src/llm/filter/scan.rs#L1071)。  
以 `20260325T104500Z` 样本为例：

- 整份输入约 `187 KB`
- `now.current_flow_snapshot` 约 `16.2 KB`
- `raw_overflow` 约 `22.5 KB`
- `events_newest_to_oldest.latest_24h_detail` 约 `21.0 KB`
- `supporting_context.cvd_path_snapshot.by_window` 约 `8.3 KB`

其中只有第一块 `current_flow_snapshot`，是可以在不降质量的前提下优先动刀的。

---

## 2. v3.1.0 的核心决策

### 2.1 这版保留的原则

- **Rust 接管 raw flow reduction**
- **模型不再从 raw flow 重建完整 `flow_parse`**
- **最终 persisted scan 仍保留完整 `flow_parse`**

### 2.2 这版新增的关键收口

与 `v3.0.0` 相比，`v3.1.0` 进一步明确：

- **确定性字段不再由模型输出**
- **模型输出改为 `flow_override`**
- **Rust 用 `precomputed_flow_base + flow_override` 合成最终 `flow_parse`**
- **模型保留一个极小的 deterministic exception 口，用于极少数异常纠偏**

这比两种极端都更适合“质量第一”：

- 比“模型继续输出完整 `flow_parse`”更省 tokens 和推理
- 比“模型完全失去 flow 纠错权”更稳

---

## 3. 为什么 `flow_override` 比完整 `flow_parse` 更合适

`v3.0.0` 的保守版仍然让模型输出完整 `flow_parse`。

问题是：

- 模型会继续花 token 输出 30 个 enum
- 模型仍然要“确认”一批它几乎不会改的字段
- 这部分确认动作本身，对质量的提升接近于零

近期 reasoning 对比样本表明：

- `xhigh / high / medium` 在 `delta_read / cvd_read.state / whale_read` 上大多高度一致
- 真正开始分化的是：
  - `state_parse`
  - `participant_parse`
  - `cross_timeframe_parse`

这说明：

- 模型的价值不在“重写 deterministic flow enum”
- 模型的价值在“解释 flow 如何约束当前 auction”

所以 `v3.1.0` 的正确做法不是保留完整 `flow_parse` 输出，
而是把模型输出收窄成：

- **只输出真正需要上下文判断的 flow judgement**

---

## 4. v3.1.0 的最终架构

## 4.1 输入侧：删除 raw `current_flow_snapshot`

当前：

```json
"now": {
  "current_flow_snapshot": {
    "cvd": {...},
    "orderbook": {...},
    "footprint": {...},
    "whales": {...}
  }
}
```

`v3.1.0` 改成：

```json
"now": {
  "precomputed_flow_base": {
    "15m": {...},
    "4h": {...},
    "1d": {...}
  },
  "flow_supporting_evidence": {
    "15m": {...},
    "4h": {...},
    "1d": {...}
  }
}
```

这里有两个层：

- `precomputed_flow_base`
  - Rust 直接生成的 deterministic flow 结论层
- `flow_supporting_evidence`
  - 为 participant/evidence 保留的紧凑近端证据层

### 4.2 输出侧：模型不再输出完整 `flow_parse`

模型改为输出：

- `structure_parse`
- `state_parse`
- `flow_override`
- `participant_parse`
- `evidence_trace`
- `cross_timeframe_parse`

也就是说：

- **模型不再输出最终完整 `flow_parse`**
- **模型只输出它真正有价值的 flow judgement**

### 4.3 最终 persisted scan 的 `flow_parse` 仍然完整

最终存储给下游的 scan 仍然保持完整：

- `flow_parse`

但这个 `flow_parse` 不再是模型直接输出，
而是 Rust 合成：

1. 先读取 `precomputed_flow_base`
2. 再读取模型输出的 `flow_override`
3. 如果存在少量 deterministic exceptions，再按合同覆盖
4. 最终合成完整 `flow_parse`

这样做的好处是：

- persisted schema 基本不变
- 下游消费路径基本不变
- 模型输出明显缩小

---

## 5. `flow_override` 是什么

### 5.1 核心原则

`flow_override` 只负责模型真正有价值的 4 个判断字段：

- `cvd_alignment_vs_price`
- `orderbook_pressure_side`
- `orderbook_near_price_constraint`
- `combined_flow_state`

因为这些字段依赖：

- 当前 auction state
- 当前 `control_read.side`
- near-price constraint 的组合判断
- structure/state/flow 的交叉解释

### 5.2 推荐的 response schema

```json
"flow_override": {
  "type": "object",
  "additionalProperties": false,
  "required": ["15m", "4h", "1d"],
  "properties": {
    "15m": { "$ref": "#/flow_override_per_tf" },
    "4h": { "$ref": "#/flow_override_per_tf" },
    "1d": { "$ref": "#/flow_override_per_tf" }
  }
}
```

每个 timeframe：

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
    },
    "exception_delta_futures": {
      "type": "string",
      "enum": ["buying", "selling", "mixed", "unclear"]
    },
    "exception_delta_spot": {
      "type": "string",
      "enum": ["buying", "selling", "mixed", "unclear"]
    },
    "exception_delta_relation": {
      "type": "string",
      "enum": ["aligned", "divergent", "unclear"]
    },
    "exception_cvd_state": {
      "type": "string",
      "enum": ["rising", "falling", "flat", "unclear"]
    },
    "exception_whale_state": {
      "type": "string",
      "enum": ["buyers", "sellers", "mixed", "unclear"]
    },
    "exception_whale_spot_vs_futures_relation": {
      "type": "string",
      "enum": ["aligned", "divergent", "unclear"]
    }
  }
}
```

### 5.3 为什么改成 flat `exception_*`

这里不使用嵌套的 `deterministic_exceptions` object，
而是直接使用 flat 的 optional `exception_*` 字段。

原因是：

- 不同 provider 对 optional object property 的 structured output 行为不完全一致
- 有些 provider 会省略非 required 字段
- 也有些 provider 会倾向于把定义过的可选 object 一并生成出来

flat 结构更稳，因为：

- 即使 provider 倾向于生成可选字段，Rust 也可以逐字段处理
- 如果某个 `exception_*` 的值与 `precomputed_flow_base` 相同，就直接视为“无异议”
- 即使模型每次都把 exception 字段写出来，只要值等于 base，行为也等价于省略

也就是说，
这些 `exception_*` 仍然只是一个极小的安全阀：

- 默认情况下，模型不需要改变 deterministic flow
- 只有当 `flow_supporting_evidence` 明显显示 Rust 预计算失真时，才需要给出 exception
- Rust merge 时，只有“与 base 不同”的 exception 才真的生效

### 5.4 最终 `flow_parse` 的合成

Rust 最终合成：

```json
"flow_parse": {
  "delta_read": {...},
  "cvd_read": {
    "state": "<from precomputed_flow_base>",
    "alignment_vs_price": "<from flow_override>"
  },
  "whale_read": {...},
  "orderbook_read": {
    "pressure_side": "<from flow_override>",
    "near_price_constraint": "<from flow_override>"
  },
  "combined_flow_state": "<from flow_override>"
}
```

如果出现 `exception_*` 字段：

- 与 `precomputed_flow_base` 相同：视为 no-op
- 与 `precomputed_flow_base` 不同：只覆盖对应 deterministic 字段

---

## 6. `precomputed_flow_base` 应该包含什么

`precomputed_flow_base` 只包含 Rust 真正可以稳定拥有的字段：

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

### 6.1 这些字段的推导边界

#### `delta_read`

锚点必须明确：

- **只描述该 timeframe 的 latest closed bar delta**

不允许混入：

- current partial window delta
- footprint window delta
- 更老的 closed bar

partial flow 若与 latest closed bar 明显背离，
只能进入：

- `flow_supporting_evidence`
- `evidence_trace.conflicting_facts`

#### `cvd_read.state`

由 `cvd_slope` 推导：

- 正 -> `rising`
- 负 -> `falling`
- 近零或不可读 -> `flat` / `unclear`

#### `whale_read`

由当前 timeframe 内的 whale delta/notional 推导：

- `state`: buyers / sellers / mixed / unclear
- `spot_vs_futures_relation`: aligned / divergent / unclear

注意：

- 它只描述该 timeframe 内的 spot-vs-futures
- **不是**跨 timeframe 的 whale 分歧

跨 timeframe 的鲸鱼分歧仍然应该进入：

- `cross_timeframe_parse.unresolved_factors`

---

## 7. `flow_supporting_evidence` 必须固定成严格 schema

`v3.1.0` 不接受继续用 `{...}` 占位的松散 evidence。

如果目标是“不降质量”，那么 compact evidence 必须是固定合同。

### 7.1 推荐固定 schema

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

### 7.2 为什么这版比只给原始数值更稳

如果只给：

- `obi_close_fut`
- `ofi_norm_spot`

这种原始数值，
模型还是得重新猜这些 feature 的市场语义。

这会把“字段语义解释”的负担重新甩回模型，
等于 latency 没降干净。

所以质量优先版本建议：

- 保留关键原始数值
- 同时给出 Rust 预计算后的方向语义

也就是：

- `obi_close_fut` + `obi_direction`
- `ofi_direction`

### 7.3 为什么这个 evidence 层足够支持 participant/evidence

当前模型输出里反复引用的 flow 证据，本质上就是：

- latest closed delta
- partial futures/spot divergence
- CVD slope
- whale magnitude
- 最近 ask/bid wall
- 最近 imbalance
- stacked buy/sell 是否贴近当前价

这些内容一旦被锁进固定 schema，
模型就仍然能写出：

- `participant_parse.constraints`
- `participant_parse.evidence`
- `evidence_trace.supporting_facts`
- `evidence_trace.conflicting_facts`

而无需再阅读 16KB 的 raw flow 大块。

### 7.4 控制体积

这层 evidence 的目标应控制在：

- 每个 timeframe 约 `350-500 bytes`
- 三个 timeframe 总体约 `1.1KB - 1.5KB`

这比当前 `current_flow_snapshot` 的 `~16.2KB` 小得多，
同时又比只给几个 enum 更安全。

### 7.5 不需要并入这层的字段

`last_closed_bar_close_location_pct` 不需要放进 `flow_supporting_evidence`。

原因是：

- 它已经存在于 `momentum_snapshot`
- 当前模型引用它，主要是为了描述 bar 位置与 auction 收盘质量
- 它不是 flow ownership 重构必须迁移的字段

所以 `v3.1.0` 保持：

- `last_closed_bar_close_location_pct` 继续留在 `momentum_snapshot`
- `flow_supporting_evidence` 只承载 flow reduction 相关的紧凑证据

---

## 8. `orderbook_read` 仍然不能偷懒

即使 deterministic 部分前置到 Rust，
`orderbook_read` 仍然是最需要谨慎的 flow family。

### 8.1 `pressure_side`

不能只由一个指标决定。

必须综合：

- near-price OBI
- near-price OFI
- 最近 ask / bid wall
- 最近 sell / buy imbalance
- stacked buy / stacked sell 是否贴近当前价

推荐方式：

- Rust 在 evidence 层给出必要事实
- 模型基于当前 auction state 最终判断 `pressure_side`

### 8.2 `near_price_constraint`

它描述的是：

- **当前价格附近最直接约束 auction 的 resting pressure**

所以它不能被简化成：

- 最近一堵墙在哪

也不能被简化成：

- 订单簿总偏向哪边

如果上方和下方都同时存在明确约束，
必须保留：

- `two_sided`

---

## 9. `combined_flow_state` 为什么继续保留原语义

`combined_flow_state` 仍然保留当前语义：

- `supportive`
- `constraining`
- `conflicted`
- `neutral`
- `unclear`

并明确：

- **它始终相对于 `state_parse.control_read.side`**

原因是：

- 当前下游已经直接消费这层 flow-vs-control 关系
- 如果在这一版同时改名、改义、改 ownership，会让质量回退来源无法隔离

所以 `v3.1.0` 建议：

- persisted scan 中保留 `combined_flow_state`
- 模型在 `flow_override` 中输出它
- Rust 在合成 `flow_parse` 时直接注入它

---

## 10. Prompt 需要怎么改

## 10.1 删除两整块

从 scan prompt 中删除：

- `FLOW FIELD SEMANTICS`
- `FLOW REDUCTION IS REQUIRED`

因为模型不再承担 raw flow reduction。

## 10.2 新增一块：`USE PRECOMPUTED FLOW BASE`

推荐写成：

```text
PRECOMPUTED FLOW BASE

`precomputed_flow_base` already contains the deterministic flow read for each timeframe.
Use it as the default flow base.

`flow_override` is where you provide only the flow judgements that require auction context:
- `cvd_alignment_vs_price`
- `orderbook_pressure_side`
- `orderbook_near_price_constraint`
- `combined_flow_state`

Do not rebuild deterministic flow fields from raw indicators unless the compact evidence materially contradicts the provided base.

Use an `exception_*` field only when the compact evidence clearly shows that a provided deterministic field is wrong.
If you use any `exception_*` field, make the reason visible in `evidence_trace` or participant constraints.
```

## 10.3 新增一块：`USE FLOW SUPPORTING EVIDENCE`

```text
FLOW SUPPORTING EVIDENCE

`flow_supporting_evidence` is the compact evidence layer for:
- participant constraints
- supporting facts
- conflicting facts
- partial-vs-closed flow divergence
- near-price orderbook pressure

Do not invent additional raw flow narratives beyond this evidence.
Use the provided evidence to explain how current flow supports, constrains, or conflicts with the current auction.
```

## 10.4 自检改写

保留最小 4 条：

1. Have I used `precomputed_flow_base` as the default deterministic flow read?
2. Have I limited `flow_override` to the context-sensitive flow judgements that matter now?
3. If I used any `exception_*` field, did the compact evidence clearly justify it?
4. Have I used `flow_supporting_evidence` to explain participant constraints and evidence, rather than rebuilding raw flow stories?

---

## 11. Schema 层怎么改

## 11.1 输入 schema

新增：

- `now.precomputed_flow_base`
- `now.flow_supporting_evidence`

删除：

- `now.current_flow_snapshot`

## 11.2 LLM response schema

删除：

- `flow_parse`

新增：

- `flow_override`

这样模型输出体积能明显缩小，
同时保留真正需要上下文判断的 flow judgement。

## 11.3 final persisted scan schema

最终 persisted scan：

- 继续保留 `flow_parse`

也就是说：

- **对模型的 response schema 做 breaking change**
- **对最终 persisted schema 不做 breaking change**

这正是 `v3.1.0` 的关键平衡点。

---

## 12. 代码层建议改动

## 12.1 `filter/scan.rs`

新增：

- `build_precomputed_flow_base(...)`
- `build_flow_supporting_evidence(...)`

删除：

- `build_current_flow_snapshot(...)` 的输入注入

保留：

- `raw_overflow`
- `events_newest_to_oldest.latest_24h_detail`

谨慎裁剪：

- `supporting_context.cvd_path_snapshot.by_window`
  - 可以裁到每个 timeframe 只保留 latest `3` entries
  - **不建议直接裁到 latest `2`**

## 12.2 `provider.rs`

需要改：

- response schema：`flow_parse -> flow_override`
- parser：解析 `flow_override`
- merge step：`precomputed_flow_base + flow_override -> final flow_parse`
- merge step 必须把“exception 值等于 base”视为 no-op

同时：

- persisted scan 对外仍是完整 `flow_parse`

## 12.3 pipeline

建议流程：

1. Rust 构建 scan input
2. Rust 预计算 deterministic flow base
3. Rust 构建固定 `flow_supporting_evidence`
4. LLM 输出 `flow_override`
5. Rust 合成最终 `flow_parse`
6. parser 校验最终 scan

---

## 13. 哪些裁剪这版能做，哪些不能做

### 13.1 这版明确建议做

- 删除 `current_flow_snapshot`
- 引入 `precomputed_flow_base`
- 引入固定 `flow_supporting_evidence`
- response schema 改成 `flow_override`
- `cvd_path_snapshot.by_window` 每个 timeframe 裁到 latest `3` entries

### 13.2 这版明确不建议做

- 删除 `raw_overflow`
- 把 `events_newest_to_oldest.latest_24h_detail` 裁成 top `25`
- 在 persisted schema 中把 `combined_flow_state` 改名

原因很简单：

- 这些改动目前还没有足够证据证明“零质量风险”
- 它们会把 v3.1.0 从“flow reduction ownership 重构”扩大成“多处上下文合同同时改变”

这不符合质量优先原则。

---

## 14. 为什么这版更接近“质量不降”

`v3.1.0` 比 `v3.0.0` 更强的地方在于：

- 它不再让模型为 6 个几乎不会改的 deterministic 字段继续输出确认
- 它把 evidence 合同写成固定 schema，而不是原则描述
- 它没有把高风险裁剪一起绑进来
- 它给 deterministic flow 仍保留了极小的异常纠偏口

因此这版的质量保护来自四层：

1. deterministic truth 交给 Rust
2. context-sensitive judgement 仍交给模型
3. compact evidence 仍足以支撑 participant/evidence
4. risky context cuts 暂不做

在这个前提下，
这版比 `v3.0.0` 更有机会同时做到：

- 比 `v2.1.0` 更快
- 比“删光 flow 让 Rust 全接管”更稳

---

## 15. 预计收益

这版的收益来自 4 部分：

### 15.1 输入大幅缩减

- 删除 `current_flow_snapshot`，约节省 `~16 KB`
- `flow_supporting_evidence` 控制在 `~1.1KB - 1.5KB`

### 15.2 Prompt 简化

- 删除 `FLOW FIELD SEMANTICS`
- 删除 `FLOW REDUCTION IS REQUIRED`

### 15.3 输出缩减

模型从输出完整 `flow_parse`，
改为只输出 `flow_override`。

相比完整 30 个左右 flow enum，
只需输出：

- 12 个主判断 enum
- 极少数情况下才出现的 `deterministic_exceptions`

### 15.4 质量风险仍受控

- persisted schema 基本不变
- 下游消费基本不变
- 模型仍保留上下文 flow judgement
- 异常场景仍有纠偏口

保守预期：

- `stage1 scan` 有机会减少 `1-3 分钟`

在 provider 稳定、输出长度没有明显膨胀的情况下，
这是目前最有机会兼顾质量与时延的方向。

---

## 16. 最终结论

如果目标是：

- **哪怕只快 1 分钟也值得**
- **但绝不能为了快而降低 Stage1 质量**

那么 `v3.1.0` 的正确方向不是：

- 继续让模型输出完整 `flow_parse`
- 也不是直接把全部 flow 真相彻底锁死到 Rust

真正合适的折中是：

- **Rust 直接拥有 deterministic flow base**
- **模型只输出 context-sensitive flow override**
- **Rust 合成最终完整 `flow_parse`**
- **用固定 compact evidence 替代 raw flow**
- **暂缓删除 `raw_overflow` 和裁剪 24h events 这类高风险动作**

一句话总结：

- `v3.0.0` 是 “Rust reduces raw flow, model still returns full flow_parse”
- `v3.1.0` 应升级成 “Rust owns deterministic flow, model returns only the contextual override”

这是目前最接近“在不降质量的前提下，真正减少模型时间输出”的方案。
