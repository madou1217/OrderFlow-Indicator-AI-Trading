# Scan Prompt & Schema 修改方案 v3.0.0

## 0. 这版只解决一个问题

`v3.0.0` 只围绕一个方向展开：

- **把 `flow_parse` 从 LLM 归纳任务改成 Rust 预计算结果**

目标不是做最小修改。

目标是：

- **在不降低 Stage1 市场解析质量的前提下，显著减少 `stage1 scan` 的输入体积、推理负担和返回时间**

这版不再继续从 prompt wording、corner case、或更多自检项入手。

这版只问一个第一性原理问题：

- **哪些东西本来就不该由 LLM 去做？**

如果某个输出层本质上只是把确定性的数值信号归纳成离散状态，
那它不应该继续消耗 LLM 的主要推理预算。

---

## 1. 第一性原理下，当前慢的根因

当前 `stage1 scan` 慢，
不是因为模型不会读市场，
而是因为我们把太多“可确定性归纳”的工作交给了模型。

### 1.1 Stage1 的真正高价值工作，不是 flow reduction

Stage1 的高价值工作是：

- 解析当前结构
- 解析当前状态
- 解析主要参与者在试图让市场做什么
- 解析跨周期主张力和未解决冲突

这几类任务需要综合：

- structure
- state
- path
- events
- background context
- participant task interaction

这些地方才是 LLM 应该花推理预算的地方。

相比之下，
`flow_parse` 的大部分字段只是把数值方向、同步关系、以及近端订单簿约束，
转换成有限状态。

这类工作更像：

- 确定性 reduction
- 规则归纳
- 结构化特征提炼

而不是高价值 market reasoning。

### 1.2 当前输入把 raw flow 直接暴露给模型，体积过大

代码里 [scan.rs](/data/systems/llm/src/llm/filter/scan.rs) 现在会把：

- `cvd`
- `orderbook`
- `footprint`
- `whales`

一起放进 `now.current_flow_snapshot`，
见 [scan.rs](/data/systems/llm/src/llm/filter/scan.rs#L1071)。

以 `20260325T104500Z` 这份输入为例：

- 整份 `scan` 输入约 `187 KB`
- 仅 `current_flow_snapshot` 相关块约 `38 KB`

这意味着：

- 输入里接近 `20%` 的体积，
  本质上是在要求模型自己做 raw flow reduction

这不是轻量提示词问题，
而是输入职责分配错误。

### 1.3 Prompt 当前明确要求模型做 flow reduction

当前 scan prompt 在 [medium_large_opportunity.txt](/data/systems/llm/src/llm/prompt/scan/medium_large_opportunity.txt) 里有两整段要求模型完成 flow reduction：

- `FLOW FIELD SEMANTICS`
- `FLOW REDUCTION IS REQUIRED`

见 [medium_large_opportunity.txt](/data/systems/llm/src/llm/prompt/scan/medium_large_opportunity.txt#L31) 和 [medium_large_opportunity.txt](/data/systems/llm/src/llm/prompt/scan/medium_large_opportunity.txt#L92)。

这等于把模型的注意力显式地拉去处理：

- latest closed delta
- partial delta divergence
- CVD alignment
- whale agreement
- orderbook pressure
- combined flow state

这进一步加重了推理负担。

### 1.4 近期样本证明：不同 reasoning 档位，flow_parse 大多收敛

对 `20260325T104500Z` 和 `20260325T110000Z` 这两组 reasoning 对比样本的复核显示：

- `xhigh`、`high`、`medium` 在 `delta_read / cvd_read / whale_read` 上大多高度一致
- 真正开始分化的，主要是：
  - `state_parse`
  - `participant_parse`
  - `cross_timeframe_parse`

也就是说，
reasoning 档位差异主要影响的是：

- 结构状态的压缩质量
- 参与者任务的表达质量
- 跨周期 tension 的组织质量

而不是 flow reduction 本身。

这说明：

- **`flow_parse` 已经高度接近“规则归纳任务”**

这正是最适合从 LLM 中剥离出来的部分。

---

## 2. v3.0.0 的核心决策

`v3.0.0` 做一个明确的职责重分配：

- **Rust 拥有 raw flow reduction**
- **LLM 保留 final `flow_parse` 的确认与最小纠偏权**

但要注意，
这不是“删掉 flow”。

而是把 Stage1 拆成两层：

### 2.1 Rust 负责

- 从原始指标中预计算 `flow_parse` 的确定性子字段
- 从原始 flow 中提炼 compact evidence
- 把这两部分作为输入提供给模型

### 2.2 LLM 负责

- 解析 structure
- 解析 state
- 解析 participant tasks
- 解析 cross-timeframe tension
- 基于 Rust 提供的预计算 flow 结果，确认或在必要时最小纠偏 `flow_parse`
- 使用 compact flow evidence 来说明：
  - 当前 participant task 的约束
  - 当前 evidence trace 的 supporting / conflicting facts

最终 persisted 的 scan 仍然包含完整 `flow_parse`，
但它不再要求模型从 raw flow 重新归纳。
模型只保留：

- **对 Rust 预计算结果的最终确认权**
- **在 compact evidence 明显冲突时的最小纠偏权**

---

## 3. 这版为什么比“只删字段”更第一性原理

如果只是：

- 删除 `current_flow_snapshot`
- 让模型继续在别处自己猜 `flow_parse`

那只是把问题藏起来。

如果只是：

- 把 `current_flow_snapshot` 缩短
- 但仍要求模型输出完整 `flow_parse`

那只是减轻了一部分负担，
没有修正 ownership。

`v3.0.0` 的核心不是“更短”，
而是：

- **把 deterministic truth 交给 deterministic system**

这正是第一性原理下最清晰的职责边界。

---

## 4. v3.0.0 的最终架构

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

`v3.0.0` 改为：

```json
"now": {
  "precomputed_flow_parse": {
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

也就是：

- **不再把大块 raw flow 丢给模型**
- 改成提供：
  - 一个紧凑的 flow 结论层
  - 一个紧凑的 flow 证据层

## 4.2 模型响应侧：保留 `flow_parse`，但不再让模型从 raw flow 重建它

这版不是：

- 删除 `flow_parse` 输出层

而是：

- **保留 `flow_parse` 作为模型输出层**
- **但把它改成“确认/最小纠偏层”**

模型仍然输出：

- `structure_parse`
- `state_parse`
- `flow_parse`
- `participant_parse`
- `evidence_trace`
- `cross_timeframe_parse`

但 prompt 要明确要求：

- 优先使用 `precomputed_flow_parse`
- 不要从 raw flow 重新归纳四个 flow family
- 只有当 `flow_supporting_evidence` 明显显示预计算结果失真时，才允许修正 `flow_parse`

### 为什么质量优先版本必须保留这一步

如果把 `flow_parse` 完全从模型输出中移除，
那模型就失去了对 Rust flow 规则的最后校验权。

一旦 Rust 在：

- `orderbook_read`
- `combined_flow_state`
- 或某些 context-sensitive flow 解释

上做错，
最终 persisted scan 就会直接写错，
而模型没有任何位置纠正。

按“输出质量第一”的要求，
这一步不能删除。

## 4.3 最终 persisted scan 仍保留完整 `flow_parse`

给下游的最终 scan 结构依然完整：

- `structure_parse`
- `state_parse`
- `flow_parse`
- `participant_parse`
- `evidence_trace`
- `cross_timeframe_parse`

只是其中的 `flow_parse` 不再要求模型从 raw flow 开始推导。
它应当来自：

- Rust 预计算结果
- 加上模型的最终确认或最小纠偏

---

## 5. Rust 预计算 `flow_parse` 的原则

## 5.1 Rust 只能计算“确定性归纳”

Rust 预计算只能覆盖：

- 从数值指标可以稳定推导出的 flow truth

Rust 不应该直接定稿：

- participant task
- sponsorship meaning
- whether a participant objective is accepted / blocked / failing
- cross-timeframe narrative
- context-sensitive flow judgement that depends on the final state read

这些仍然属于 LLM。

进一步说，
即使在 `flow_parse` 内部，
也不是所有子字段都同等适合完全前置到 Rust。

质量优先版本建议区分：

- **Rust 预计算确定性子字段**
- **模型确认或补完上下文相关子字段**

## 5.2 Rust 预计算必须是“规则引擎”，不是“单指标取符号”

尤其是 `orderbook_read`。

像下面这种做法不够好：

- `pressure_side = sign(OBI)`
- `near_price_constraint = nearest wall direction`

这会明显损失质量。

例如在 `20260325T104500Z` 这个样本里，
总 `obi` 是正的，
但 near-price OFI、partial-window OBI、ask wall、ask clusters 共同形成了上方约束。

这类样本说明：

- `orderbook_read` 必须是多信号聚合，
  不能退化成单指标。

## 5.3 Rust 预计算不能吃掉 participant/evidence 需要的近端细节

如果只保留一个很薄的 `flow_parse`，
删光所有 near-price evidence，
模型会在这些地方退化：

- `participant_parse.constraints`
- `participant_parse.evidence`
- `evidence_trace.supporting_facts`
- `evidence_trace.conflicting_facts`

所以 `v3.0.0` 的核心不是：

- “删除 raw flow”

而是：

- **用 compact flow evidence 替代 raw flow**

---

## 6. v3.0.0 建议的最终 `flow_parse`

质量优先版本建议：

- **保持最终 persisted `flow_parse` 的外部合同尽量接近当前 `v2.1.0`**
- **不在这版引入不必要的 breaking rename**

也就是说，
最终 scan 中的 `flow_parse` 仍然维持当前结构：

```json
"flow_parse": {
  "delta_read": {
    "futures": "buying|selling|mixed|unclear",
    "spot": "buying|selling|mixed|unclear",
    "relation": "aligned|divergent|unclear"
  },
  "cvd_read": {
    "state": "rising|falling|flat|unclear",
    "alignment_vs_price": "supports|lags|opposes|unclear"
  },
  "whale_read": {
    "state": "buyers|sellers|mixed|unclear",
    "spot_vs_futures_relation": "aligned|divergent|unclear"
  },
  "orderbook_read": {
    "pressure_side": "buy|sell|mixed|unclear",
    "near_price_constraint": "offers_above|bids_below|two_sided|none|unclear"
  },
  "combined_flow_state": "supportive|constraining|conflicted|neutral|unclear"
}
```

### 为什么这版不改 `combined_flow_state`

当前 `combined_flow_state` 的语义，
已经被下游和当前 schema 使用：

- 它描述的是 flow 相对于 `state_parse.control_read.side` 的支持、约束或冲突

见当前 provider schema：
[provider.rs](/data/systems/llm/src/llm/provider.rs#L3712)。

如果在这版把它改成新的 `combined_flow_read`，
虽然 ownership 更“纯”，
但会带来两个质量风险：

- 下游会失去当前直接可消费的 flow-vs-control 关系
- 模型与 Rust 的边界会因为 rename 和语义迁移同时变化，难以隔离质量回退来源

所以 `v3.0.0` 质量优先版本建议：

- **保留 `combined_flow_state` 不变**
- Rust 只提供它的 provisional 候选值
- 模型在知道 `state_parse.control_read.side` 后做最终确认或修正

---

## 7. 每个字段的 Rust 推导原则

## 7.1 `delta_read`

定义：

- **只锚定该 timeframe 的 latest closed bar delta**

推导：

- `futures`: `delta_fut` 的方向
- `spot`: `delta_spot` 的方向
- `relation`: 两者是否同向

来源：

- 当前已在 [scan.rs](/data/systems/llm/src/llm/filter/scan.rs#L1261) 的 `build_cvd_latest_by_window` 中可直接获得

## 7.2 `cvd_read`

定义：

- `state`: 由该 timeframe 的 `cvd_slope` 推导
- `alignment_vs_price`: 由模型最终确认，不建议完全交给单一 price proxy

原因是：

- `alignment_vs_price` 不只是数值方向关系
- 它经常依赖当前 auction 是 `reentry`、`rejected_back_inside`、还是 `acceptance_attempt`
- 这些状态本身又依赖 structure/state parse

所以质量优先版本建议：

- Rust 预计算 `cvd_read.state`
- 模型基于 `state_parse` 和 compact evidence 最终确认 `alignment_vs_price`

## 7.3 `whale_read`

定义：

- `state`: 当前 timeframe 的 whale direction
- `spot_vs_futures_relation`: 当前 timeframe 内的 futures whale 与 spot whale 是否同向

`dominance` 的确是高价值信息，
但质量优先版本建议先不要把它作为这版的 persisted schema breaking change。

更稳的做法是：

- Rust 在 `flow_supporting_evidence` 里提供 whale magnitude / dominance evidence
- 模型在 `participant_parse` 或 `evidence_trace` 中使用这些信息
- 等 latency 优化单独稳定后，再决定是否把 `dominance` 正式升格到 persisted schema

## 7.4 `orderbook_read`

这是最关键、也最不能偷懒的部分。

### `pressure_side` 不能只看一个指标

必须综合：

- near-price OBI
- near-price OFI
- depth imbalance
- 最近 ask / bid wall
- 最近 ask / bid cluster
- stacked buy / stacked sell 是否贴近当前价
- partial-window footprint 的买卖堆叠方向

推荐规则：

- 先把每一类特征映射成 `buy / sell / mixed / unclear`
- 再做加权投票
- 只有多数明确一致时才输出 `buy` 或 `sell`
- 否则输出 `mixed`

### `near_price_constraint` 的定义

它描述的是：

- **当前价格附近最直接约束 auction 的 resting pressure 结构**

不是：

- “全市场最大的墙在哪”

也不是：

- “最近一堵墙在哪”

所以应优先看：

- 当前价附近最近的 ask/bid wall
- 当前价附近最近的 ask/bid cluster
- 当前价附近 stacked imbalance 的方向

### `two_sided` 必须严格保留

只要：

- 上方有明确 offers
- 下方也有明确 bids

就应该保留 `two_sided`，
不能被强行压成单边。

## 7.5 `combined_flow_state`

质量优先版本建议：

- Rust 只生成 `combined_flow_state` 的 provisional 值
- 模型在完成 `state_parse.control_read.side` 后做最终确认

推荐逻辑保持当前语义不变：

- `supportive`: flow 支持 `control_read.side`
- `constraining`: flow 约束 `control_read.side`
- `conflicted`: flow families 彼此明显冲突
- `neutral`: 无明显支持或约束

这样：

- 下游合同不变
- 模型仍保留最后的上下文纠偏能力

---

## 8. `flow_supporting_evidence` 必须保留什么

为了不降 participant 和 evidence 质量，
`v3.0.0` 不能只保留最终 flow enum。

还必须给模型一个紧凑证据层：

```json
"flow_supporting_evidence": {
  "15m": {
    "latest_closed_delta": {...},
    "partial_window_delta": {...},
    "cvd_slope": ...,
    "whale_delta": {...},
    "orderbook_near_price": {...},
    "footprint_near_price": {...}
  }
}
```

### 建议保留的最小近端证据

每个 timeframe 只保留：

- latest closed futures/spot delta
- current partial window delta 及其与 latest closed 的关系
- CVD slope
- futures/spot whale delta notional
- nearest bid wall / nearest ask wall
- near-price OBI / OFI 离散状态
- whether stacked buy / stacked sell exists near price
- nearest sell imbalance / buy imbalance relative to price

### 不再保留的大块 raw flow

删除：

- 全量 orderbook cluster arrays
- 全量 wall lists
- 全量 footprint stacks arrays
- 全量 whale trades detail
- 全量 CVD by-window raw series

这些都不再直接交给模型。

---

## 9. Prompt 需要怎么改

## 9.1 删除两整块

从 scan prompt 中删除：

- `FLOW FIELD SEMANTICS`
- `FLOW REDUCTION IS REQUIRED`

因为 raw flow reduction 不再由模型完成。

## 9.2 新增一块：`USE PRECOMPUTED FLOW`

推荐写成：

```text
PRECOMPUTED FLOW

`precomputed_flow_parse` already contains the reduced flow parse for each timeframe.
Treat it as the primary flow input for this scan.

Do not recompute delta, CVD, whale, or orderbook states from raw indicators.

Use `flow_supporting_evidence` only to:
- explain participant constraints
- note material divergence between latest closed flow and current partial flow
- connect current flow to the current auction state
- explain unresolved conflicts that still matter now

Keep the provided `precomputed_flow_parse` unless the compact evidence materially contradicts it.
If you adjust a flow field, keep the change minimal and make the conflict visible in `evidence_trace` or participant constraints.
```

## 9.3 `REQUIRED COVERAGE` 改写

每个 timeframe 仍然要求模型输出 5 层：

- `structure_parse`
- `state_parse`
- `flow_parse`
- `participant_parse`
- `evidence_trace`

但 `flow_parse` 的任务改成：

- **确认 Rust 预计算结果**
- **只在 compact evidence 明显冲突时做最小纠偏**

## 9.4 自检改写

把原来的 flow 自检改成：

- Have I used the provided `precomputed_flow_parse` as the default flow read for each timeframe?
- Have I changed any flow field only when `flow_supporting_evidence` materially contradicts the precomputed result?
- If I changed a flow field, did I expose the reason in `evidence_trace` or participant constraints?
- Have I avoided rebuilding raw flow narratives that are not needed for the final market parse?

---

## 10. Schema 层要怎么改

## 10.1 LLM response schema

质量优先版本建议：

- **保留 `flow_parse` 在 LLM response schema 中**
- **保持外部字段合同尽量与当前 `v2.1.0` 一致**

也就是说，
`v3.0.0` 的 latency 优化重点应该放在：

- 输入缩减
- prompt 简化
- reduction ownership 调整

而不是：

- 在同一版里同时做大规模 output schema breaking change

## 10.2 final persisted scan schema

final scan 仍然包含：

- `flow_parse`

且字段合同尽量保持兼容当前下游消费方式。

## 10.3 这版不建议做的 breaking change

质量优先版本建议暂缓：

- 删除 `flow_parse`
- 把 `combined_flow_state` 改名为 `combined_flow_read`
- 把 `whale_read.dominance` 直接加入 persisted schema

这些改动都可能是对的，
但不应与“时延优化主改动”在同一版耦合。

---

## 11. 代码层建议改动

## 11.1 `filter/scan.rs`

当前 [scan.rs](/data/systems/llm/src/llm/filter/scan.rs#L225) 会把：

- `current_flow_snapshot`

注入 `now`。

`v3.0.0` 建议改成：

- `precomputed_flow_parse`
- `flow_supporting_evidence`

并新增独立的 flow reducer：

- `build_precomputed_flow_parse(...)`
- `build_flow_supporting_evidence(...)`

## 11.2 `provider.rs`

当前 [provider.rs](/data/systems/llm/src/llm/provider.rs#L3712) 定义了完整 `flow_parse` 的 provider schema。

`v3.0.0` 建议改成：

- 输入侧新增 `precomputed_flow_parse`
- 输入侧新增严格定义的 `flow_supporting_evidence`
- response schema 继续保留 `flow_parse`
- parser 继续校验完整 `flow_parse`

也就是说：

- **把 raw flow reduction 从模型拿走**
- **但不把 final flow truth 的最后确认权从模型拿走**

## 11.3 Stage1 parse pipeline

建议流程变成：

1. Rust 构建 scan input
2. Rust 预计算 deterministic flow fields
3. Rust 构建 strict compact flow evidence
4. LLM 基于预计算 flow 输出完整 scan，并在必要时最小纠偏 `flow_parse`
5. parser 校验模型输出的 `flow_parse` 是否完整、是否与 input contract 一致

---

## 12. 为什么这版不会降低模型质量

前提是：

- 不删除 near-price evidence
- 不把 `orderbook_read` 简化成单指标
- 不让 Rust 去接管 participant/state/cross-timeframe
- 不剥夺模型对 `flow_parse` 的最终确认权

在这个前提下，
这版不仅不会降低质量，
反而更有机会提升质量。

原因是：

- LLM 不再浪费大量推理预算在 deterministic flow reduction
- LLM 可以把注意力集中到真正高价值的：
  - structure lifecycle
  - value-state compression quality
  - participant task quality
  - cross-timeframe tension quality

同时：

- Rust 预计算如果有偏差，模型仍然有位置纠正它
- output schema 基本不变，便于隔离 latency 改动和质量改动

也就是说，
这版的目标不是“让模型少看一点”，
而是：

- **让模型少做不该由它做的事**

---

## 13. 预计收益

这版的收益来自 3 部分：

### 13.1 输入体积下降

以 `20260325T104500Z` 为例，
当前 `current_flow_snapshot` 约占整份输入的 `20%`。

如果改成：

- `precomputed_flow_parse`
- `flow_supporting_evidence`

输入中与 flow 相关的体积有机会从 `~38 KB` 降到 `~6-10 KB`。

### 13.2 Prompt 简化

删除两整块 flow reduction prompt，
模型不再承担这部分 reasoning。

### 13.3 输出层保持稳定

质量优先版本不依赖“删除输出层”来省时间。

这意味着：

- 输出 token 的收益较小
- 但质量风险显著更低

这符合：

- “哪怕只减少 1 分钟都值得，但不能牺牲质量”

---

## 14. 对时延的预期

这版的目标不是承诺一个精确分钟数，
而是从职责上消掉一整类 LLM 工作。

质量优先版本的保守预期应更克制：

- `stage1 scan` 有机会减少 `1-3 分钟`

影响因素包括：

- 输入 token 减少
- prompt 复杂度下降
- 模型不再自己做 raw flow reduction
- 大部分 flow 相关输入不再需要被模型展开阅读

如果 compact evidence 设计得足够紧，
这是目前最有机会在**不牺牲质量**的前提下，
真正把 scan latency 降下来的方向。

---

## 15. 最终结论

如果 `stage1 scan` 现在最大的瓶颈是：

- 输入太大
- prompt 太重
- 模型在做不该由它做的 flow reduction

那么 `v3.0.0` 的正确方向不是继续微调 prompt，
也不是继续删字段凑短输入。

真正正确的方向是：

- **让 Rust 接管 raw flow reduction**
- **让 LLM 不再从 raw flow 重建 `flow_parse`**
- **让 LLM 保留对 final `flow_parse` 的确认与最小纠偏权**
- **让模型把主要推理预算集中在结构、状态、参与者和跨周期解析**

一句话总结：

- `v2.1.0` 仍然是 “LLM reads and reduces raw flow”
- `v3.0.0` 应该升级成 “Rust reduces raw flow, LLM validates and interprets it”

这才是基于第一性原理、又能兼顾质量与时延的正确终态。
