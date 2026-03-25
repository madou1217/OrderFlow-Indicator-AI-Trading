# Scan Prompt & Schema 修改方案 v2.1.0

## 0. 这版要解决什么

如果这是对 `stage1 scan` 的最后一次修改，
这版不再继续从样本症状出发补 corner case。

`v2.1.0` 要解决的是更根本的问题：

- **模型为什么没有稳定地把高价值市场真相表达出来**

从第一性原理看，
这个问题至少同时包含两部分：

### A. schema 没有给某些真相正式位置

例如：

- 双 value model 冲突
- flow layer 之间的分歧
- participant task 当前受到的约束

### B. 模型没有被明确要求完成“高价值信息归纳”

即使输入里已经有：

- OBI
- OFI
- whale delta
- footprint delta
- CVD
- liquidity walls
- depth imbalance

模型仍然可能在 5000+ 行输入前主动做信息筛选，
把这些 flow 信息降级为“细节”，
最后只留下一个粗糙 summary。

所以 `v2.1.0` 的目标不是单纯“加一个新层”，
而是同时完成两件事：

1. **给高价值真相正式位置**
2. **强制模型完成这些真相的归纳**

---

## 1. 第一性原理下，当前问题的根因

当前样本暴露出的错误，
表面上看像：

- `4h delta` 权重不稳
- `1d value_location` 把双源冲突压丢
- `OFI / OBI / whale` 没被充分表达
- 参与者任务表达不够稳定

但从根上看，
根因其实只有 6 个。

### 1.1 需求没有把“市场真相”拆成足够清晰的层

现在的表达里，
常常把下面这些混在一起：

- structure
- state
- flow
- participant task
- evidence

结果是：

- 模型只能边读边压缩
- 不同性质的真相被迫写进同一层

### 1.2 schema 没有允许“同时成立的真相”并存

只要 schema 只有单一 `value_location`，
那下面这种真实状态就一定会被压缩：

- `PVS = rejected_from_above`
- `TPO = reentered_value`

这不是模型错误，
而是 schema 不允许它同时说真话。

### 1.3 flow 没有被定义为“必答解析对象”

当前 prompt 虽然允许模型使用 flow，
但没有把它定义成：

- **每个 timeframe 都必须完成的独立归纳任务**

于是模型面对超长输入时，
最容易忽略的正是 flow 细节。

### 1.4 flow 输出过于自由文本化

就算给了 `flow_parse`，
如果仍然是：

```json
"cvd_read": "",
"whale_read": "",
"orderbook_read": ""
```

那本质上还是把高价值信息放进 prose。

这种 schema 不足以稳定约束模型完成真正的 flow reduction。

### 1.5 flow 字段定义还不够严格

即使引入了 `flow_parse`，
如果 flow 字段定义仍然不够严格，
模型依然可能混淆：

- latest closed bar delta
- current partial window delta
- 同 timeframe 的 source divergence
- 跨 timeframe 的 sponsorship divergence
- supportive / constraining 到底相对于什么

所以 `v2.1.0` 不只是“新增 flow 层”，
还必须把 flow 字段的时间锚点、层级边界、参照系定义清楚。

### 1.6 prompt 没有明确写出下游验收标准

本次调用里的模型当然不需要知道自己是“Stage 1”还是“Stage 2”。

但它需要知道：

- **它的输出必须让下游不重建 raw input 也能理解当前市场**

这不是 pipeline 叙事，
而是任务验收标准。

这个锚点应该保留。

---

## 2. v2.1.0 的最终目标

这版不再追求“更会总结”。

这版追求的是：

- **让模型稳定地产出当前市场的结构化真相**

最终应明确交付 5 类内容：

### 2.1 当前什么结构仍在组织价格

- 当前 active range
- 当前 active / failing / invalidated structures
- 当前真正重要的 demand / supply / key levels

### 2.2 当前 auction 处于什么状态

- 当前各 value model 的状态
- 当前 range state
- 当前 auction state
- 当前 control read
- 当前 sponsorship state

### 2.3 当前 flow 在支持什么、约束什么、冲突什么

- latest closed delta 在表达什么
- CVD 在表达什么
- whale sponsorship 在表达什么
- orderbook / OBI / OFI / near-price walls 在表达什么
- 这些 flow layer 是否一致

### 2.4 当前主要参与者在试图让市场做什么

- 当前 participant task
- task status
- 当前约束这个 task 的因素

### 2.5 当前跨周期最重要的主张力是什么

- shared levels
- cross-market confirmation / divergence
- unresolved conflicts

---

## 3. 这版的核心设计原则

## 3.1 不给模型讲 pipeline 身份，只给任务和验收标准

最终 prompt 不需要写：

- 你是 Stage 1
- 你不是 Stage 2
- 你不是 forecast engine

这些都不是本次调用的核心任务对象。

但最终 prompt 应该保留一条简洁的验收标准：

```text
The output is only successful if a downstream model can understand the current market without reconstructing the raw input.
```

这句应该保留，
因为它定义了：

- 输出必须可消费
- 输出必须是自足的市场解析

### 3.2 不再只定义“可以用什么”，而是定义“必须完成什么归纳”

特别是 flow。

prompt 不应只是说：

- 你可以参考 OBI / OFI / whale / delta

而应明确说：

- **每个 timeframe 都必须完成 flow reduction**

### 3.3 不要求模型发明绝对阈值

像下面这种原始数字：

- `obi_k_dw_close_fut = -0.489`
- `ofi_norm_spot = -1.69`

如果 prompt 让模型自己猜“这到底算不算很强”，
模型就容易忽略。

所以这版要求模型优先完成的是：

- 方向
- 同步 / 分歧关系
- 是否贴近当前价格
- 是否正在约束当前 auction 或 participant task

而不是强行解释绝对数值阈值。

### 3.4 用强约束结构替代自由文本漂移

真正重要的 parse 层，
应该优先用：

- enum-like states
- factual relations
- constrained subfields

而不是纯 prose。

---

## 4. v2.1.0 的终态理解

这版建议把任务收敛成两条核心定义：

- **market = structure + state + flow**
- **participant = current task + task status + constraints**

这是面向设计和 schema 的核心原则。

模型最终收到的 prompt，
不必直接写这两句。

但 prompt 和 schema 应完全围绕这两条原则展开。

---

## 5. v2.1.0 推荐终态输出契约

## 5.1 Top level

- `schema_version`
- `meta`
- `timeframes`
- `cross_timeframe_parse`

## 5.2 Each timeframe

- `structure_parse`
- `state_parse`
- `flow_parse`
- `participant_parse`
- `evidence_trace`

这 5 层的阅读顺序必须固定：

1. 先看结构
2. 再看状态
3. 再看 flow
4. 再看 participant tasks
5. 最后看证据

这个顺序的目标是：

- 让下游先读市场骨架
- 再读当前状态
- 再读 flow 是否支持这个状态
- 再读参与者当前任务
- 最后才看支撑和冲突证据

---

## 6. 各层字段的最终定义

## 6.1 `structure_parse`

这一层只负责结构。

建议保留：

- `active_range`
- `range_width_vs_atr`
- `dominant_demand_zone`
- `dominant_supply_zone`
- `key_levels`
- `invalidation_level`
- `structure_lifecycle`

这一层回答：

- 当前什么结构在组织价格
- 哪些 levels 现在重要
- 哪些结构 active / failing / invalidated

这里不表达：

- flow 强弱
- participant motive
- 方向 verdict

---

## 6.2 `state_parse`

这一层只负责当前 auction state。

建议改为：

```json
"state_parse": {
  "value_read": {
    "pvs": "",
    "tpo": "",
    "combined": ""
  },
  "range_state": "",
  "auction_state": "",
  "control_read": {
    "side": "",
    "clarity": ""
  },
  "sponsorship_state": ""
}
```

这里最关键的是：

- **不再使用单一 `value_location`**

而是显式保留：

- `value_read.pvs`
- `value_read.tpo`
- `value_read.combined`

这样下面这种真实市场状态就能稳定表达：

- `pvs = rejected_from_above`
- `tpo = reentered_value`
- `combined = conflicted`

这不是补某个样本，
而是让 schema 允许市场真相并存。

### `value_read.combined` 的枚举应收敛到最小增量

为了让下游改动最小，
建议：

- `value_read.combined` 直接沿用当前 `value_location` 的 7 个枚举值
- 仅额外新增 1 个值：`conflicted`

建议枚举集合为：

- `above_value`
- `below_value`
- `inside_value`
- `accepted_above`
- `accepted_below`
- `rejected_from_above`
- `rejected_from_below`
- `conflicted`

定义方式如下：

- 当 `pvs` 与 `tpo` 一致时，`combined` = 该一致值
- 当 `pvs` 与 `tpo` 不一致时，`combined` = `conflicted`

这样下游如果只需要一个压缩值，
可以继续读取：

- `value_read.combined`

下游如果需要更细节的冲突来源，
再读取：

- `value_read.pvs`
- `value_read.tpo`

这也是当前实现改动最小的方案：

- finalize 中读取 `value_location` 的 JSON pointer
- 从 `/market_state/value_location`
- 改为 `/state_parse/value_read/combined`

同时，
下游只需要在原 7 个枚举值基础上新增处理：

- `conflicted`

### `auction_state` 必须只回答 auction process，不重复 `range_state`

`range_state` 与 `auction_state` 的边界必须明确：

- `range_state` 负责回答：价格当前如何与 active range 交互
- `auction_state` 负责回答：当前 auction process 正在做什么

也就是说：

- `range_state` 是位置 / 区间交互描述
- `auction_state` 是拍卖过程描述

建议 `auction_state` 枚举收敛为：

- `balancing`
- `reentry`
- `acceptance_attempt`
- `rejection_attempt`
- `rejected_back_inside`
- `auction_unresolved`

定义建议：

- `balancing` = auction 正在 value / balance 内旋转
- `reentry` = 价格刚回到原有 value / balance 内，正在重新组织
- `acceptance_attempt` = 正在尝试在 value 或 range 外建立接受
- `rejection_attempt` = 正在尝试把外侧拍卖打回内部
- `rejected_back_inside` = 外侧探出已失败，auction 被明确打回内部
- `auction_unresolved` = 当前拍卖过程不够清晰，不能稳定归类

这样 `auction_state` 补充的是：

- 当前 process

而不是重复：

- `range_state` 的 edge interaction

### `sponsorship_state` 应保留在 `state_parse`

`sponsorship_state` 不应消失。

从第一性原理看，
它表达的不是：

- 当前 flow 是否支持 control side

而是：

- **当前 auction 背后的 sponsorship 是否稳固、脆弱、衰减、缺失，或仍未解决**

这和 `combined_flow_state` 有交集，
但不是同一件事。

建议继续保留：

- `active`
- `fragile`
- `fading`
- `absent`
- `unresolved`

并把它放在：

- `state_parse.sponsorship_state`

而不是：

- `flow_parse`
- `participant_parse`

原因：

- `combined_flow_state` 回答的是：flow 相对于 `control_read.side` 是否支持、约束或冲突
- `sponsorship_state` 回答的是：当前 auction 是否有稳定 sponsorship 在支撑它

也就是说：

- `flow_parse` 负责 flow relationship
- `state_parse.sponsorship_state` 负责 sponsorship condition

这也是当前实现改动最小的方案：

- finalize 中读取 `sponsorship_state` 的 JSON pointer
- 从 `/market_state/sponsorship_state`
- 改为 `/state_parse/sponsorship_state`

---

## 6.3 `flow_parse`

这一层是 `v2.1.0` 最关键的新层。

但它不能是 free-text summary，
必须是：

- **强约束的 flow reduction layer**

建议结构：

```json
"flow_parse": {
  "delta_read": {
    "futures": "",
    "spot": "",
    "relation": ""
  },
  "cvd_read": {
    "state": "",
    "alignment_vs_price": ""
  },
  "whale_read": {
    "state": "",
    "spot_vs_futures_relation": ""
  },
  "orderbook_read": {
    "pressure_side": "",
    "near_price_constraint": ""
  },
  "combined_flow_state": ""
}
```

### 推荐状态枚举方向

`delta_read.futures` / `delta_read.spot`

- `buying`
- `selling`
- `mixed`
- `unclear`

`delta_read.relation`

- `aligned`
- `divergent`
- `unclear`

`cvd_read.state`

- `rising`
- `falling`
- `flat`
- `unclear`

`cvd_read.alignment_vs_price`

- `supports`
- `lags`
- `opposes`
- `unclear`

`whale_read.state`

- `buyers`
- `sellers`
- `mixed`
- `unclear`

`whale_read.spot_vs_futures_relation`

- `aligned`
- `divergent`
- `unclear`

`orderbook_read.pressure_side`

- `buy`
- `sell`
- `mixed`
- `unclear`

`orderbook_read.near_price_constraint`

- `offers_above`
- `bids_below`
- `two_sided`
- `none`
- `unclear`

`combined_flow_state`

- `supportive`
- `constraining`
- `conflicted`
- `neutral`
- `unclear`

### 这一层真正解决的问题

它不是简单“给 flow 一个地方放”。

它要强制模型完成下面这个归纳过程：

1. delta 读出来
2. CVD 读出来
3. whale sponsorship 读出来
4. orderbook / OFI / OBI / near-price wall 读出来
5. 再判断这些 flow family 是支持、约束还是冲突

这样 high-value flow 信息才不会只出现在 prose 里。

### 这一层如何处理原始数值

这版不要求模型解释绝对阈值。

这版要求模型优先完成：

- sign / direction
- aligned vs divergent
- near-price relevance
- whether it constrains the current auction

例如：

- `obi_k_dw_close_fut = -0.489`
- `ofi_norm_spot = -1.69`

不要求模型判断“是否极端”，
但要求模型判断：

- orderbook pressure 是偏买、偏卖还是混合
- 它是否正在限制当前 participant task

### `delta_read` 的时间锚点必须固定

`delta_read` 必须明确读取：

- **该 timeframe 的 latest closed bar delta**

它不读取：

- current partial window footprint delta

如果 current partial window 与 latest closed bar materially diverges，
则应把这种分歧写入：

- `evidence_trace.conflicting_facts`

也就是说：

- `delta_read` 负责固定时间锚点的 delta truth
- partial-window divergence 负责进入证据层，说明当前 live flow 是否正在挑战该 truth

### `whale_read` 只负责当前 timeframe 的 whale truth

`whale_read` 不应承担跨 timeframe whale 分歧。

它只负责当前 timeframe 内部的 whale read，
以及：

- 该 timeframe 内 `spot` 与 `futures` whale 是否 aligned / divergent

所以字段名应明确为：

- `spot_vs_futures_relation`

而不是含糊的：

- `cross_source_relation`

跨 timeframe 的 whale 分歧，
例如：

- 15m whale selling
- 4h whale buying

不应放在 timeframe 内 `whale_read`，
而应进入：

- `cross_timeframe_parse.unresolved_factors`

这更符合：

- 每层只负责自己 timeframe 的 truth

### `combined_flow_state` 的参照系必须明确

`combined_flow_state` 不是“flow 看起来总体怎样”的泛化标签。

它必须回答：

- **当前 flow 相对于 `state_parse.control_read.side`，是在支持、约束、冲突，还是中性**

因此：

- `supportive` = flow supports the control side described in `state_parse.control_read.side`
- `constraining` = flow constrains that control side
- `conflicted` = flow families disagree materially with each other
- `neutral` = no clear support or constraint

也就是说，
`combined_flow_state` 的含义依赖于：

- `state_parse.control_read.side`

这会形成一个清晰的跨层关系：

- `state_parse` 定义当前 auction 正在偏向哪边
- `flow_parse` 判断 flow 是否支持这边

---

## 6.4 `participant_parse`

这一层只负责 participant tasks。

建议结构：

```json
"participant_parse": {
  "participant_observations": [
    {
      "participant_role": "",
      "current_task": "",
      "task_status": "",
      "constraints": [],
      "evidence": [],
      "confidence": ""
    }
  ]
}
```

建议增加数量约束：

- `participant_observations.maxItems = 3`
- `constraints.maxItems = 3`
- `evidence.maxItems = 3`

目的不是压短本身，
而是强制模型只保留：

- 当前最主要的 participant tasks
- 当前最重要的 task constraints
- 当前最关键的 supporting evidence

否则模型很容易把所有想到的限制都塞进 `constraints`，
重新滑回 narrative accumulation。

### 关键点 A：去掉 side 摘要字段

不再保留：

- `aggressive_side`
- `absorption_side`

因为它们会把 participant parse 再压回方向摘要。

### 关键点 B：核心不是“谁强”，而是“现在想做什么”

`current_task` 应表达：

- defending lower value
- forcing reentry
- blocking acceptance above resistance
- holding price inside balance
- reclaiming upper value
- stabilizing after rejection

### 关键点 C：新增 `constraints`

这是这一版最关键的 participant 强化。

因为 participant parse 如果只写：

- 这个角色正在做什么

是不够的。

还必须写：

- 当前是什么在阻止它完成

典型 `constraints` 包括：

- near-price ask wall
- negative OFI / OBI
- opposing higher-timeframe flow
- failed reclaim of upper value
- unresolved dual-value conflict

这会让 participant parse 真正变成：

- **task + status + constraints**

而不是：

- prose 风格的多空故事

---

## 6.5 `evidence_trace`

这一层继续只保留最少必要证据：

```json
"evidence_trace": {
  "supporting_facts": [],
  "conflicting_facts": [],
  "fragility_summary": ""
}
```

它只负责：

- 给 parse 提供最少 supporting / conflicting facts

它不负责：

- 放主状态定义
- 放 flow 主结论
- 放 participant task 主结论

这些都应该放在各自拥有它们的层里。

---

## 7. `cross_timeframe_parse` 的最终收敛方向

跨周期层不应该再变成第二个 summary engine。

建议保留：

- `shared_levels`
- `cross_market_parse`
- `main_tension`
- `unresolved_factors`

建议结构：

```json
"cross_timeframe_parse": {
  "shared_levels": [],
  "cross_market_parse": {
    "spot_vs_futures_gap_pct": 0,
    "flow_driver": "",
    "latest_4h_delta_relation": ""
  },
  "main_tension": "",
  "unresolved_factors": []
}
```

### 关于 `relationship_facts`

如果实际使用中证明它持续提供高价值，
可以保留一个极薄的 factual version。

如果它长期只是重复：

- inside_higher_tf_value
- inside_higher_tf_range

而没有明显提升下游理解效率，
则建议删除。

因为跨周期层真正重要的是：

- 哪些共享锚点正在起作用
- 当前最关键的跨周期张力
- 当前最关键的 unresolved conflicts

其中也包括：

- cross-timeframe whale disagreement
- lower-timeframe flow 与 higher-timeframe sponsorship 的分歧

---

## 8. 相比 v2.0.1，字段应如何调整

## 8.1 删除

- `market_state`
- 单值 `value_location`
- `participant_parse.aggressive_side`
- `participant_parse.absorption_side`
- `relationship_facts`（如果无法证明高价值）

## 8.2 替换

- `market_state` -> `state_parse`
- 单值 `value_location` -> `value_read.pvs / value_read.tpo / value_read.combined`
- `market_state.sponsorship_state` -> `state_parse.sponsorship_state`

## 8.3 新增

- `flow_parse`
- `participant_observations[].constraints`

## 8.4 保留

- `active_range`
- `range_width_vs_atr`
- `dominant_demand_zone`
- `dominant_supply_zone`
- `key_levels`
- `invalidation_level`
- `structure_lifecycle`
- `state_parse.sponsorship_state`
- `evidence_trace`
- `shared_levels`
- `main_tension`
- `unresolved_factors`

---

## 9. 推荐 prompt 方向

最终 prompt 的重点不应是继续加一堆限制句。

重点应是：

1. 定义交付对象
2. 定义每层字段职责
3. 定义 flow 是必答题
4. 定义下游验收标准

建议核心开头方向如下：

```text
Use the input to produce a structured parse of the current market.

The output is only successful if a downstream model can understand the current market without reconstructing the raw input.

For each timeframe, you must parse:
- current structure
- current state
- current flow
- current participant tasks
- unresolved conflicts that still matter now

Each layer owns one type of truth:
- structure_parse owns structural organization
- state_parse owns current auction state
- flow_parse owns current flow support and current flow conflict
- participant_parse owns current participant tasks
- evidence_trace owns the minimum supporting and conflicting facts
```

### Flow 必答要求

建议再加一段明确要求：

```text
FLOW REDUCTION IS REQUIRED

For every timeframe, explicitly reduce current flow into:
- delta_read
- cvd_read
- whale_read
- orderbook_read

A timeframe parse is incomplete if it does not state whether these flow families support, constrain, or conflict with the current auction.

Do not ignore flow simply because the raw input is long.
Reduce it into the schema.

`delta_read` describes the latest closed bar delta for that timeframe.
If the current partial window materially diverges from the latest closed bar,
note the divergence in `evidence_trace.conflicting_facts`.

`whale_read` describes whale behavior only for the current timeframe.
If whale direction diverges across timeframes, preserve that divergence in `cross_timeframe_parse.unresolved_factors`.

`combined_flow_state` is relative to `state_parse.control_read.side`:
- `supportive` means flow supports that control side
- `constraining` means flow constrains that control side
- `conflicted` means flow families materially disagree with each other
- `neutral` means no clear support or constraint
```

### Flow 字段语义说明也必须写进 prompt

仅仅告诉模型“参考 live flow”是不够的。

对于 5000+ 行输入，
prompt 还必须明确告诉模型：

- 哪类字段属于 `delta_read`
- 哪类字段属于 `cvd_read`
- 哪类字段属于 `whale_read`
- 哪类字段属于 `orderbook_read`

建议在 `HOW TO READ THE INPUT` 后补一段类似：

```text
FLOW FIELD SEMANTICS

Interpret flow families by market meaning, not by raw feature-name complexity.

- `delta_read` is derived from the timeframe's latest closed bar delta split between futures and spot.
- Current partial-window footprint delta does not replace `delta_read`; it only matters if it materially diverges, and then it should appear in `evidence_trace.conflicting_facts`.
- `cvd_read` is derived from the timeframe CVD slope and whether it supports, lags, or opposes price.
- `whale_read` is derived from whale delta / whale notional for the current timeframe, and from whether spot and futures whale activity align or diverge on that timeframe.
- `orderbook_read` is derived from orderbook and near-price execution constraints such as OBI, OFI, depth imbalance, stacked imbalance, liquidity walls, and other near-price resting pressure.

Do not ignore a flow family simply because its field names are feature-engineered or verbose.
Reduce each family into the schema.
```

这一段的作用不是给模型讲实现细节，
而是明确：

- flow family 的语义归属
- 这些 feature 名称不是“可忽略中间变量”
- 模型必须把它们归纳进对应字段

### 冲突保留要求

建议再加一段：

```text
PRESERVE SIMULTANEOUS TRUTHS

If the input contains simultaneous truths, preserve them as simultaneous truths.

Do not compress:
- dual value-model conflict
- price/flow divergence
- whale/orderbook disagreement

into a cleaner single state unless the schema explicitly requires that compression.
```

这不是在修 corner case，
而是在明确：

- 冲突本身就是市场真相

---

## 10. 推荐自检

对于 JSON-only 输出模型，
自检不应过重。

考虑到 `stage1 scan` 当前已经存在明显延迟，
最终 prompt 里只保留核心自检。

### 10.1 核心自检

这 7 条必须保留，
因为它们直接决定输出是否完成了最关键的解析义务。

1. 我是否分别完成了 structure、state、flow、participant task 的解析？
2. 我是否把 `delta_read` 明确锚定为该 timeframe 的 latest closed bar delta？
3. 我是否明确完成了 `delta / CVD / whale / orderbook` 四类 flow reduction？
4. 我是否明确写出了这些 flow family 是支持、约束、冲突还是中性？
5. 我是否写出了这些 tasks 的 `task_status` 和 `constraints`？
6. 我是否把 `sponsorship_state` 作为独立状态表达，而没有把它混进 `combined_flow_state` 或 participant prose？
7. 下游模型是否可以不重建 raw input 就读懂当前市场？

---

## 11. 最终判断

如果这是最后一次修改，
我不建议继续在 `v2.0.1` 上补更多零散规则。

我建议把系统直接收敛到下面这套终态理解：

- **market = structure + state + flow**
- **participant = current task + task status + constraints**

但这次比前一版更进一步：

- 不只是“给 flow 一个位置”
- 而是**把 flow 变成必答、强约束、可消费的结构化归纳层**

真正的根修不是：

- 再加几句更谨慎的话
- 再补几个 reversal / recency / fragility rule

而是：

- 让高价值真相有正式位置
- 让冲突信息可以并存表达
- 让 flow 不再被长输入自动降级
- 让 participant parse 围绕 current task + constraints
- 让下游可以直接消费，而不必回看 raw input

如果 `v2.1.0` 按这版落地，
它最大的价值不是“模型更听话”，
而是：

- **模型终于被要求稳定地完成当前市场的结构化归纳**
