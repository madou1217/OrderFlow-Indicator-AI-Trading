# Scan Prompt & Schema 修改方案 v2.0.1

v2.0.1 的目标不是继续给 `v2.0.0` 叠规则。

这版要做的是把 Stage 1 的需求再纯化一次：

- 更符合第一性原理
- 更像清晰的需求定义，而不是 corner case 修补
- 更直接服务 Stage 2 对市场与参与者的阅读

所以 v2.0.1 的核心不是“加更多限制”，而是：

- **把当前市场状态优先于旧故事写成明确需求**
- **把参与者当前目标必须匹配当前市场任务写成明确需求**
- **把输出契约收敛成对 Stage 2 友好的清晰结构**
- **删除重复字段，减少层级混淆和重复表达**

---

## 第一部分：第一性原理下，Stage 1 到底要交付什么

Stage 1 不是：

- 一个提前帮 Stage 2 选边的交易摘要器
- 一个把市场压成单一方向 verdict 的压缩器
- 一个把历史路径讲成完整故事的叙事器

Stage 1 应该是：

- **市场解析器**
- **参与者目的与行为解析器**

也就是说，Stage 1 的最低交付标准只有 3 件事：

1. 把当前市场结构解析清楚
2. 把当前主要参与者的目的和行为解析清楚
3. 让 Stage 2 不必回看原始输入，也能直接读懂当前市场

所以从第一性原理看，Stage 1 必须回答的不是：

- 谁最终赢了
- 下一步大概率先去哪
- 哪边更适合交易表达

而是：

- 当前什么结构仍在组织价格
- 当前什么结构已经不再组织价格
- 当前主要参与者分别在试图让市场做什么
- 这些目标当前是在生效、被阻挡、正在失败，还是仍未解决
- 当前跨周期的主要张力和未解决因素是什么

---

## 第二部分：v2.0.0 暴露出的根因，不是 corner case

`v2.0.0` 的方向已经是对的：

- 先解析市场
- 再解析参与者目的
- 再做跨周期整合

但实际样本暴露出的几个问题，不应该被理解成几个独立 corner case。

它们的共同根因其实只有 3 个：

### 1. “当前 active read 到底由什么定义”还没有被写清楚

当市场已经从上一拍的 continuation / acceptance / defense 切换到新的 active-now state，
模型还可能沿用上一拍的 read。

根因不是“某个 reversal case 没覆盖”，
而是：

- **需求里还没有明确规定：active read 必须由当前仍在组织价格的结构、状态和参与者任务来定义**

### 2. `current_objective` 的定义还不够硬

`current_objective` 现在容易被模型写成：

- 上一拍的延续预期

而不是：

- 当前参与者此刻正在试图完成的市场任务

根因不是 wording 小问题，
而是：

- **需求里还没有明确规定：objective 必须表达当前市场任务，而不是沿用前一拍叙述**

### 3. 输出契约还没有完全按 Stage 2 阅读顺序收敛

只要输出契约里还混有：

- 重复摘要字段
- parser 自我说明字段
- 跨层重复 lifecycle

模型就自然会重复写、混淆层级职责、让 parse 和 summary 重新缠在一起。

根因不是模型“话太多”，
而是：

- **需求里还没有清晰地区分核心解析字段和辅助阅读字段**

---

## 第三部分：v2.0.1 的设计原则

### 1. Active Read Must Be Defined by the Current Organizing State

Stage 1 的 active read 必须由下面这些“当前仍在组织价格”的东西共同定义：

- 当前 price location
- 当前 value state
- 当前 active structure
- 当前 flow condition
- 当前 participant task

只要这些当前组织因素已经变化，
active read 就必须跟着变化。

上一拍发生过什么，
可以保留为：

- path explanation
- structure lifecycle
- background context

但不能替代当前 organizing state 本身。

### 2. Current Objective Must Match the Current Market Task

`current_objective` 必须回答的是：

- 当前 auction state 下，这个参与者现在想让市场做什么

它不是：

- 上一拍 continuation 的延续口号

所以当市场状态改变后，
`current_objective` 必须跟着改变。

这不是在“重置一句话”，
而是在重新定义：

- 当前这个参与者此刻到底在试图让市场做什么

### 3. Evidence Trace Supports the Parse, It Does Not Replace It

证据层的职责是：

- 用最少的 supporting / conflicting facts 说明 parse 为什么这么写

证据层不应该：

- 再写一份平行 narrative
- 再把 market_state 重复一遍
- 再把 participant objective 重复一遍
- 承担“模型自我解释”的职责

### 4. Keep Only Fields That Add Parse Value

如果一个字段只是：

- 对更底层字段再做一次压缩摘要
- 看起来完整，但不增加新的解析信息
- 下游也不直接需要

那么它就不该保留在核心 schema 里。

### 5. Stage 2 Readability Is a First-Class Requirement

输出结构不只是为了“字段齐全”。

它必须按 Stage 2 的阅读顺序组织：

1. 先看到市场结构
2. 再看到当前状态
3. 再看到参与者目的与行为
4. 再看到最少必要证据
5. 最后看到跨周期张力和未解决因素

### 6. One Fact, One Owning Layer

Stage 1 的每一层都应该只负责一类信息：

- `structure_parse` 负责结构
- `market_state` 负责状态
- `participant_parse` 负责参与者任务
- `evidence_trace` 负责最少必要证据
- `cross_timeframe_parse` 负责跨周期整合

同一事实不应该在多个层里反复出现。

这不是为了“更短”本身，
而是为了：

- 保持需求边界清晰
- 减少层级混淆
- 让 Stage 2 能快速知道每层该读什么

---

## 第四部分：Prompt 侧新增 3 条需求化原则

下面 3 条建议补进：

- [medium_large_opportunity.txt](/data/systems/llm/src/llm/prompt/scan/medium_large_opportunity.txt)
- [big_opportunity.txt](/data/systems/llm/src/llm/prompt/scan/big_opportunity.txt)

### A. 加到 `ACTIVE NOW VS BACKGROUND` 段后

新增小节：`ACTIVE READ DEFINITION`

建议文本：

```text
ACTIVE READ DEFINITION

Define the active read from what is organizing the auction now.

The active read must be grounded in the current combination of:
- current price location
- current value state
- current active structure
- current flow condition
- current participant task

If those active organizing factors have changed, the parse must be rewritten from the new current state.

Prior continuation, acceptance, or defense is still useful only when it remains part of the current organizing state.
Otherwise it belongs in structure lifecycle or background explanation.

Example:
- a prior above-value continuation may become a reentry-and-stabilization state once current structure and flow no longer support continuation
```

这条的重点不是在修一个 reversal 样例，
而是在定义：

- active read 到底由什么构成
- 旧结构什么时候仍算 active
- 旧结构什么时候只该留在 lifecycle 或 background 里

### B. 加到 `PARTICIPANT PURPOSE` 段后

新增小节：`CURRENT OBJECTIVE MUST MATCH CURRENT TASK`

建议文本：

```text
CURRENT OBJECTIVE MUST MATCH CURRENT TASK

`current_objective` must describe the participant's current market task now.

It should answer:
- what this participant is trying to make the market do now

If the latest closed bar or the active-now state shows that the participant is no longer working on the prior task, rewrite `current_objective` to the new current task.

Examples of current tasks:
- stabilizing after reentry
- defending lower value
- forcing deeper reentry
- holding price inside the current bracket
- reclaiming upper value
```

这条的目标是让 `participant_parse` 真正变成“当前任务解析”，
而不是“方向意图复述”。

### C. 加到 `OUTPUT STYLE` 段后

新增小节：`FIELD RESPONSIBILITY DISCIPLINE`

建议文本：

```text
FIELD RESPONSIBILITY DISCIPLINE

Each field must do one job only.

- `key_levels.reason` explains the structural role of the level.
- zone `reason` explains why that zone exists now.
- `participant_observations` explain participant behavior and current task.
- `evidence_trace` provides only the minimum supporting and conflicting facts for the parse.
- `cross_timeframe_parse` integrates timeframe parses; it does not restate them.

Do not repeat the same fact across layers unless that fact changes meaning in the new layer.

Prefer the shortest expression that fully satisfies the field's job.
```

---

## 第五部分：v2.0.1 的输出结构，应该如何服务 Stage 2

为了让 Stage 2 易读，
每个 timeframe 的输出应该明确分成 4 层：

### 1. `structure_parse`

它回答：

- 当前什么结构在组织价格
- 哪些 levels 现在最重要
- 哪些结构 active / failing / invalidated

### 2. `market_state`

它回答：

- 当前市场处于什么状态

也就是：

- `control_side`
- `control_clarity`
- `value_location`
- `range_state`
- `sponsorship_state`

### 3. `participant_parse`

它回答：

- 当前主要参与者在试图做什么
- 这些任务现在是否 working / blocked / failing / unresolved

也就是：

- 参与者目的与行为解析

### 4. `evidence_trace`

它回答：

- 为什么这份 parse 应该这样写

它只保留最少必要的：

- `supporting_facts`
- `conflicting_facts`
- `fragility_summary`

它不再承担：

- 模型自我解释
- 第二份 narrative

这 4 层的阅读顺序，正好对应 Stage 2 的需要：

1. 先看结构
2. 再看状态
3. 再看参与者
4. 最后看证据

这里的关键不是“压短”。

而是：

- 每一层只回答它自己的问题
- 每一层不替别的层重复回答

---

## 第六部分：v2.0.1 保留的高价值字段

### 1. Core Parse Fields

这些是 Stage 1 不应删除的核心字段：

- `active_range`
- `range_width_vs_atr`
- `dominant_demand_zone`
- `dominant_supply_zone`
- `key_levels`
- `invalidation_level`
- `structure_lifecycle`
- `market_state`
- `participant_observations`
- `evidence_trace`
- `main_tension`
- `unresolved_factors`

### 2. Stage 2 Navigation Fields

下面这些字段不是 parse 本体，
但它们以较低冗余成本提升 Stage 2 的快速阅读，
所以仍然值得保留：

- `relationship_facts`
- `cross_market_parse`
- `shared_structure.key_shared_levels`

其中：

- `relationship_facts` 提供跨周期位置关系
- `cross_market_parse` 提供最简跨市场确认框架
- `key_shared_levels` 提供跨周期共享锚点

---

## 第七部分：v2.0.1 的最小瘦身清单

下面这批字段属于“低价值重复项”，优先删除。

### 1. 删除 `participant_parse.aggressive_side`

原因：

- 它是对 `participant_observations` 的再次压缩
- 容易逼模型提前做 side verdict
- 它不是参与者解析本体

### 2. 删除 `participant_parse.absorption_side`

原因：

- 与 `passive_liquidity` / `observed_behavior` 高度重复
- 仍然属于摘要型字段，不是解析型字段

### 3. 将 `validation` 重定义为 `evidence_trace`

建议：

- 正式把 `validation` 这个层改名为 `evidence_trace`

原因：

- 这个层表达的其实是证据轨迹
- 不是“验证模型自己有没有讲通”
- 这个名字更符合第一性原理，也更利于 Stage 2 阅读

如果实现上要分步迁移，
可以暂时保留旧 key 名，
但语义必须按 `evidence_trace` 执行。

### 4. 删除 `validation.read_basis`

原因：

- 它是 parser 自我说明
- 不是市场结构本身
- 它不增加 Stage 2 可读性

### 5. 删除 `validation.recent_closed_bars_align_with_read`

原因：

- 低信息密度布尔位
- 可由 `supporting_facts / conflicting_facts` 隐式表达

### 6. 删除 `validation.cvd_slope_aligns_with_read`

原因：

- 和 `conflicting_facts` 高度重复
- 是摘要型布尔位，不是解析型字段

### 7. 删除 `validation.current_partial_bar_aligns_with_read`

原因：

- 仍然是 parser self-report
- 不属于市场本体

### 8. 删除 `cross_timeframe_parse.shared_structure.shared_structure_lifecycle`

原因：

- 容易重复各 timeframe 内的 `structure_lifecycle`
- 诱导模型再造一套跨周期结构命名
- 跨周期层保留 `key_shared_levels` 就足够

### 9. 删除 `spot_premium_state`，保留 `spot_vs_futures_gap_pct`

建议：

- **保留 `spot_vs_futures_gap_pct`**
- **删除 `spot_premium_state`**
- **由下游从 `spot_vs_futures_gap_pct` 推导 premium state**

原因：

- 数值比标签更原始
- 标签可以由下游按阈值推导
- 可以减少“同一信息的标签 + 数值双写”

### 10. 保留 `range_width_vs_atr`

建议：

- **保留 `range_width_vs_atr`，不纳入删减**

原因：

- 它是低信息密度标签
- 但它仍然提供了一个有价值的归一化视角
- 它能用很低的 schema 成本补充 `active_range` 的相对宽度信息
- 如果删除它，某些“绝对价格区间看得见，但相对波动尺度看不见”的信息会真实丢失

所以在 `v2.0.1` 里，
更合理的做法是：

- 删除其他重复字段
- 保留 `range_width_vs_atr`

## 第八部分：结合当前实现层的现实约束

当前 Stage 2 finalize 直接读取的，主要是：

- `market_state`
- `cross_market_parse`
- `main_tension`
- `unresolved_factors`

见 [core.rs](/data/systems/llm/src/llm/filter/core.rs#L102)。

这说明一个现实问题：

- `v2.0.0` 里很多更细的字段，虽然理论上有价值
- 但当前代码并没有直接消费它们

所以 v2.0.1 的思路不是继续加字段，
而是：

- 保留真正增加 parse value 的字段
- 删除主要增加重复表达、但不明显增加可读性的字段

这也意味着：

- 如果执行 `spot_premium_state` 的删减，并改为由下游从 `spot_vs_futures_gap_pct` 推导
- 或把 `validation` 重命名为 `evidence_trace`

就需要同步修改 finalize 的读取逻辑。

---

## 第九部分：v2.0.1 推荐输出契约

### Top level

- `schema_version`
- `meta`
- `timeframes`
- `cross_timeframe_parse`

### Each timeframe

- `structure_parse`
- `market_state`
- `participant_parse`
- `evidence_trace`

### `structure_parse`

- `active_range`
- `range_width_vs_atr`
- `dominant_demand_zone`
- `dominant_supply_zone`
- `key_levels`
- `invalidation_level`
- `structure_lifecycle`

### `market_state`

- `control_side`
- `control_clarity`
- `value_location`
- `range_state`
- `sponsorship_state`

### `participant_parse`

- `participant_observations`

Each observation keeps:

- `participant_role`
- `observed_behavior`
- `current_objective`
- `objective_status`
- `evidence`
- `confidence`

### `evidence_trace`

- `supporting_facts`
- `conflicting_facts`
- `fragility_summary`

### `cross_timeframe_parse`

- `relationship_facts`
- `cross_market_parse`
- `shared_structure`
- `main_tension`
- `unresolved_factors`

### `cross_market_parse`

- `spot_vs_futures_gap_pct`
- `flow_driver`
- `latest_4h_delta_relation`

### `shared_structure`

- `key_shared_levels`

这个契约的阅读顺序必须保持不变：

1. 结构
2. 状态
3. 参与者
4. 证据
5. 跨周期整合

这样 Stage 2 才能顺着 parse 读，而不是重新拼市场。

同样重要的是：

- 每一层都必须只承担自己的职责
- 相同事实不应跨层重复出现

---

## 第十部分：v2.0.1 的自检补充

除了 `v2.0.0` 原有自检外，建议新增 3 条：

```text
9. Is the active read defined by the current organizing state, rather than by a prior narrative that is no longer organizing price?
10. Does each `current_objective` describe the participant's current market task now, rather than a stale continuation goal?
11. Does each field only perform its own job, without repeating the same fact across multiple layers?
```

---

## 第十一部分：结论

`v2.0.1` 的本质不是“继续修几个表现问题”。

它更准确的定位是：

- **把 Stage 1 的需求再纯化一次**
- **把状态变更和目标重写写成刚性原则**
- **把输出契约收敛成更适合 Stage 2 阅读的结构**
- **把重复字段减掉，减少层级混淆和重复表达**
- **在减法里保留 `range_width_vs_atr` 这类低成本但仍有解析价值的字段**

一句话总结：

- **v2.0.1 要让 Stage 1 更像一个清晰的市场解析对象**
- **而不是一个越来越复杂的 prompt 规则集合**
