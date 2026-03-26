# Scan Prompt & Schema 修改方案 v4.0.0

## 0. 版本定位

`v4.0.0` 不是在 `v3.4.0` 上继续微调 wording。

这版要解决的是一个更根本的问题：

- **当前 `stage1 scan` 并没有真正完成 `4h-1d` 市场分析的职责。**

它更像是在输出：

- 当前最近、正在组织价格的局部区域

而不是输出：

- **能支撑 `stage2` 做 `4h-1d` 交易决策的完整市场地图**

本版的核心改动是：

- 保留 `v3.3.0` 的 deterministic 预计算
- 保留 `v3.4.0` 的 `15m execution_context`
- **重做 `4h / 1d structure_parse`**

这里要明确一件事：

- **`v4.0.0` 不建立在“去掉 15m 完整解析就足以修复 `4h / 1d` 价格带过窄”这个假设上。**

去掉 `15m` full scan 只会：

- 减轻一部分 near-price anchoring bias

但它本身并不能自动解决：

- `4h / 1d` 价格带过窄
- `4h / 1d` 结构层数不足

真正的根因修复是：

- **把 `4h / 1d structure_parse` 从“单层 dominant zone 摘要”重做成“可交易的结构层级地图”。**

目标不是再优化“当前组织区”的表达，而是让 `stage1` 真正输出：

- `4h / 1d` 的关键价格带层级
- `4h / 1d` 的当前市场结构
- `4h / 1d` 的主要参与者正在守哪里、打哪里、盯哪里

---

## 1. 最高原则

本版完全服从下面这个最高原则：

- 不降模型输出质量是第一需求
- 不为了减少模型思考时长而降低输出质量
- 不用考虑工作量

因此，本版不是为了“简化 schema 让模型更快”。

本版是为了：

- **让 `stage1` 终于输出 `stage2` 真正需要的市场地图**

只有在这个前提下，任何输入/输出优化才有意义。

---

## 2. 重新明确 Stage1 / Stage2 的职责

## 2.1 Stage1 的职责

`stage1 scan` 的职责是：

- 解析市场
- 解析主要参与者的目的和行为
- 为 `stage2` 提供一张足以支撑 `4h-1d` 交易决策的市场地图

关键点：

- `stage1` 不是只回答“当前最近哪里在起作用”
- `stage1` 也不是只回答“当前 market state 是什么”
- `stage1` 必须回答：
  - `4h / 1d` 当前市场由哪些关键结构组成
  - 这些结构在价格上分别在哪里
  - 从当前价格往上、往下，下一层重要区域是谁
  - 哪些参与者在守这些区域、打这些区域、试图把价格带到哪里

一句话说：

- **`stage1` 负责市场地图**

## 2.2 Stage2 的职责

`stage2` 的职责是：

- 读取 `stage1` 的市场地图
- 结合当前 execution-time 数据
- 给出：
  - 方向
  - `entry`
  - `stop_loss`
  - `take_profit`
  - `leverage`

一句话说：

- **`stage2` 负责路径和执行**

因此：

- `stage1` 不负责直接给出交易参数
- `stage2` 也不应该被迫从 raw 输入重新搭地图

---

## 3. 第一性原理下，当前设计哪里错了

当前 `stage1 scan` 的错误，不是“价格算错了”这么简单。

根本问题是：

- **它把 `4h / 1d` 市场分析压缩成了“当前最近主组织区摘要”**

这会带来 4 个直接后果。

## 3.1 只输出最近一层，不输出结构层级

当前 `structure_parse` 的中心字段是：

- `dominant_demand_zone`
- `dominant_supply_zone`

这两个字段天然只能表达一层。

于是模型会输出：

- 当前最近最重要的一层 demand
- 当前最近最重要的一层 supply

但输出不了：

- 这一层下面的下一层支撑是谁
- 这一层上面的下一层压力是谁
- 更外层 thesis invalidation 在哪
- 更外层 thesis objective 在哪

这不是“表达不够好”，而是**结构不够表达**。

## 3.5 `15m` anchoring 只是放大器，不是根因

移除 `15m` 完整解析是对的，因为它能减轻一部分近价格锚定。

但必须明确：

- 即使 `15m` 不再做 full scan
- 只要 `4h / 1d structure_parse` 仍然只有单层 `dominant_demand_zone / dominant_supply_zone`

模型依然会继续把高时间框架压成：

- 最近一层
- 当前一层
- 最显眼的一层

所以：

- **`v4.0.0` 的主要修复不是“拿掉 15m”**
- **而是“把 `4h / 1d` 的主输出改成 ladder-first 的结构地图”**

## 3.2 “active now” 被错误实现成“只保留当前最近一层”

从第一性原理看，`active now` 的真正含义应该是：

- 当前仍然对 auction 有效、仍然影响 `4h-1d` 交易路径的结构

它不等于：

- 只保留离当前价格最近的一层

更远的 `4h / 1d` 结构，只要仍然对：

- 方向判断
- `tp`
- `sl`
- `entry`

有决策意义，就仍然是 active 的一部分。

## 3.3 失效区域和角色翻转区域没有被正确处理

当前设计把 `structure_lifecycle` 做成了：

- `active`
- `failing`
- `invalidated`

这在研究层面有意义，但对 `stage2` 的直接交易决策帮助有限。

`stage2` 真正需要的不是：

- 一份历史生命周期记录

而是：

- 现在还能用来做决策的价格带地图

因此：

- **完全失效且不再影响当前 auction 的区域，不应留在主输出里**
- **失效后已经翻转成当前 supply / demand 的区域，应保留，但要以“当前角色”表达**

例如：

- 原来的 reclaim band 失败后，如果现在成了 overhead supply  
  那它不应该再以“旧 reclaim attempt”身份保留，  
  而应该以“当前压力带”身份保留。

## 3.4 `stage2` 会天然被近端结构绑架

如果 `stage1` 只给最近一层，`stage2` 虽然理论上还能看 raw 输入，
但在实践里会天然被：

- 最近的 support
- 最近的 resistance
- 最近的 ask wall / bid wall

绑住。

这会导致：

- `entry`、`tp`、`sl` 都偏近端
- `4h / 1d` 交易被缩成一笔“当前附近的结构表达”

这不是 `stage2` 的错，而是 `stage1` 地图不完整。

---

## 4. v4.0.0 的核心决策

## 4.1 保留什么

保留：

- `v3.3.0` 的 deterministic 预计算
- `v3.4.0` 的 `execution_context_15m`
- `4h / 1d` 的 full market parse

## 4.2 删除什么

从 `4h / 1d structure_parse` 中删除：

- `dominant_demand_zone`
- `dominant_supply_zone`
- `invalidation_level`
- `structure_lifecycle`

原因不是这些字段完全没价值，
而是它们会把市场压成：

- 单层 dominant zone
- 单个 invalidation scalar
- 一组 lifecycle commentary

这三者都不适合直接服务 `stage2` 交易决策。

## 4.3 用什么替代

用下面这组字段替代：

- `support_ladder`
- `resistance_ladder`
- `current_structure_zone`
- `key_levels`

一句话说：

- **从“单层 dominant zone”切换为“可交易的结构层级地图”**
- **其中 `support_ladder / resistance_ladder` 是主输出，`current_structure_zone` 只是当前位置标签**

---

## 5. `stage1` 到底要不要给价格带

答案是：

- **必须给**

而且这是 `stage1` 的核心职责之一。

因为 `stage2` 最终要输出：

- `entry`
- `stop_loss`
- `take_profit`

如果 `stage1` 不给出 `4h / 1d` 关键价格带，
那 `stage2` 就必须自己从原始结构候选里重建市场地图。

那说明：

- `stage1` 没完成自己的职责

---

## 6. 如果给价格带，要不要给“全量”

这个问题要分清两种“全量”。

## 6.1 不该给的“全量”

不该给：

- 所有 raw candidate
- 所有历史结构
- 所有近端节点
- 所有已失效区域

这不是市场解析，只是把候选池倒给 `stage2`。

## 6.2 应该给的“全量”

应该给的是：

- **对 `4h-1d` 交易决策完整的结构层级**

也就是：

- 从当前价格往上，直到 thesis 目标区为止的关键阻力层级
- 从当前价格往下，直到 thesis 失效区为止的关键支撑层级

这不是 raw 全量。

这是：

- **operative full ladder**

一句话说：

- 不是给所有候选
- 而是给足以支撑交易决策的完整层级

这里要再强调一次：

- `stage1` 不能停在“当前最近正在组织价格的一层”
- 必须把直到 thesis objective / thesis invalidation 为止、仍然影响交易决策的层级都交给 `stage2`

---

## 7. `4h / 1d structure_parse` 应该改成什么

## 7.1 新的职责

新的 `structure_parse` 不再回答：

- “当前最 dominant 的 demand / supply 是哪一层”

而要回答：

- 上方从近到远的重要阻力层级是谁
- 下方从近到远的重要支撑层级是谁
- 当前价格正处于哪一层主结构里
- 哪些层级是当前 thesis 的真正决策边界

## 7.2 推荐 schema

```json
"structure_parse": {
  "active_range": {
    "low": 2143.52,
    "high": 2164.48
  },
  "range_width_vs_atr": "narrow|normal|wide",
  "support_ladder": [
    {
      "zone_id": "1d_support_1",
      "low": 2142.15,
      "high": 2147.96,
      "importance": "immediate|next|outer",
      "structural_role": "value_edge|imbalance_support|demand_flip|swing_support|balance_low|liquidity_cluster",
      "status": "active|testing|failing|flipped",
      "reason": "string"
    }
  ],
  "resistance_ladder": [
    {
      "zone_id": "1d_resistance_1",
      "low": 2160.06,
      "high": 2164.48,
      "importance": "immediate|next|outer",
      "structural_role": "value_edge|reclaim_band|supply_flip|swing_resistance|balance_high|liquidity_cluster",
      "status": "active|testing|failing|flipped",
      "reason": "string"
    }
  ],
  "current_structure_zone": {
    "zone_id": "1d_current_balance_2143_2164",
    "low": 2143.52,
    "high": 2164.48,
    "zone_role": "balance|reclaim_band|rejection_band|value_area|imbalance_bracket|swing_boundary",
    "status": "active|testing|failing|flipped",
    "reason": "string"
  },
  "key_levels": [
    {
      "price": 2151.49,
      "type": "pivot|value_edge|imbalance_edge|liquidity_wall|swing_level",
      "reason": "string"
    }
  ]
}
```

## 7.3 为什么这样更对

这组字段能明确分开：

- 下方层级
- 上方层级
- 当前所在主结构区

这样 `stage2` 才能区分：

- 下一层支撑
- 更远层目标
- 更外层失效
- 当前局部表达

## 7.4 `current_structure_zone` 的真正定位

`current_structure_zone` 不是新的中心字段。

它的职责只有一个：

- 告诉 `stage2` 当前价格现在位于哪一层主结构里

它不负责替代：

- 下方支撑层级
- 上方阻力层级
- thesis invalidation / objective 的外层边界

所以在 `v4.0.0` 里：

- **`support_ladder / resistance_ladder` 才是 `structure_parse` 的主输出**
- **`current_structure_zone` 只是辅助定位字段**

如果没有 ladders，只有 `current_structure_zone`，
那系统就会退化回：

- “当前组织区摘要”

这正是本版要避免的。

---

## 8. `support_ladder / resistance_ladder` 的原则

## 8.1 顺序

两条 ladder 都必须按：

- **从当前价格最近的一层，到更外层的一层**

排序。

## 8.2 层级要求

如果数据里存在多层有效结构，就不能只停在最近一层。

也就是说：

- 不能因为当前最近一层最显眼，就省略下一层
- 不能因为当前价格还没打到外层，就省略外层 thesis 目标/失效区

## 8.3 数量要求

推荐：

- `support_ladder`: 默认 3 层，必要时允许到 4 层
- `resistance_ladder`: 默认 3 层，必要时允许到 4 层

这 3 层分别对应：

- `immediate`
- `next`
- `outer`

如果数据里不存在更外层有效结构，可以少于 3。

但如果存在，就不能偷懒只写 1 层。

这里的关键不是固定数量本身，
而是：

- **不能因为当前 schema 的近端偏置或低 item 限制，省略仍然影响 thesis 的下一层/外层结构**

因此：

- `key_levels` 可以继续是补充字段
- 但 `support_ladder / resistance_ladder` 必须承担完整层级地图的职责
- 不能把结构深度继续压回 `key_levels` 的 item 限制里

## 8.4 已失效区域如何处理

规则很明确：

- 完全失效且不再影响当前 auction 的区域：不写入 ladder
- 失效后已翻转成当前作用区的区域：保留，但用当前角色写入 ladder

所以：

- “旧角色”不保留
- “当前角色”保留

---

## 9. 参与者解析也必须绑定到价格带

如果 `stage1` 的任务之一是解析主要参与者的目的和行为，
那 `participant_parse` 不能只写抽象任务。

它必须和结构地图连起来。

## 9.1 当前问题

现在的 `participant_parse` 可以写出：

- reclaim upper value
- defend lower balance
- block acceptance above reclaim band

但它没有把这些任务稳定地绑到结构层级上。

这会让 `stage2` 很难直接把 participant task 转成：

- `entry`
- `stop_loss`
- `take_profit`

## 9.2 推荐改法

```json
"participant_observations": [
  {
    "participant_role": "higher_timeframe_sponsorship|passive_liquidity|responsive_crowd|initiative_flow",
    "current_task": "string",
    "task_status": "accepted|blocked|failing|unresolved",
    "defended_zone_ids": ["1d_support_1"],
    "attacked_zone_ids": ["1d_resistance_1"],
    "next_target_zone_ids": ["1d_support_2"],
    "constraints": ["string"],
    "evidence": ["string"],
    "confidence": "low|medium|high"
  }
]
```

这样 `participant_parse` 就会回答：

- 谁在守哪一层
- 谁在打哪一层
- 如果打穿/守住，下一层会是谁

这比单纯写行为 prose 更适合 `stage2` 直接消费。

---

## 10. `state_parse` 和 `flow_parse` 仍然保留

本版不推翻：

- `state_parse`
- `flow_parse`
- `participant_parse`
- `evidence_trace`

因为它们仍然是必要层。

本版只重做：

- `structure_parse`

因为当前最根本的问题不在 flow，而在：

- `4h / 1d` 的结构地图没有被完整表达出来

---

## 11. `15m` 继续保持 execution layer

本版不回退 `v3.4.0`。

也就是说：

- `15m` 仍然不是 thesis timeframe
- `15m` 仍然只输出 `execution_context_15m`

原因很简单：

- 当前真正缺的不是 15m
- 而是 `4h / 1d` 的层级地图

所以：

- `15m execution_context`
- `cross_timeframe_parse.execution_alignment_15m`

继续保留。

但要明确：

- `15m` 降级成 execution layer 只是为了减轻近价格锚定
- **它不是 `v4.0.0` 解决 `4h / 1d` 结构过窄与层数不足的主要手段**

真正的主要手段仍然是：

- `4h / 1d` 改成 ladder-first 的结构地图

---

## 12. 对 prompt 的要求

## 12.1 不再让模型输出单层 dominant zone

prompt 不应再要求：

- `dominant_demand_zone`
- `dominant_supply_zone`

而应要求：

- `support_ladder`
- `resistance_ladder`
- `current_structure_zone`

## 12.2 新的 prompt 要求

应明确写：

```text
For 4h and 1d, do not stop at the nearest active zone.
Return the operative structural ladder needed for a 4h to 1d trade decision.

For each thesis timeframe:
- identify the immediate support and resistance layers
- identify the next support and resistance layers when they still matter for the thesis
- identify the outer support or resistance layer when it defines the thesis objective or invalidation
- identify the current structure zone only as the current location label, not as a substitute for the ladder

Do not include invalidated zones that no longer affect the current auction.
If a formerly invalidated zone now acts as live supply or demand, include it by its current role.
```

## 12.3 参与者 prompt 要求

应明确写：

```text
Anchor participant tasks to the structural ladders.
State which zones are being defended, attacked, or targeted next.
Do not describe participant behavior without linking it to the current price-band map.
```

---

## 13. 对 schema 的直接影响

## 13.1 删除

从 `4h / 1d structure_parse` 删除：

- `dominant_demand_zone`
- `dominant_supply_zone`
- `invalidation_level`
- `structure_lifecycle`

## 13.2 新增

新增：

- `support_ladder`
- `resistance_ladder`
- `current_structure_zone`

并把 `participant_parse` 扩成 zone-linked 版本。

## 13.3 保留

保留：

- `active_range`
- `range_width_vs_atr`
- `key_levels`
- `state_parse`
- `flow_parse`
- `evidence_trace`
- `execution_context_15m`
- `cross_timeframe_parse`

---

## 14. 对 Stage2 的意义

按这个设计，`stage2` 将不再只看到：

- 当前最近的一层 zone

而会看到：

- 当前结构层
- 下一层支撑/压力
- 外层 thesis 失效/目标层
- 哪个 participant 在守哪一层、打哪一层

这会直接改善：

- `direction` 的判断
- `entry` 选择
- `stop_loss` 放置
- `take_profit` 选择
- `leverage` 的风险定级

一句话说：

- `stage2` 会第一次真正拿到一张可交易的 `4h / 1d` 地图

---

## 15. 风险与边界

## 15.1 本版不做的事

本版不做：

- 不进一步压缩 `4h / 1d` 输入
- 不删除 `raw_overflow`
- 不减少 `4h / 1d` 的结构候选来源
- 不回退 `15m execution_context`

因为本版的第一目标不是提速，而是：

- **先把 `stage1` 的 4h/1d 市场分析职责做对**

## 15.2 本版最大的收益

本版最大的收益不是 token 节省。

而是：

- `stage1` 不再输出一张“当前组织区摘要”
- 而是输出一张“可供交易的结构层级地图”

这才符合：

- `stage1 scan = 解析市场 + 解析主要参与者目的和行为`

---

## 16. 最终结论

按第一性原理重做之后，结论非常明确：

- `stage1` 必须给出价格带
- 但给的不是 raw 全量候选
- 而是对 `4h-1d` 交易决策完整的结构层级

因此：

- 当前 `v3.x` 的单层 `dominant zone` 设计不够
- `4h / 1d structure_parse` 必须升级成：
  - `current_structure_zone`
  - `support_ladder`
  - `resistance_ladder`
  - zone-linked `participant_parse`

这版的核心不是让模型更快，
而是让 `stage1` 第一次真正完成它应该完成的事：

- **输出一张能支撑 `stage2` 做 `4h-1d` 交易的市场地图**
