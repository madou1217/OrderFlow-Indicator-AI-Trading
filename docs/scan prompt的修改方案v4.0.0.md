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
- **重做 `4h / 1d` 的主输出层**
- **精简 participant_parse，用内联价格替代 zone_id 引用**
- **删除 `key_levels`，ladder 是唯一结构层级输出**

这里要明确一件事：

- **`v4.0.0` 不建立在"去掉 15m 完整解析就足以修复 `4h / 1d` 价格带过窄"这个假设上。**

去掉 `15m` full scan 只会：

- 减轻一部分 near-price anchoring bias

但它本身并不能自动解决：

- `4h / 1d` 价格带过窄
- `4h / 1d` 结构层数不足

真正的根因修复是：

- **把 `4h / 1d` 的主输出从"单层 dominant zone 摘要"重做成"可交易的结构层级地图"。**

目标不是再优化"当前组织区"的表达，而是让 `stage1` 真正输出：

- `4h / 1d` 的关键价格带层级
- `4h / 1d` 的当前市场结构
- `4h / 1d` 的主要参与者正在守哪里、打哪里、盯哪里

这里的"关键价格带层级"要进一步定义清楚：

- 以 `4h / 1d` 决策真正会用到的主要价格带为主
- 相邻价格带的间隔要足够大，避免把 near-price 噪音塞满 ladder
- 每个 thesis timeframe 只保留最重要的价格带，总数控制在 **最多 6 条**

---

## 1. 最高原则

本版完全服从下面这个最高原则：

- 不降模型输出质量是第一需求
- 不为了减少模型思考时长而降低输出质量
- 不用考虑工作量

因此，本版不是为了"简化 schema 让模型更快"。

本版是为了：

- **让 `stage1` 终于输出 `stage2` 真正需要的市场地图**

只有在这个前提下，任何输入/输出优化才有意义。

同时，本版遵守一条实现原则：

- **模型的 token 应花在"选对价格层级"上，不是花在"填分类枚举"上**

因此，schema 的每个字段都必须直接服务于 stage2 交易决策。
如果一个字段的信息可以从其他字段推导出来，或者只增加分类开销而不增加决策信息量，就不应存在。

对应到 `4h / 1d` 的主输出层上，这意味着：

- 输出应优先保留主要价格带本身
- 每条价格带都应具有明确的交易决策意义
- 不为历史叙事、重复分类、近端噪音保留字段空间

---

## 2. 重新明确 Stage1 / Stage2 的职责

## 2.1 Stage1 的职责

`stage1 scan` 的职责是：

- 解析市场
- 解析主要参与者的目的和行为
- 为 `stage2` 提供一张足以支撑 `4h-1d` 交易决策的市场地图

关键点：

- `stage1` 不是只回答"当前最近哪里在起作用"
- `stage1` 也不是只回答"当前 market state 是什么"
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

当前 `stage1 scan` 的错误，不是"价格算错了"这么简单。

根本问题是：

- **它把 `4h / 1d` 市场分析压缩成了"当前最近主组织区摘要"**

这会带来 7 个直接后果。

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

这不是"表达不够好"，而是**结构不够表达**。

## 3.2 `key_levels` 被近价格锚定吞噬

当前 `key_levels` 最多 6 个，本来应该覆盖整个 thesis 范围。

但实际日志表明：

- 4h 的 6 个 key_levels 全部挤在 $13 范围内（2133-2147）
- 1d 的 6 个 key_levels 只有 1 个在远端（2102），其余 5 个挤在 $15 内

原因：`dominant_demand_zone / dominant_supply_zone` 锚定了模型对"当前重要区域"的理解，`key_levels` 自然围绕这两个 zone 展开，而不是按 thesis 范围展开。

`key_levels` 不是独立的层级地图——它是 dominant zone 的附属品。
只要 dominant zone 锚定在近端，key_levels 也必然聚集在近端。

## 3.3 `15m` anchoring 只是放大器，不是根因

移除 `15m` 完整解析是对的，因为它能减轻一部分近价格锚定。

但必须明确：

- 即使 `15m` 不再做 full scan
- 只要 `4h / 1d structure_parse` 仍然只有单层 `dominant_demand_zone / dominant_supply_zone`

模型依然会继续把高时间框架压成：

- 最近一层
- 当前一层
- 最显眼的一层

所以：

- **`v4.0.0` 的主要修复不是"拿掉 15m"**
- **而是"把 `4h / 1d` 的主输出改成 ladder-first 的结构地图"**

## 3.4 "active now" 被错误实现成"只保留当前最近一层"

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

## 3.5 失效区域和角色翻转区域没有被正确处理

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
- **失效后已经翻转成当前 supply / demand 的区域，应保留，但要以"当前角色"表达**

例如：

- 原来的 reclaim band 失败后，如果现在成了 overhead supply
  那它不应该再以"旧 reclaim attempt"身份保留，
  而应该以"当前压力带"身份保留。

## 3.6 `structure_lifecycle` 是最大的 token 浪费

v3.4.0 日志显示 `structure_lifecycle` 在 4h 和 1d 各产生 5 个 structure，共占 57+ 行字段。

这些结构绝大多数是历史叙事：

- 某个 acceptance_attempt 正在 failing
- 某个 value_area 仍然 active
- 某个 demand_zone 正在被 test

这些信息对 stage2 的直接交易决策贡献极低——stage2 需要的是"哪些价格还能用"，不是"哪些结构曾经有过什么生命周期"。

这些 token 完全应该花在 ladder 的价格层级选择上。

## 3.7 `stage2` 会天然被近端结构绑架

如果 `stage1` 只给最近一层，`stage2` 虽然理论上还能看 raw 输入，
但在实践里会天然被：

- 最近的 support
- 最近的 resistance
- 最近的 ask wall / bid wall

绑住。

这会导致：

- `entry`、`tp`、`sl` 都偏近端
- `4h / 1d` 交易被缩成一笔"当前附近的结构表达"

这不是 `stage2` 的错，而是 `stage1` 地图不完整。

---

## 4. v4.0.0 的核心决策

## 4.1 保留什么

保留：

- `v3.3.0` 的 deterministic 预计算
- `v3.4.0` 的 `execution_context_15m`
- `4h / 1d` 的 full market parse

## 4.2 删除什么

从 `4h / 1d` 的主输出层中删除：

- `dominant_demand_zone` — 被 `support_ladder[0]` 替代
- `dominant_supply_zone` — 被 `resistance_ladder[0]` 替代
- `invalidation_level` — 被 support/resistance ladder 最远层替代
- `structure_lifecycle` — 完全删除，历史叙事，token 浪费
- `key_levels` — 被 ladders 完全替代
- `range_width_vs_atr` — 可由 `active_range` 和 stage2 已有 ATR 数据自行判断

### 为什么删除 `key_levels`

有了 `support_ladder` + `resistance_ladder`（共最多 6 条主要价格带），`key_levels` 变成冗余：

- 如果同时保留 `key_levels` 和 ladders，模型会把重要信息分散到两处，导致两边都不完整
- POC 等单点 level 可以作为 zone（`low ≈ high`）放入 ladder
- `cross_timeframe_parse.shared_levels` 已经承担跨时间框架关键点位的角色

一个输出里不应该有两个竞争性的"关键价格"字段。ladder 是唯一的结构层级输出。

## 4.3 用什么替代

用下面这组字段替代：

- `support_ladder`
- `resistance_ladder`
- `active_range`

一句话说：

- **从"单层 dominant zone + key_levels 补充"切换为"价格带地图优先的主输出层"**
- **`support_ladder / resistance_ladder` 是主输出，`active_range` 只负责描述当前 containing bracket**

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

## 6. 如果给价格带，要不要给"全量"

这个问题要分清两种"全量"。

## 6.1 不该给的"全量"

不该给：

- 所有 raw candidate
- 所有历史结构
- 所有近端节点
- 所有已失效区域

这不是市场解析，只是把候选池倒给 `stage2`。

## 6.2 应该给的"全量"

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

- `stage1` 不能停在"当前最近正在组织价格的一层"
- 必须把直到 thesis objective / thesis invalidation 为止、仍然影响交易决策的层级都交给 `stage2`

同时，这个完整层级不是"越多越好"。

它应该满足：

- 只保留主要价格带
- 价格带之间有足够的层级间隔
- 每个 thesis timeframe 的价格带总数最多 6 条

---

## 7. `4h / 1d structure_parse` 应该改成什么

## 7.1 新的职责

新的 `structure_parse` 不再回答：

- "当前最 dominant 的 demand / supply 是哪一层"

而要回答：

- 下方从近到远的重要支撑层级是谁
- 上方从近到远的重要阻力层级是谁
- 哪些层级是当前 thesis 的真正决策边界

## 7.2 推荐 schema

```json
"structure_parse": {
  "active_range": {
    "low": 2102.0,
    "high": 2185.2
  },
  "support_ladder": [
    {
      "low": 2136.16,
      "high": 2137.66,
      "role": "value_edge",
      "reason": "4h sigma2 floor and daily TPO lower value edge cluster"
    },
    {
      "low": 2133.81,
      "high": 2133.81,
      "role": "liquidity_cluster",
      "reason": "nearest bid wall below the 4h bracket floor"
    },
    {
      "low": 2102.0,
      "high": 2102.0,
      "role": "swing_low",
      "reason": "daily swing low, thesis invalidation if lost"
    }
  ],
  "resistance_ladder": [
    {
      "low": 2145.36,
      "high": 2147.16,
      "role": "value_edge",
      "reason": "ask wall and 4h lower value edges cap reentry"
    },
    {
      "low": 2158.83,
      "high": 2164.26,
      "role": "reclaim_band",
      "reason": "daily PVS lower value overlaps upper TPO band, failed reclaim"
    },
    {
      "low": 2185.2,
      "high": 2185.2,
      "role": "swing_high",
      "reason": "upper daily PVS value and prior rejection ceiling"
    }
  ]
}
```

### 7.2.1 zone 字段设计原则

每个 zone 只有 4 个字段：`low`、`high`、`role`、`reason`。

**不设 `zone_id`**：LLM 维护一致 ID 极其不可靠。如果 `participant_parse` 引用 `"1d_support_1"`，模型很容易在 ladder 里写成 `"1d_support_s1"` 或别的名字，导致 validation 失败或引用断裂。

**不设 `importance` 枚举**：层级重要性隐含在排序里——第一个 zone 就是 immediate，最后一个就是 outer。额外的枚举只增加分类 token 开销。

**不设 `status` 枚举**：如果一个 zone 还在 ladder 里，它就是 active 的。如果它正在 testing 或 failing，写在 `reason` 里。已经完全失效的 zone 不应出现在 ladder 中。

这样做的直接好处：模型在每个 zone 上只花 4 个字段的 token，而不是 7 个。省下来的 token 用在"选对价格层级"上，而不是"填分类枚举"上。

### 7.2.2 价格带数量原则

每个 thesis timeframe 的 ladder 输出，目标是：

- **support_ladder + resistance_ladder 合计最多 6 条价格带**

这 6 条不是固定地平均分配给上下两侧。

而是按当前 thesis 路径决定：

- 哪一侧承担更多决策层级，就给哪一侧更多价格带
- 另一侧只保留仍然影响 `entry / sl / tp` 的主要带

一句话说：

- **数量服从 thesis 决策路径，不服从平均分配**

### 7.2.3 `role` 枚举

```
value_edge | imbalance_support | imbalance_resistance |
demand_flip | supply_flip |
swing_low | swing_high |
balance_boundary | liquidity_cluster |
reclaim_band | rejection_band
```

`role` 的职责是标注这个 zone 的结构角色，不是它的状态。

### 7.2.4 单点 level 的处理

POC、sigma 线、pivot 等单点 level：用 `low ≈ high` 的 zone 表示。

例如：

```json
{
  "low": 2151.49,
  "high": 2151.49,
  "role": "balance_boundary",
  "reason": "daily TPO POC balance pivot"
}
```

不需要额外的 `key_levels` 字段来容纳单点 level。

## 7.3 为什么不保留 `current_structure_zone`

初版方案包含了 `current_structure_zone` 作为"当前位置标签"。

不再保留，原因：

- **`active_range` 已经描述了包含当前价格的主区间**
- **`support_ladder[0]` 和 `resistance_ladder[0]` 的间距就是当前价格所在的局部结构**
- 额外输出一个 zone object 不增加信息量，只增加 token 开销
- 更关键的是：如果 `current_structure_zone` 存在，模型有退化到"围绕 current zone 展开分析"的风险，重蹈 `dominant_zone` 的覆辙

`active_range` + ladders 已经完整描述了"当前价格在结构中的位置"。

## 7.4 为什么这样更对

这组字段能明确分开：

- 下方层级（从近到远）
- 上方层级（从近到远）

这样 `stage2` 才能直接读到：

- 下一层支撑
- 更远层目标
- 更外层失效
- 上方第一层阻力
- 更远层 thesis objective

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

- 每个 thesis timeframe 的 ladder 总数：max 6 条主要价格带

如果当前 thesis 路径只需要 4-5 条主要价格带，可以少于 6。

如果当前 thesis 路径需要 6 条以内的完整层级，输出应覆盖到完整层级。

这里的关键不是固定数量本身，
而是：

- **不能因为当前 schema 的近端偏置或低 item 限制，省略仍然影响 thesis 的下一层/外层结构**

## 8.4 thesis 边界覆盖要求

这是价格带地图完整性的核心要求：

- `support_ladder` 的最外层应覆盖 thesis invalidation 区域
- `resistance_ladder` 的最外层应覆盖 thesis objective 区域（或更远的结构天花板）
- 中间价格带应覆盖从当前价到 thesis 边界之间仍然影响决策的主要层级

这层要求的作用不是给模型加额外规则，
而是明确我们真正需要什么：

- 一张从当前价延伸到 thesis 边界的完整价格带地图

## 8.5 1d 与 4h 的分辨率区分

- 1d ladder 必须反映 1d 分辨率的结构，不是 4h 分辨率
- 如果 1d 和 4h 的 ladder 几乎一样宽（如 v3.4.0 日志中 1d active_range $27 vs 4h $28），说明 1d 没有给出应有的结构深度

Prompt 中应明确写：

```
A 1d ladder expresses 1d-resolution structure, with wider spacing and higher-level bands than the 4h ladder.
The 4h ladder expresses the nearer thesis structure inside that broader 1d map.
```

## 8.6 已失效区域如何处理

规则很明确：

- 完全失效且不再影响当前 auction 的区域：不写入 ladder
- 失效后已翻转成当前作用区的区域：保留，但用当前角色写入 ladder

所以：

- "旧角色"不保留
- "当前角色"保留

---

## 9. 参与者解析必须绑定到价格

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

## 9.2 为什么不用 zone_id 引用

初版方案使用 `defended_zone_ids` / `attacked_zone_ids` / `next_target_zone_ids` 引用 ladder 中的 zone_id。

这个方向是对的——participant 必须绑定到具体价格。但 zone_id 引用有两个致命问题：

1. **LLM 生成 ID 不一致**：模型在 ladder 里写 `"1d_support_1"`，在 participant 里引用 `"1d_support_s1"` 或 `"1d_sup_1"`。这不是偶尔发生，是常态。
2. **Validation 复杂度爆炸**：需要交叉验证 participant 里引用的每个 zone_id 确实存在于 ladder 中。

## 9.3 推荐改法：内联 `target_zone`

用 optional 的内联价格 zone 替代 zone_id 引用：

```json
"participant_observations": [
  {
    "participant_role": "initiative_flow",
    "current_task": "Keep rejection below 4h value 2146-2147",
    "task_status": "accepted",
    "target_zone": {"low": 2133.81, "high": 2136.16},
    "constraints": ["2136.16 floor is still holding", "buy-side OB pressure present"],
    "evidence": ["4h combined value read is rejected_from_below", "whale flow seller-aligned"],
    "confidence": "high"
  }
]
```

**`target_zone`**（optional，可为 null）：这个 participant 正在推价格去的地方。

- 对于 initiative sellers：target_zone 是下方的 breakdown 目标
- 对于 passive liquidity defenders：target_zone 是 null（他们在防守，不在推价格）
- 对于 higher_timeframe_sponsorship：target_zone 是他们试图 reclaim 的区域

`defended` 和 `attacked` 的价格已经隐含在 `current_task` 文本里（例如 "Keep rejection below 2146-2147"）。
只有 `target_zone` 需要显式结构化，因为它直接影响 stage2 的 TP 选择。

这样做：

- 不需要 zone_id
- 不需要交叉验证
- participant 的目标价格仍然是结构化的
- stage2 可以直接读取 target_zone 来辅助 TP 决策

---

## 10. `state_parse` 和 `flow_override` 仍然保留

本版不推翻：

- `state_parse`
- `flow_override`（模型只输出 4 个 context-sensitive 字段）
- `participant_parse`
- `evidence_trace`

因为它们仍然是必要层。

本版只重做：

- `structure_parse`

因为当前最根本的问题不在 flow，而在：

- `4h / 1d` 的结构地图没有被完整表达出来

## 10.1 `evidence_trace` 收紧

将 `supporting_facts` 和 `conflicting_facts` 从 max 4 降到 max 3。

加上 `fragility_summary`，这层已经够了。
省下的 token 用在 ladder 上。

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

- `4h / 1d` 改成 ladder-only 的结构地图

---

## 12. 对 prompt 的要求

## 12.1 prompt 的核心需求定义

prompt 应把 `4h / 1d` 的主要任务定义成：

- 输出可直接用于 `4h-1d` 交易决策的主要价格带地图
- 用 `support_ladder / resistance_ladder` 承载完整结构层级
- 让价格带从当前价一直延伸到 thesis 边界
- 让每条价格带都直接服务 `entry / sl / tp` 判断

## 12.2 ladder 覆盖需求

应明确写：

```text
THESIS PRICE-BAND MAP

For 4h and 1d, return the operative structural ladder needed for a 4h to 1d trade decision.

For each thesis timeframe:
- begin each ladder with the nearest still-relevant major structural zone
- extend support_ladder to the thesis invalidation boundary
- extend resistance_ladder to the thesis objective or outer structural ceiling
- include the intermediate major zones that still shape the thesis path
- keep the ladder focused on major price bands with meaningful spacing
- let the 1d ladder express 1d-resolution structure, not a narrow copy of the 4h ladder
- represent former zones by their current live role when they still shape the auction
```

## 12.3 参与者 prompt 要求

应明确写：

```text
PARTICIPANT TASKS

Anchor participant tasks to the structural ladders.
State which price levels are being defended, attacked, or targeted next directly in current_task.
Use target_zone to explicitly mark the structured price target when a participant is pushing price toward a specific zone.
Describe participant behavior through the current price-band map so a downstream model can directly use it for trading decisions.
```

---

## 13. 对 schema 的直接影响

## 13.1 删除

从 `4h / 1d structure_parse` 删除：

| 字段 | 替代方 |
|------|--------|
| `dominant_demand_zone` | `support_ladder[0]` |
| `dominant_supply_zone` | `resistance_ladder[0]` |
| `invalidation_level` | support/resistance ladder 最远层 |
| `structure_lifecycle` | 完全删除，ladder 已覆盖 active 结构 |
| `key_levels` | ladder zones 完全替代 |

## 13.2 新增

| 字段 | 说明 |
|------|------|
| `support_ladder` | 与 `resistance_ladder` 合计最多 6 条主要价格带，从近到远 |
| `resistance_ladder` | 与 `support_ladder` 合计最多 6 条主要价格带，从近到远 |
| `target_zone` (participant) | optional，标注参与者推价目标 |

## 13.3 保留

保留：

- `active_range`
- `state_parse`（不变）
- `flow_override`（不变）
- `participant_parse`（加 `target_zone`）
- `evidence_trace`（收紧到 max 3）
- `execution_context_15m`（不变）
- `cross_timeframe_parse`（不变）

---

## 14. 完整 per-timeframe schema 总览

```
structure_parse:
  active_range: {low, high}
  support_ladder: [                                与 resistance_ladder 合计 max 6, 从近到远
    {low, high, role, reason}
  ]
  resistance_ladder: [                             与 support_ladder 合计 max 6, 从近到远
    {low, high, role, reason}
  ]

state_parse:                                       (不变)
  value_read: {pvs, tpo, combined}                 (precomputed merge)
  range_state: enum
  auction_state: enum
  control_read: {side, clarity}
  sponsorship_state: enum

flow_override:                                     (不变，模型只输出这 4 个)
  cvd_alignment_vs_price: enum
  orderbook_pressure_side: enum
  orderbook_near_price_constraint: enum
  combined_flow_state: enum

participant_parse:
  participant_observations: [                       max 3
    participant_role: enum
    current_task: string
    task_status: enum
    target_zone: {low, high} | null                新增，optional
    constraints: [string]                           max 3
    evidence: [string]                              max 3
    confidence: enum
  ]

evidence_trace:
  supporting_facts: [string]                        max 3 (原 4→3)
  conflicting_facts: [string]                       max 3 (原 4→3)
  fragility_summary: string
```

---

## 15. 对 Stage2 的意义

按这个设计，`stage2` 将不再只看到：

- 当前最近的一层 zone

而会看到：

- 下方从近到远的支撑层级
- 上方从近到远的阻力层级
- 最远层 = thesis 边界
- 哪个 participant 正在推价格去哪里（target_zone）

这会直接改善：

- `direction` 的判断：ladder 的深度和 participant task 方向提供 thesis 方向性
- `entry` 选择：support/resistance_ladder[0] 是最近执行区
- `stop_loss` 放置：ladder 的外层提供结构化的 SL 参考
- `take_profit` 选择：participant 的 target_zone + 对侧 ladder 提供 TP 候选
- `leverage` 的风险定级：ladder 的深度（从近到远有多少层）反映路径拥挤度

一句话说：

- `stage2` 会第一次真正拿到一张可交易的 `4h / 1d` 地图

---

## 16. Prompt self-check 更新

```text
BEFORE OUTPUT

Before finalizing the JSON, run this self-check:

1. Does each ladder span from the nearest active zone to the thesis boundary, not just the immediate neighborhood?
2. Is the last zone in support_ladder the thesis invalidation boundary?
3. Is the last zone in resistance_ladder the thesis objective or outer structural ceiling?
4. Does the 1d ladder reflect 1d-resolution structure, not just a copy of the 4h ladder?
5. Have I used `precomputed_value_read` as the fixed value-state base?
6. Have I limited `flow_override` to the 4 context-sensitive flow judgements?
7. Does `combined_flow_state` match the relationship between current flow evidence and `state_parse.control_read.side`?
8. Have I anchored each participant task to specific price levels from the ladders?
9. Is `execution_context_15m` a pure execution-layer summary that only contributes entry-quality context?
10. Could a downstream model understand the current market and make a 4h-1d trade decision without reconstructing the raw input?

If any answer is no, revise the parse before emitting the final JSON.
```

---

## 17. 风险与边界

## 17.1 本版不做的事

本版不做：

- 不进一步压缩 `4h / 1d` 输入
- 不删除 `raw_overflow`
- 不减少 `4h / 1d` 的结构候选来源
- 不回退 `15m execution_context`

因为本版的第一目标不是提速，而是：

- **先把 `stage1` 的 4h/1d 市场分析职责做对**

## 17.2 本版最大的收益

本版最大的收益不是 token 节省。

而是：

- `stage1` 不再输出一张"当前组织区摘要"
- 而是输出一张"可供交易的结构层级地图"

这才符合：

- `stage1 scan = 解析市场 + 解析主要参与者目的和行为`

## 17.3 Token 预算变化

虽然 token 节省不是目标，但本版的 token 变化是正向的：

| 删除 | 估算节省 |
|------|---------|
| `structure_lifecycle` (2 tf × ~5 structures × ~6 fields) | ~60 行 |
| `key_levels` (2 tf × ~6 levels × 3 fields) | ~36 行 |
| `dominant_demand_zone` + `dominant_supply_zone` | ~12 行 |
| `invalidation_level` | ~2 行 |

| 新增 | 估算开销 |
|------|---------|
| `support_ladder` + `resistance_ladder` (2 tf × ~6 zones × 4 fields) | ~48 行 |
| `target_zone` (2 tf × ~3 participants × 1 zone) | ~6 行 |

净变化：约 -56 行。节省的 token 被重新分配到"选对价格层级"上。

---

## 18. 最终结论

按第一性原理重做之后，结论非常明确：

- `stage1` 必须给出价格带
- 但给的不是 raw 全量候选
- 而是对 `4h-1d` 交易决策完整的结构层级

因此：

- 当前 `v3.x` 的单层 `dominant zone` + `key_levels` 设计不够
- `4h / 1d structure_parse` 必须升级成：
  - `support_ladder`
  - `resistance_ladder`
  - 两条 ladder 合计最多 6 条主要价格带，并覆盖从当前价到 thesis 边界的完整决策层级
  - 内联 `target_zone` 的 `participant_parse`
- 删除 `dominant_demand_zone`、`dominant_supply_zone`、`invalidation_level`、`structure_lifecycle`、`key_levels`
- 不设 `zone_id`、`importance`、`status` 枚举——模型的 token 花在选价格，不是填分类

这版的核心不是让模型更快，
而是让 `stage1` 第一次真正完成它应该完成的事：

- **输出一张能支撑 `stage2` 做 `4h-1d` 交易的市场地图**
