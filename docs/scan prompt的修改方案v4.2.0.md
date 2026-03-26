# Scan Prompt & Schema 修改方案 v4.2.0

## 0. 版本定位

`v4.2.0` 不是推翻 `v4.1.0`。

这版收敛两个在真实样本里已经暴露出来的问题：

1. `4h / 1d ladder` 仍然容易被近端价格带挤满，导致 outer boundary 丢失。
2. `v4.1.0` 里同一批价格几何仍然会在多个字段里重复出现，增加 token 和注意力噪音。

一句话说：

- **`v4.1.0` 让 ladder 成为唯一价格地图**
- **`v4.2.0` 让价格地图既完整，又去重**

---

## 1. 第一性原理判断

`stage1 scan` 的职责是：

- 解析市场
- 解析主要参与者的目的和行为
- 给 `stage2` 一张足以支撑 `4h-1d` 交易决策的市场地图

`stage2` 的职责是：

- 在这张地图之上
- 结合实时数据
- 给出方向、`entry`、`stop_loss`、`take_profit`、`leverage`

所以 `stage1` 输出的价格带，不应该是：

- 所有 raw 候选价格带
- 也不应该只是最近几层局部带

它应该是：

- **从当前价格到 thesis 边界的主要路径地图**

这张地图天然包含三类结构：

- 当前 containing bracket
- 当前到中间路径上的主要结构带
- thesis path 的 outer boundary

其中：

- 当前到中间路径是一个 `array` 结构
- outer boundary 是一个**确定性单槽位 truth**

这就是 `v4.1.0` 还不够的根本原因：

- 它把 outer boundary 也放进了 ladder 数组
- 结果 LLM 的 proximity bias 会优先填近端条目
- outer boundary 没有自己的 slot，就会被挤掉

---

## 2. 为什么数组容器天然不够

真实样本已经证明：

- 即使 prompt 里写“ladder 要延伸到 thesis boundary”
- LLM 仍然会优先填：
  - nearest support
  - nearest reclaim
  - nearest cap

因为数组有一个结构性问题：

- 所有条目地位平等
- 没有一个位置被保留给 outer boundary

这带来三个后果：

### 2.1 outer boundary 没有保留槽位

prompt 可以建议“保留 outer boundary”，
但 schema 不能强制“数组里必须有一条 outer boundary”。

### 2.2 validator 也无法硬检查

validator 可以检查：

- 数组存在
- 条数上限
- 字段类型

但无法可靠判断：

- 哪一条才算真正的 outer boundary

### 2.3 stage2 还要自己猜

如果 outer boundary 只是数组中的一条，
`stage2` 还得自己判断：

- 哪条是 near
- 哪条是 intermediate
- 哪条是 outer ceiling / invalidation floor

这会把本该由 `stage1` 完成的地图工作，又推回给 `stage2`。

---

## 3. v4.2.0 的核心决策

## 3.1 显式引入 `thesis_floor` 与 `thesis_ceiling`

每个 thesis timeframe（`4h`、`1d`）新增两个 required 字段：

- `thesis_floor`
- `thesis_ceiling`

它们是 named slots，不是数组条目。

这两个字段的职责是：

- `thesis_floor`
  - 当前 thesis 路径向下的最外层 still-live invalidation / swing boundary
- `thesis_ceiling`
  - 当前 thesis 路径向上的最外层 objective / structural ceiling

一句话说：

- **outer boundary 不再靠数组“记得留一条”**
- **outer boundary 直接成为 required 字段**

## 3.2 ladder 只负责近端到中间路径

有了 `thesis_floor` / `thesis_ceiling` 之后：

- `support_ladder`
- `resistance_ladder`

就不再承担 outer boundary 的职责。

它们只负责：

- 当前价附近最相关的 major zone
- 向 thesis boundary 走过去时，中间真正会塑造路径的 major zones

这样每个容器的职责就被切开了：

- `active_range`: 当前 containing bracket
- `support_ladder / resistance_ladder`: near + intermediate path nodes
- `thesis_floor / thesis_ceiling`: outer path limits

## 3.3 保持 ladder 小而清晰

outer boundary 拿出去之后，ladder 不需要再保留太多条目。

建议改成：

- `support_ladder.maxItems = 3`
- `resistance_ladder.maxItems = 3`
- `support_ladder + resistance_ladder` combined max = `5`

这里 combined max 设成 `5`，而不是更低，是为了不牺牲：

- 单边路径更复杂时的中间层表达
- `4h` 和 `1d` 在不同市场阶段的非对称结构数量

所以这版不是为了压缩而压缩，而是：

- outer boundary 拿出去后
- ladder 只保留真正的 path nodes

---

## 4. schema 总体形态

## 4.1 thesis timeframe

每个 `4h / 1d` timeframe 变成：

```json
{
  "active_range": { "low": ..., "high": ... },

  "support_ladder": [
    { "low": ..., "high": ..., "role": "...", "reason": "..." }
  ],
  "resistance_ladder": [
    { "low": ..., "high": ..., "role": "...", "reason": "..." }
  ],

  "thesis_floor": {
    "low": ...,
    "high": ...,
    "reason": "..."
  },
  "thesis_ceiling": {
    "low": ...,
    "high": ...,
    "reason": "..."
  },

  "state_parse": { ... },
  "flow_override": { ... },
  "participant_parse": { ... },
  "fragility_summary": "..."
}
```

## 4.2 `active_range` 的职责

`active_range` 继续保留。

它的职责不是 thesis 边界，而是：

- 当前 containing bracket
- 当前价格正在被哪一个主区间承载

这和：

- `support_ladder / resistance_ladder`
- `thesis_floor / thesis_ceiling`

表达的是不同层次的信息，不冗余。

## 4.3 ladder entry 的职责

每个 ladder entry 只表达：

- 一个 near / intermediate path node
- 一个独立的主要结构角色

它不再负责表达 outer thesis limit。

## 4.4 `thesis_floor` / `thesis_ceiling` 的职责

这两个字段是 `stage2` 最直接可消费的 outer anchors：

- `thesis_floor`
  - 更适合做最远 invalidation reference
- `thesis_ceiling`
  - 更适合做最远 objective / ceiling reference

这两个价格带不在 ladder 中重复。

---

## 5. 去重方案

这版不只解决 outer boundary，还要解决 `v4.1.0` 残留的重复表达。

### 5.1 价格几何只保留一份

价格几何的唯一承载层应该是：

- `active_range`
- `support_ladder`
- `resistance_ladder`
- `thesis_floor`
- `thesis_ceiling`

其他结构层不应重新复制完整价格带。

### 5.2 去重的边界

这里的去重，不是把整个 JSON 做成数据库范式化。

LLM 不是关系型数据库。

因此去重应当只发生在：

- 结构价格地图层
- cross-timeframe 重复地图层

而不应强迫 `participant_parse` 通过引用系统去解引用价格。

### 5.3 participant 保留内联价格可读性

`participant_parse` 里的价格重复，不应被视为需要消灭的坏重复。

原因是：

- ladder 说的是：这里有一个结构价格带
- participant 说的是：这个参与者正在守/打这个价格带

这两处虽然价格相同，但语义不同。

所以 `participant_observations` 继续保留：

- `current_task`
- `target_zone: {low, high} | null`

不改成：

- `target_zone_id`
- `at_stake_zone_ids`

### 5.4 `current_task` 可以继续带价格

`current_task` 的第一职责是：

- 让 `stage2` 一眼读懂谁在守哪层、谁在打哪层

因此它可以继续直接写：

- `Defend 2127.29-2155.85; keep daily PVS rejection intact`
- `Break 2117.19/2111.67 and expose 2098.68-2102.0`

这比让另一个 LLM 再去做一次 id dereference 更可靠。

### 5.5 `unresolved_factors` 继续保持关系型

`unresolved_factors` 在 `v4.1.0` 已经收敛成：

- value conflict
- flow conflict
- sponsorship conflict
- execution vs thesis conflict

这版继续保持，不把价格列表重新塞进去。

### 5.6 不再恢复 `shared_levels`

outer boundary 单独命名后，也不需要恢复任何第二张价格地图。

去重后的地图仍然只有一份：

- timeframe-level bands

---

## 6. 对 prompt 的需求描述

因为 `thesis_floor` / `thesis_ceiling` 已经有了显式槽位，
prompt 不再需要长篇解释“记得给 ladder 留 outer boundary”。

更正确的需求描述应该是：

```text
THESIS PRICE-BAND MAP

The downstream model reads each 4h and 1d map to decide direction, entry,
stop-loss, take-profit, and leverage for a 4h to 1d trade.

For that to work, each thesis timeframe needs:

- `active_range`: the current containing bracket
- `support_ladder` and `resistance_ladder`: the near and intermediate major zones
  that shape the path from current price
- `thesis_floor`: the outermost still-live support boundary for the thesis path
- `thesis_ceiling`: the outermost still-live resistance boundary for the thesis path

The ladder does not carry the outer boundary. The ladder carries the near and
intermediate path between current price and those boundaries.
```

这段不是命令模型“别忘了什么”，而是在定义：

- `stage2` 需要读到什么样的成品

---

## 7. 对 participant prompt 的需求描述

participant 层的需求也应改成：

```text
PARTICIPANT TASK MAP

The downstream model needs to read each participant task and directly see which
price zones are at stake.

`current_task` should make the defended or attacked price bands immediately legible.
`target_zone` keeps the main price band the participant is pushing price toward
when such a target is clear.
```

这样 prompt 描述的是：

- `stage2` 要怎样读 participant intent

而不是命令模型“去锚定价格”。

---

## 8. validator 与 stage2 的收益

有了显式 outer boundary 字段之后：

### 8.1 validator 可以硬检查

现在 validator 可以直接检查：

- `thesis_floor` 是否存在
- `thesis_ceiling` 是否存在
- 价格区间是否有效
- `thesis_floor` 是否真的位于 `support_ladder` 最外侧
- `thesis_ceiling` 是否真的位于 `resistance_ladder` 最外侧

它不再需要从数组里猜哪一条算 outer。

也就是说，validator 还可以直接检查：

- `thesis_floor.high <= min(support_ladder[*].low)`
- `thesis_ceiling.low >= max(resistance_ladder[*].high)`

不需要额外引入距离阈值；只要保证 outer boundary 确实位于 ladder 之外即可。

如果某一侧 ladder 为空：

- 跳过该侧的 outer-vs-ladder 距离校验
- 但 `thesis_floor` / `thesis_ceiling` 本身仍然必须存在

这样可以覆盖“某一侧没有有意义的 intermediate path node，只剩 outer boundary”这种合法场景。

### 8.2 stage2 读取更直接

`stage2` 不需要再从 ladder 里猜最外层：

- 直接读 `thesis_floor`
- 直接读 `thesis_ceiling`

### 8.3 ladder 更干净

因为 outer boundary 已经有命名槽位，

- ladder 就只负责 path nodes
- near nodes 不会再挤掉 outer boundary

---

## 9. 实施要求

### 9.1 schema

新增：

- `thesis_floor`
- `thesis_ceiling`

### 9.2 merge / validation

validator 需要：

- 检查 `thesis_floor` / `thesis_ceiling` 必填
- 检查 ladder combined max = `5`
- 检查 `thesis_floor.high <= min(support_ladder[*].low)`
- 检查 `thesis_ceiling.low >= max(resistance_ladder[*].high)`
- 当某一侧 ladder 为空时，跳过该侧的 min/max 距离校验，但 outer boundary 仍必须存在
- 如果 outer boundary 落在 ladder 内部，则视为 schema-valid 但 semantics-invalid，应拒绝这份 scan

### 9.3 prompt

把 ladder 需求从“记得延伸到 outer boundary”改成：

- `thesis_floor`
- `thesis_ceiling`
- `ladder only carries near/intermediate nodes`

### 9.4 review / evaluation

后续复核时重点看：

- `4h / 1d` 是否都有 outer boundary
- ladder 是否只保留 near / intermediate major bands
- participant 是否仍然直接可读，不需要再做引用解码
- stage2 是否能直接读取 outer boundary 而不再猜测

---

## 10. 最终结论

`v4.2.0` 的收敛结论是：

- `v4.1.0` 的 ladder-first 方向是对的
- 但 outer boundary 不能继续放在 ladder 数组里碰运气
- **outer boundary 应成为 required named fields：`thesis_floor` 和 `thesis_ceiling`**
- 同时，去重应发生在价格地图层，而不是把 participant 变成引用系统

一句话说：

- **`stage1 scan` 要给 `stage2` 的，不是最近几层价格带**
- **而是一张去掉重复地图、但保留 participant 直接可读性的 thesis price-band map**
