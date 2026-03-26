# Scan Prompt & Schema 修改方案 v4.1.0

## 0. 版本定位

`v4.1.0` 不是重做 `v4.0.0`。

这版只收敛一个问题：

- `cross_timeframe_parse.shared_levels` 在 `v4.0.0` 之后，已经和 `timeframes.4h / timeframes.1d` 的 `support_ladder + resistance_ladder` 发生了明显职责重叠。

`v4.0.0` 已经把 `4h / 1d` 的主输出改成了：

- `active_range`
- `support_ladder`
- `resistance_ladder`

这意味着：

- `4h / 1d` 的主价格地图已经完整存在
- `stage2` 读市场时已经有足够的结构层级

在这个前提下，再保留一份 `shared_levels`，就会产生第二张价格地图。

---

## 1. 第一性原理判断

`stage1 scan` 的职责是：

- 解析市场
- 解析主要参与者的目的和行为
- 给 `stage2` 一张足以支撑 `4h-1d` 交易决策的市场地图

如果 `v4.0.0` 的 ladder-first 设计是成立的，那么：

- `timeframes.4h.support_ladder + resistance_ladder`
- `timeframes.1d.support_ladder + resistance_ladder`

就已经是这张地图的主体。

此时 `shared_levels` 再保留一份“跨周期关键点位列表”，会导致：

- 地图重复
- 注意力分散
- `stage2` 在 ladder 与 shared_levels 之间来回判断“哪个更重要”

这不符合“每个字段都必须直接服务决策”的原则。

一句话说：

- **如果 ladder 是主地图，`shared_levels` 就不应再作为第二张价格地图存在。**

---

## 2. 当前问题是什么

从当前 `v4.0.0` 的实际输出看，`shared_levels` 里的大多数价格，本质上已经被 ladder 覆盖。

例如：

- `2102.00` 这类 swing low，已经在 `support_ladder`
- `2142.63` 这类 reclaim band 下沿，已经在 `resistance_ladder`
- `2200.11` 这类 outer swing high，已经在 `resistance_ladder`

所以 `shared_levels` 现在的主要效果是：

- 从 ladder 里再摘一遍点位

而不是新增一层真正不可替代的 cross-timeframe 信息。

这会带来三个直接问题：

### 2.1 结构信息重复

同一批关键价格在：

- timeframe ladder
- cross_timeframe shared_levels

两处重复出现。

### 2.2 stage2 注意力被二次分流

`stage2` 原本只需要围绕：

- `4h / 1d ladders`
- `participant_parse`
- `execution_context_15m`

来做交易决策。

如果还要额外读一份 `shared_levels`，它就要再做一次判断：

- 这个 shared level 是否比 ladder 更重要？
- 如果 shared level 与 ladder 有轻微不一致，应该信哪边？

### 2.3 价格地图重新分裂

`v4.0.0` 的重要改进，是让 ladder 成为唯一结构地图。

保留 `shared_levels` 会把这个收敛重新打开：

- ladder 是一张地图
- shared_levels 又像另一张“关键点位地图”

这违背了 `v4.0.0` 的主方向。

---

## 3. v4.1.0 的核心决策

### 3.1 删除 `shared_levels`

从 `cross_timeframe_parse` 中删除：

- `shared_levels`

### 3.2 保留 `cross_timeframe_parse` 的其余字段

保留：

- `execution_alignment_15m`
- `main_tension`
- `unresolved_factors`
- `cross_market_parse`（persisted full scan 中继续保留，由 Rust merge 注入）

这样 `cross_timeframe_parse` 的职责就变得清晰：

- 不再输出第二张价格地图
- 只负责跨周期关系本身

一句话说：

- **价格在 timeframe ladders 里**
- **跨周期关系在 cross_timeframe_parse 里**

---

## 4. 为什么直接删除，而不是继续收窄

理论上，也可以把 `shared_levels` 缩成：

- 只保留 `2-3` 个真正的跨周期共振点

例如：

- shared POC cluster
- shared AVWAP/value overlap
- shared multi-timeframe pivot

但从第一性原理看，这仍然不是最干净的方案。

因为：

- 这些点位本身仍然是价格地图的一部分
- 只要它们对交易决策重要，就应该进入对应 timeframe 的 ladder
- 如果它们不够重要到进入 ladder，就不应在 cross 层单独保留一份

所以 `v4.1.0` 直接选择更干净的收敛：

- **删掉 `shared_levels`**

---

## 5. schema 修改

## 5.1 `cross_timeframe_parse`

从：

```json
"cross_timeframe_parse": {
  "shared_levels": [...],
  "execution_alignment_15m": "...",
  "main_tension": "...",
  "unresolved_factors": [...]
}
```

改成：

```json
"cross_timeframe_parse": {
  "execution_alignment_15m": "...",
  "main_tension": "...",
  "unresolved_factors": [...]
}
```

最终 persisted scan 中仍可保留：

```json
"cross_market_parse": {
  "spot_vs_futures_gap_pct": ...,
  "flow_driver": "...",
  "latest_4h_delta_relation": "..."
}
```

所以 full shape 是：

```json
"cross_timeframe_parse": {
  "execution_alignment_15m": "...",
  "main_tension": "...",
  "unresolved_factors": [...],
  "cross_market_parse": {...}
}
```

## 5.2 `timeframes`

不变。

`4h / 1d` 继续承担完整价格带地图：

- `active_range`
- `support_ladder`
- `resistance_ladder`
- `state_parse`
- `flow_override` / `flow_parse`
- `participant_parse`
- `fragility_summary`

---

## 6. 对 prompt 的要求

prompt 不再要求输出：

- `shared_levels`

而应明确表达这个需求：

```text
CROSS-TIMEFRAME RELATION

The cross-timeframe layer is not a second price map.

The downstream model already reads the 4h and 1d ladders as the full thesis price-band map.
The cross-timeframe layer only needs to explain:

- whether 15m execution is supporting or degrading the 4h-1d thesis
- what the main cross-timeframe tension is
- what unresolved conflicts still matter now

`unresolved_factors` are relational statements about conflicting signals, not price levels.
Each factor describes a tension that still matters now, not another zone in disguise.
```

这段需求描述的重点是：

- 不是告诉模型“不要输出 shared_levels”
- 而是说明：
  - `cross_timeframe_parse` 的职责不包括第二张价格地图

---

## 7. 对 stage2 的影响

删除 `shared_levels` 后，`stage2` 的读取路径会更干净：

- 结构价格地图：只看 `4h / 1d ladders`
- execution 环境：看 `execution_context_15m`
- 跨周期关系：看 `execution_alignment_15m + main_tension + unresolved_factors + cross_market_parse`

这里的 `unresolved_factors` 只负责保留关系型冲突：

- value model conflict
- flow conflict
- sponsorship conflict
- execution vs thesis conflict

它不负责重新列出价格带。

这会带来三个直接收益：

- 降低重复信息
- 降低近端点位被重复强调的概率
- 强化 ladder 作为唯一结构地图的权威性

---

## 8. 为什么这不会降低输出质量

删除 `shared_levels` 不会减少真正的市场信息量，前提是：

- 所有对交易决策重要的价格层级，都已经进入 `4h / 1d ladders`

在 `v4.0.0` 的设计里，这本来就是要求。

因此：

- `shared_levels` 的删除不是删信息
- 而是删重复表达

一句话说：

- **`shared_levels` 不是新的真相，只是已存在真相的第二次表达。**

---

## 9. 实施要求

### 9.1 response schema

从 reduced model response schema 中删除：

- `cross_timeframe_parse.shared_levels`

### 9.2 full persisted scan schema

也删除：

- `cross_timeframe_parse.shared_levels`

### 9.3 validator

删除：

- `shared_levels` 的 required 校验
- `validate_key_levels_with_max(.../shared_levels...)`

### 9.4 prompt

删掉 output contract 中对 `shared_levels` 的要求。

### 9.5 tests

更新：

- scan response shape tests
- parse/merge tests
- finalize input tests（如果有固定断言 cross layer 字段）

---

## 10. 最终结论

`v4.1.0` 的收敛结论很简单：

- `v4.0.0` 已经让 ladder 成为主价格地图
- 那么 `shared_levels` 就不该继续存在

最终职责分工应是：

- `4h / 1d ladders`：唯一的 thesis 价格地图
- `execution_context_15m`：唯一的 15m execution 层
- `cross_timeframe_parse`：只表达跨周期关系，不再重复输出价格地图

一句话说：

- **删掉 `shared_levels`，让价格地图只保留一份。**
