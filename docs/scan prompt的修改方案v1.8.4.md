# Scan Prompt & Schema 修改方案 v1.8.4

这版不再沿着“给 Stage 1 增加更多 gate”去修。

核心出发点只有一个：

- **Stage 1 scan 负责扫描市场与主要参与者意图**
- **Stage 2 entry / pending / management 负责基于 Stage 1 市场地图和实时数据做交易决策**

所以这版要解决的问题不是：

- 如何让 Stage 1 更像一个更严格的判定器

而是：

- 如何让 Stage 1 的输出更贴合自己的职责边界
- 如何删掉超出 Stage 1 功能范围的内容
- 如何保留真正对 Stage 2 有价值的市场全貌

---

## 第一部分：基于职责边界的评估结论

按第一性原理看，Stage 1 应该回答的是：

- 当前各 timeframe 的市场状态是什么
- 谁在主动推动
- 谁在吸收
- 供需和 value 在哪里
- 哪些跨周期关系和分歧仍然存在
- 主要参与者意图和市场结构是否一致

Stage 1 不应该替 Stage 2 回答的是：

- 价格下一步最可能先走到哪里
- 路径上先碰到哪个 barrier
- 当前 read 的“最近目标”是什么

这些已经开始进入：

- path expression
- route selection
- trade construction

也就是更接近 Stage 2 的职责。

---

## 第二部分：当前 schema 里哪些该删，哪些该留

### 1. 建议删除：`structure_map.path_map`

这是当前最明确超出 Stage 1 职责边界的字段。

原因不是它“完全没用”，而是：

- 它要求 Stage 1 开始表达路径
- 它要求 Stage 1 开始决定 objective / barrier
- 它天然更接近交易表达，而不是市场扫描

如果 Stage 2 的职责是：

- 读取 Stage 1 市场地图
- 结合实时数据判断当前该怎么表达

那么 `path_map` 本来就应该由 Stage 2 去思考，而不是由 Stage 1 预先代做。

#### 一句话定性

`path_map` 不是坏字段。  
但它更像 **Stage 2 决策辅助字段**，不是 **Stage 1 市场扫描字段**。

所以这版建议：

- **直接删除 `path_map`**

---

### 2. 建议保留：`state`

包括：

- `control_side`
- `control_clarity`
- `value_location`
- `range_state`
- `sponsorship_state`

这组字段仍然是 Stage 1 的核心。

因为它们回答的是：

- 当前盘面状态是什么

不是：

- 当前该怎么交易

---

### 3. 建议保留：`flow_map`

包括：

- `aggressive_side`
- `absorption_side`
- `trapped_side`
- `role_observations`

这组字段本质上就在描述：

- 主要参与者行为
- 参与者意图在盘面上的体现

这正是 Stage 1 的职责。

需要优化的是语义和输出风格，不是删掉它们。

---

### 4. 建议保留：`structure_map` 的其余部分

保留：

- `active_range`
- `range_width_vs_atr`
- `dominant_demand_zone`
- `dominant_supply_zone`
- `key_levels`
- `invalidation_level`

原因是这些都属于：

- 市场结构本身
- 供需分布本身
- 结构失效位置本身

它们不是交易计划。

其中：

- `invalidation_level`
  也建议保留

因为它表达的是：

- 这个 timeframe 的结构 read 在哪里被破坏

这仍然是市场地图的一部分，不等于 Stage 2 的 stop-loss。

---

### 5. 建议保留：`validation`

包括：

- `read_basis`
- `supporting_facts`
- `conflicting_facts`
- 3 个 alignment check
- `fragility_summary`

这组字段不冗余。

它们的价值在于：

- 让 Stage 2 知道这份 scan 为什么这么写
- 让 Stage 2 知道哪里不干净、哪里有冲突

所以它们不是“多余解释”，而是：

- 扫描层的不确定性说明

---

### 6. 建议保留：`cross_timeframe_map`

包括：

- `ownership_map`
- `relationship_map`
- `cross_market_snapshot`
- `cross_timeframe_structure`

这组字段虽然有一定摘要层重复，但我建议继续保留。

原因是：

- 它们帮助 Stage 2 不必自己再重建一次跨周期关系
- 它们提供的是“市场全貌压缩摘要”
- 它们仍然在描述市场，而不是在做交易建议

其中：

- `ownership_map`
  虽然是摘要，但仍然值得保留

因为它让 Stage 2 一眼看到：

- broader regime owner
- active swing owner
- immediate owner

这层摘要收益，大于那一点摘要重复。

---

## 第三部分：v1.8.4 的最终修改方向

### 1. Schema 结构修改

只做一个真正的结构改动：

- **删除 `structure_map.path_map`**

其余结构：

- 不删
- 不新增
- 不再为了修个别字段去加 gate

因为这次的目标不是让 Stage 1 更严格，而是让它更像 Stage 1。

---

### 2. Prompt 语义修改

这版 prompt 修改也只围绕职责边界展开：

- 不再让 Stage 1 组织路径
- 不再让 Stage 1 给出 objective / barrier
- 继续让 Stage 1 聚焦：
  - market state
  - participant behavior
  - supply / demand
  - value
  - structural invalidation
  - cross-timeframe tension

---

## 第四部分：对提示词的具体修改建议

### 1. `FIELD MEANINGS` 建议替换为

```text
FIELD MEANINGS

`sponsorship_state`
- This describes whether observed flow is being accepted and structurally carried by price on that timeframe, and how clean or fragile that sponsorship currently is.

`trapped_side`
- Use this when one side has already lost the structural position it depended on and the market is showing failed acceptance, failed continuation, or growing forced-exit risk for that side.

`range_state`
- This describes how price is interacting with the current `active_range`.
- `testing_range_high` and `testing_range_low` should reflect real interaction with the active range edge, not merely being in the upper half or lower half of the range.

`role_observations`
- Keep `observed_behavior` evidence-first and market-native.
- Describe observable behavior and market effect, not hidden motives or participant stories.

`validation`
- `supporting_facts` should record the main facts that genuinely support the current read.
- `conflicting_facts` should record materially relevant disagreements that make the read less clean.
- If PVS and TPO disagree in a way that changes the read, that belongs in `conflicting_facts`.
- If spot and futures diverge in a way that changes the read, that also belongs in `conflicting_facts` or `fragility_summary`.
```

### 2. `OUTPUT` 建议替换为

```text
OUTPUT

Return JSON only and follow `scan_v1_8_4`.

Top level:
- `schema_version`
- `meta`
- `timeframes`
- `cross_timeframe_map`

For each timeframe return:
- `state`
- `flow_map`
- `structure_map`
- `validation`

`structure_map` is for:
- `active_range`
- `range_width_vs_atr`
- `dominant_demand_zone`
- `dominant_supply_zone`
- `key_levels`
- `invalidation_level`

`cross_timeframe_map` must contain:
- `ownership_map`
- `relationship_map`
- `cross_market_snapshot`
- `cross_timeframe_structure`

Keep `relationship_map` factual, not interpretive.
```

### 3. `BEFORE YOU OUTPUT` 建议替换为

```text
BEFORE YOU OUTPUT, VERIFY

- Did you keep each timeframe grounded in observable control, value, structure, flow, and validation?
- Are your fields describing the market state they are meant to describe, rather than a looser or stronger version of that state?
- Are `state`, `flow_map`, `structure_map`, and `validation` internally consistent with each other on each timeframe?
- Does `active_range` reflect the live auction bracket, and does `range_state` reflect how price is actually interacting with that bracket?
- Do `sponsorship_state` and `trapped_side` describe real structural conditions rather than simple directional pressure or temporary weakness?
- Did you preserve materially relevant disagreement and uncertainty when the market picture is not fully aligned?
- Are `role_observations`, `supporting_facts`, and `conflicting_facts` evidence-first, non-redundant, and free of participant-story fiction?
```

---

## 第五部分：Schema 变更建议

### 版本号

```diff
- scan_v1_8_2
+ scan_v1_8_4
```

### 结构变更

从 `structure_map` 中删除：

```diff
- path_map
```

也就是说，`structure_map` 最终保留：

```json
{
  "active_range": {},
  "range_width_vs_atr": "",
  "dominant_demand_zone": {},
  "dominant_supply_zone": {},
  "key_levels": [],
  "invalidation_level": 0
}
```

同时删除与 `path_map` 相关的 schema 定义：

- `path_map`
- `path_side`
- `first_objective_ref`
- `first_barrier_ref`
- `level_reference`

---

## 第六部分：一句话结论

如果严格按你的职责划分：

- **Stage 1 scan 负责市场与主要参与者意图**
- **Stage 2 负责交易表达与执行判断**

那么当前最值得删掉、也最应该删掉的字段就是：

- **`path_map`**

其余核心结构不建议再删。

因为它们仍然属于：

- 市场状态
- 参与者行为
- 供需结构
- 跨周期关系

这些都还是 Stage 1 的正当职责范围。  
