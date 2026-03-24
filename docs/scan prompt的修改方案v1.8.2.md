# Scan Prompt & Schema 修改方案 v1.8.2

这版不是继续加更多校准规则，而是回到第一性原理：

- Stage 1 的问题，不是“模型不够听话”
- 而是几个关键字段的**市场语义定义不够清楚**
- 所以 v1.8.2 的目标不是限制模型，而是把字段到底在市场上代表什么说清楚

核心方向：

1. 保留 `v1.8` 的两个正确方向：
   - `ACTIVE RANGE`
   - `CROSS-MARKET`
2. 不再通过“必须 / 不得 / 至少”式规则去钳制输出
3. 改成：
   - 明确定义字段语义
   - 让模型从底层市场事实自然映射到正确字段

---

## 第一部分：v1.8.2 要解决的真实问题

结合 `2026-03-24T08:45:00Z ETHUSDT` 的真实 `stage1 scan` 输出，当前最值得修的不是“格式”，而是下面这 6 个字段语义问题：

1. `active_range`
- 现在容易被模型理解成 lookback window 的历史极值区间
- 但 Stage 2 真正需要的是：**当前价格正在旋转、接受、拒绝的结构区间**

2. `sponsorship_state`
- 现在容易被理解成“delta 正 + whale flow 正 = active”
- 但真实市场里，**流是否被价格接受** 才决定 sponsorship 的质量

3. `trapped_side`
- 现在容易被写成“当前不占优的一边”
- 但 `trapped` 不是弱势，不是 vulnerability
- `trapped` 是一种更强的市场状态：**先前占位的一侧已经失去结构位置，并且失败接受/失败延续已经出现**

4. `path_map`
- 现在 `objective` 和 `barrier` 的边界容易混
- Stage 2 需要的是：
  - 哪个结构会吸引价格先去测试
  - 哪个 opposing structure 会最先真正阻挡路径

5. `validation`
- 现在更像通用 supporting / conflicting list
- 但它在第一性原理上应承担更具体职责：
  - 记录**当前 read 为什么成立**
  - 以及**什么分歧让这个 read 不干净**

6. `cross_market_snapshot`
- 这不是补一个“额外指标”
- 它的本质是：**spot 与 futures 是否在共同表达同一个市场方向**

---

## 第二部分：v1.8.2 的设计原则

### 1. 不通过限制修改模型

v1.8.2 不使用这种思路：

- “不要三个 timeframe 都是 mixed”
- “必须引用 supporting_context”
- “balanced 也必须给 invalidation”

因为这些都不是第一性原理，只是在外部修正模型输出。

### 2. 通过定义字段语义修改模型

v1.8.2 使用这种思路：

- `active_range` 到底是什么
- `sponsorship_state` 到底在市场上意味着什么
- `trapped_side` 到底是什么状态
- `path_map` 的 `objective` 和 `barrier` 分别是什么
- `validation` 应该承载什么类型的分歧

这样模型不是被“校准”，而是被要求**真正理解字段在市场上的含义**。

### 3. Stage 1 只负责扫描，不负责替 Stage 2 做交易表达

因此 v1.8.2 仍然坚持：

- 不输出 trade advice
- 不输出 best expression
- 不输出 no-trade 结论
- 只输出市场结构、参与者行为、跨周期关系、cross-market 关系

---

## 第三部分：关键字段的语义重定义

### 3.1 `active_range`

#### 正确定义

`active_range` 是：

- 当前价格正在旋转的 bracket
- 当前接受 / 拒绝真正发生的结构区间
- 当前 market state 成立所依赖的活跃边界

#### 不是

`active_range` 不是：

- lookback window 的历史最高 / 最低
- path 上所有 bars 的总体极值
- 一个“越大越完整”的范围

#### 模型应如何理解

模型应优先从这些信息里定义 `active_range`：

- 当前 value area
- 当前 sigma band bracket
- 当前 bracket_board 里的 active / current_inside 结构
- 当前仍然有效的 pivots / acceptance edges

如果 path 里的历史极值已经不属于当前 rotation，就不应进入 `active_range`。

---

### 3.2 `sponsorship_state`

#### 正确定义

`sponsorship_state` 描述的不是“有没有正流”，而是：

- 当前这段 flow 是否真的被价格和结构接受
- sponsorship 是不是仍在推动当前 timeframe 的结构状态

#### 语义边界

- `active`
  - flow 与价格接受一致
  - 当前结构正在被延续，而不是只剩流量表面偏向

- `fragile`
  - 有 sponsorship
  - 但价格重新回到 value、落回关键结构内、或顶在明显 opposing structure 下

- `fading`
  - sponsorship 还没完全消失
  - 但推动能力已经明显减弱

- `absent`
  - 没看到足够 sponsorship

- `unresolved`
  - 支持和削弱证据都存在，当前无法干净归类

#### 核心思想

正 delta、正 whale flow、正 CVD slope，都只是 sponsorship evidence。  
是否 `active`，还要看价格有没有真正接受这个 flow。

---

### 3.3 `trapped_side`

#### 正确定义

`trapped_side` 不是“当前不占优的一侧”，而是：

- 先前占有利位置的一侧
- 已经失去结构位置
- 并且 failed acceptance / failed continuation / forced unwind 风险已经出现

#### 不是

这些情况不应轻易写成 `trapped`：

- 只是弱势
- 只是当前没占优
- 只是上方 / 下方有阻力或支撑
- 只是 vulnerability 提高

#### 模型应如何理解

只有当市场已经显示出：

- 该侧的结构接受失败
- 该侧的延续失败
- 该侧继续持有会越来越被动

这时才应写 `trapped_side`。

否则更适合：

- `none`
- 或 `unclear`

---

### 3.4 `path_map`

#### 正确定义

`path_map` 不是 trade plan。  
它描述的是：**如果当前 read 延续，价格最近会先遇到什么结构。**

#### `first_objective_ref`

`first_objective_ref` 是：

- 当前方向如果延续
- 最近、最自然、最可能先被价格测试的结构磁铁

它更接近：

- 磁铁
- 下一跳
- 最近的结构吸引点

#### `first_barrier_ref`

`first_barrier_ref` 是：

- 当前方向前方
- 第一个真正可能阻滞、拒绝、减速或破坏路径的 opposing structure

它更接近：

- cap
- opposing structure
- 路径上的第一道真正阻碍

#### 边界原则

如果一个 level 本身更像 overhead resistance / opposing wall，  
它通常应优先进入 `barrier`，而不是被写成 `objective`。

---

### 3.5 `validation`

#### 正确定义

`validation` 的职责不是写一些通用的正反 bullets。  
它的职责是：

1. 说明当前 read 为什么成立
2. 说明哪些分歧让这个 read 不够干净

#### `supporting_facts`

这里写：

- 当前 state 的主要确认事实
- 真的在支持当前 read 的结构 / flow / price acceptance

#### `conflicting_facts`

这里写：

- materially relevant 的分歧
- 会改变、削弱、延迟、污染当前 read 的事实

#### 特别重要

以下这类分歧，如果 materially relevant，应优先进入 `validation`：

- `PVS` 与 `TPO` 对当前 value state 的不同表达
- `spot` 与 `futures` 的确认 / 背离
- delta 强但价格未接受
- 价格回到 value、但 flow 仍偏单边

这不是因为要“强制写”，而是因为这些本来就是当前 read 不够干净的核心事实。

---

### 3.6 `cross_market_snapshot`

#### 正确定义

`cross_market_snapshot` 的作用不是补充指标，而是回答：

- 当前 move 是 spot 在带，还是 futures 在带
- spot 和 futures 是共同确认，还是结构上存在分歧

#### 它描述的是

- `spot_premium_state`
  - 当前是 spot premium、futures premium，还是 near flat

- `spot_vs_futures_gap_pct`
  - 当前现货与期货价格差

- `flow_driver`
  - 当前 move 更像由 spot 驱动、futures 驱动，还是两边都没有明显主导

- `latest_4h_delta_relation`
  - 在最新 `4h` 级别，spot 与 futures delta 是 aligned、divergent，还是不够清楚

#### 本质

这是当前市场“谁在表达方向”的客观快照。  
不是 trade bias 字段。

---

## 第四部分：JSON Schema 变更（scan_v1_8_2）

### 版本升级

```diff
- "const": "scan_v1_7"
+ "const": "scan_v1_8_2"
```

### Schema 设计原则

v1.8.2 不做大规模结构重写。  
重点是：

- 保留 `v1.7` 的整体结构
- 保留 `v1.8` 中 `cross_market_snapshot`
- 改写关键字段的 `description`
- 让 schema 语义更清楚

### 关键差异

#### 1. `structure_map.invalidation_level`

保持：

```json
"type": ["number", "null"]
```

但描述改成：

```json
"description": "Use a concrete price only when this timeframe read has an objectively supported directional break anchor. Use null when the read is balanced or structurally unresolved and no single-sided invalidation is cleanly supported."
```

#### 2. `state.sponsorship_state`

描述改成：

```json
"description": "Describe whether observed flow is being accepted and structurally carried by price on this timeframe."
```

#### 3. `flow_map.trapped_side`

描述改成：

```json
"description": "Use trapped_side when one side has already lost advantageous structural positioning and failed acceptance or failed continuation is evident."
```

#### 4. `path_side.first_objective_ref`

描述改成：

```json
"description": "Nearest structural magnet if the current directional read continues from here. This is not necessarily the first opposing barrier."
```

#### 5. `path_side.first_barrier_ref`

描述改成：

```json
"description": "First meaningful opposing structure likely to resist, cap, or degrade the path before further extension."
```

#### 6. `validation_block.conflicting_facts`

描述改成：

```json
"description": "Material disagreements that weaken, delay, or dirty the current read. Use this for meaningful PVS/TPO disagreement, spot/futures divergence, or flow-versus-price acceptance conflict when relevant."
```

#### 7. `cross_timeframe_map.cross_market_snapshot`

保留，并使用：

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["spot_premium_state", "spot_vs_futures_gap_pct", "flow_driver", "latest_4h_delta_relation"],
  "properties": {
    "spot_premium_state": {
      "type": "string",
      "enum": ["spot_premium", "futures_premium", "near_flat"]
    },
    "spot_vs_futures_gap_pct": { "type": "number" },
    "flow_driver": {
      "type": "string",
      "enum": ["futures_led", "spot_led", "balanced", "unclear"]
    },
    "latest_4h_delta_relation": {
      "type": "string",
      "enum": ["aligned", "divergent", "flat_or_unclear"]
    }
  }
}
```

---

## 第五部分：提示词修改（medium_large_opportunity.txt）

### 完整替换稿

```text
You are an expert order-flow market scanner for __SYMBOL__.

Use ONLY the provided scan JSON. Do not invent signals, levels, participant behavior, or cross-timeframe relationships that are not supported by the input.

GOAL
Build an objective multi-timeframe scan of the current market and participant environment for `15m`, `4h`, and `1d`.

If the data leaves the current market state unclear or unresolved, say so directly.

HOW TO READ THE INPUT

Read the input as one live market environment:

- `now` gives current location, value state, active structures, live flow, and momentum snapshot
- `path_newest_to_oldest` shows how price arrived here across timeframes
- `events_newest_to_oldest` reveals initiation, absorption, exhaustion, and divergence behavior
- `supporting_context` gives broader flow, cross-market, and background context
- `raw_overflow` is supplemental detail and should be used only when it materially changes the scan

FIRST-PRINCIPLES OPERATING LENS

Scan the market from the bottom up:

1. Identify observed control on each timeframe.
2. Identify how clear or fragile that control is.
3. Identify where price sits relative to value and the active range.
4. Identify where supply and demand appear concentrated.
5. Identify who is acting aggressively, who is absorbing, and which side appears trapped or forced.
6. Identify whether the move is being sponsored, fading, or unresolved.
7. Identify how `15m`, `4h`, and `1d` relate to each other.
8. Identify whether spot and futures are confirming each other or materially diverging.

Treat the three timeframes as one market viewed at three different horizons:

- `15m` is the immediate auction and near-term control
- `4h` is the active swing environment
- `1d` is the broader regime and outer structure

ACTIVE RANGE

`active_range` means the bracket price is currently rotating within.

It is the live structure where acceptance, rejection, and rotation are actually happening now.

Build it from current structure:
- value areas
- sigma bands
- bracket_board structures
- still-relevant pivots and structural edges

CROSS-MARKET

`cross_market_snapshot` describes whether spot and futures are jointly expressing the current market state or materially diverging.

Use it to describe:
- whether spot or futures is leading
- whether the current premium is on spot or futures
- whether the latest `4h` delta relationship is aligned or divergent

It is an objective market-state snapshot.

FIELD MEANINGS

`sponsorship_state`
- This describes whether observed flow is actually being accepted and structurally carried by price on that timeframe.

`trapped_side`
- Use this when one side has already lost advantageous structural positioning and failed acceptance or failed continuation is evident.

`path_map`
- `first_objective_ref` is the nearest structural magnet if the current read continues.
- `first_barrier_ref` is the first meaningful opposing structure likely to resist or degrade that path.
- These are not the same thing.

`validation`
- `supporting_facts` should record the main facts that genuinely support the current read.
- `conflicting_facts` should record materially relevant disagreements that make the read less clean.
- If PVS and TPO disagree in a way that changes the read, that belongs in `conflicting_facts`.
- If spot and futures diverge in a way that changes the read, that also belongs in `conflicting_facts` or `fragility_summary`.

Use `supporting_context` only when it materially sharpens, weakens, or changes the scan.
Do not cite it mechanically.

OBJECTIVITY RULES

- Prefer observed control over narrative explanation.
- Prefer observable participant behavior over identity stories.
- Prefer explicit uncertainty over forced certainty.
- Prefer structural facts over abstract adjectives.
- Do not encode pullback, reversal, continuation, or best-expression trade advice into fields that are meant to be factual.

OUTPUT

Return JSON only and follow `scan_v1_8_2`.

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

`cross_timeframe_map` must contain:
- `ownership_map`
- `relationship_map`
- `cross_market_snapshot`
- `cross_timeframe_structure`

Keep `relationship_map` factual, not interpretive.
Keep `path_map` structural, not a trade plan.

BEFORE YOU OUTPUT, VERIFY

- Did you keep each timeframe grounded in observable control, value, structure, flow, and validation?
- If the market is mixed, conflicted, or unclear, did you say so explicitly?
- Did you keep `active_range` tied to the live rotation bracket rather than historical extremes?
- Does `sponsorship_state` reflect price-accepted flow rather than delta alone?
- If you used `trapped_side`, does it reflect real structural entrapment rather than simple weakness, overhead resistance, or general vulnerability?
- Does `path_map` clearly distinguish structural objective from structural barrier?
- Did you surface materially relevant PVS/TPO or spot/futures disagreement inside `validation` when they changed the read?

FORMAT

Return JSON only.
Follow the provider schema exactly.
```

---

## 第六部分：需同步修改的代码位置

| 文件 | 修改 |
|------|------|
| `provider.rs` | 新增 `scan_v1_8_2` schema 与校验 |
| `provider.rs` | 更新关键字段 `description` |
| `provider.rs` | 更新 Qwen / custom-llm output contract 为 `scan_v1_8_2` |
| `systems/llm/src/llm/prompt/scan/medium_large_opportunity.txt` | 替换为 v1.8.2 prompt |

---

## 第七部分：一句话结论

`v1.8.2` 不是“更严格的 v1.8.1”，而是：

- 保留 `ACTIVE RANGE`
- 保留 `CROSS-MARKET`
- 通过字段语义定义，而不是外部限制规则，去修正：
  - `trapped_side`
  - `sponsorship_state`
  - `path_map`
  - `validation`

这更符合 Stage 1 的第一性原理职责边界。
