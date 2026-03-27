# LLM层工作流修改方案 v1.3.1

## 文档定位

本文是对 [llm层工作流的修改方案v1.3.0.md](/data/docs/llm层工作流的修改方案v1.3.0.md) 的正式修订版。

如需阅读不依赖旧稿的单一主文档，请优先参考 [llm层工作流的修改方案v1.3.1-完整合并版.md](/data/docs/llm层工作流的修改方案v1.3.1-完整合并版.md)。

使用规则：
- 若 v1.3.1 与 v1.3.0 冲突，以 v1.3.1 为准
- v1.3.1 未明确替换的条款，继续沿用 v1.3.0
- v1.3.1 的目标不是重写整份方案，而是把 v1.3.0 剩余的 5 个实现级缺口落成可执行合同

---

## 版本变更记录

- v1.0.0：初版三层架构设计（代码层 + Stage1 + Stage2），基于 26 个指标
- v1.1.0：新增 i27 options_surface 指标融入，指标总数变为 27 个
- v1.2.0：修复 v1.1.0 复核中发现的 7 个问题
- v1.3.0：修复 v1.2.0 复核中发现的 6 个阻塞点
- v1.3.1：修复 v1.3.0 剩余的 5 个合同级缺口

### v1.3.1 变更内容

1. 补齐 `market_tradeable=false` 的重新进入闭环：`recheck_conditions` 改为结构化谓词，Stage2 每个 cycle 消费；命中后触发紧急刷新
2. 修正 `path_quality` 的 RR 定义与样例数值，新增 `expected_entry_basis` / `expected_entry` / `quality_target_basis` / `quality_target` / `planned_stop_loss`
3. 删除未落地的 `since_activation` 表述，改为明确的事件时序约束 `event_after_precondition`
4. 补全 `execution_intent` 合同：新增 `intent_mode` / `ttl_minutes` / `max_drift_pct` / `zone_mid`
5. 用 `price_location` + 状态层字段替代模糊的 “POC ±30%” 表述，使 `market_tradeable=false` 可机器执行

---

## 第一性原理对齐

基于 [订单流交易员交易流程V1.md](/data/docs/订单流交易员交易流程V1.md)，高质量交易系统必须同时满足：

1. 地图不过时：剧本一旦失效，不能继续拿旧地图开仓
2. 确认发生在对的位置、对的时间、对的顺序
3. 执行层有足够分辨率，但执行不反过来污染决策层
4. 没有 edge 时允许明确不做
5. 开仓前必须证明“到目标的路径”优于“到止损的路径”

v1.3.1 的 5 个修复点，分别对应以上 5 条。

---

## 替换条款 A：`market_tradeable=false` 与重新进入闭环

本节替换 v1.3.0 中与 `market_tradeable=false`、`recheck_conditions`、Stage2 对 no-trade 地图的消费方式相关的条款。

### A.1 Stage1 的 `market_tradeable=false` 判定

删除 v1.3.0 中“POC ±30% range”的表述，改为以下机器化规则：

`market_tradeable = false` 当且仅当以下条件全部满足：

```json
{
  "all": [
    { "field": "price_location.at_edge", "operator": "eq", "expected": false },
    { "field": "price_location.vs_4h_value", "operator": "eq", "expected": "inside" },
    { "field": "price_location.vs_1d_value", "operator": "eq", "expected": "inside" },
    { "field": "ema_regime.4h.regime", "operator": "eq", "expected": "between" },
    { "field": "long_short_ratios.regime", "operator": "in", "expected": ["balanced", "unwind"] },
    { "field": "funding.bias", "operator": "in", "expected": ["neutral", "slightly_long", "slightly_short"] },
    { "field": "vpin.regime", "operator": "in", "expected": ["normal", "suppressed"] },
    { "field": "open_interest.oi_change_4h_pct", "operator": "abs_lt", "value": 2.0 }
  ]
}
```

解释：
- `at_edge=false + vs_4h_value=inside + vs_1d_value=inside`：价格位于 value 中间，不在边缘
- `ema_regime=between`：趋势结构不明确
- `ratio/funding/vpin/oi_change` 没有给出挤仓、恐慌、扩张中的异常状态

只要上述任意一条不满足，`market_tradeable = true`，Stage1 继续构建 paths。

### A.2 Stage1 的 `recheck_conditions`

`recheck_conditions` 不再使用自然语言，统一改为结构化谓词。示例：

```json
{
  "market_tradeable": false,
  "not_tradeable_reason": "price inside 4H and 1D value middle, no edge, no crowding, no toxic flow",
  "recheck_conditions": [
    {
      "id": "r1",
      "field": "price_location.at_edge",
      "operator": "eq",
      "expected": true,
      "reason": "price reached structural edge"
    },
    {
      "id": "r2",
      "field": "long_short_ratios.regime",
      "operator": "in",
      "expected": ["crowded_long", "crowded_short"],
      "reason": "crowding emerged"
    },
    {
      "id": "r3",
      "field": "vpin.regime",
      "operator": "in",
      "expected": ["elevated", "extreme"],
      "reason": "flow toxicity increased"
    },
    {
      "id": "r4",
      "field": "open_interest.oi_change_4h_pct",
      "operator": "abs_gte",
      "value": 2.0,
      "reason": "positioning accelerated"
    }
  ],
  "paths": [],
  "thesis_premises": null,
  "driver_attribution": null
}
```

### A.3 Stage2 对 `market_tradeable=false` 的消费规则

Stage2 每个 15m cycle 必须执行：

1. 如果 `market_tradeable=false`，先检查 `recheck_conditions`
2. 任意一条命中：
   - `request_stage1_refresh = true`
   - `refresh_urgency = "recheck_condition_met"`
   - 本轮不新开仓，等待 Stage1 在下一个 15m cycle 内重画地图
3. 若没有命中：
   - 继续不新开仓
   - 仅做持仓管理

示例输出：

```json
{
  "market_tradeable_check": {
    "market_tradeable": false,
    "recheck_conditions_check": [
      { "id": "r1", "pass": true, "actual": true },
      { "id": "r2", "pass": false, "actual": "balanced" },
      { "id": "r3", "pass": false, "actual": "normal" },
      { "id": "r4", "pass": false, "actual": 0.8 }
    ],
    "request_stage1_refresh": true,
    "refresh_urgency": "recheck_condition_met",
    "reason": "price reached structural edge while previous map was marked not tradeable"
  },
  "decision": "NO_TRADE"
}
```

这条闭环的目标是：
- 没有 edge 时明确不做
- edge 一旦出现，系统不需要等到下一个 4H 才重新看图

---

## 替换条款 B：`path_quality` 的 RR 定义与计算合同

本节替换 v1.3.0 中 `path_quality` 的 RR 定义与示例数值。

### B.1 统一 RR 公式

`rr_ratio` 的定义统一为：

```text
rr_ratio = abs(quality_target - expected_entry) / abs(expected_entry - planned_stop_loss)
```

其中：
- `expected_entry`：Stage1 预估的最可能成交价，不是 activation 边界本身
- `quality_target`：用于路径质量判断的目标位，不必等于 `first_path_target_mid`，但必须显式声明
- `planned_stop_loss`：执行层应使用的止损价，不得缺省

### B.2 `path_quality` 必填字段

每条 path 的 `path_quality` 必须至少包含：

```json
{
  "expected_entry_basis": "activation_mid | entry_zone_mid | explicit_retest_level",
  "expected_entry": 0.0,
  "quality_target_basis": "first_path_target_mid | next_path_target_mid | explicit_quality_target",
  "quality_target": 0.0,
  "planned_stop_loss": 0.0,
  "reward_distance_pct": 0.0,
  "risk_distance_pct": 0.0,
  "rr_ratio": 0.0,
  "obstacles_to_target": [],
  "obstacles_to_stop": [],
  "path_resistance": "low | medium | high",
  "quality_note": ""
}
```

计算规则：

```text
reward_distance_pct = abs(quality_target - expected_entry) / expected_entry * 100
risk_distance_pct   = abs(expected_entry - planned_stop_loss) / expected_entry * 100
rr_ratio            = reward_distance_pct / risk_distance_pct
```

### B.3 Stage2 的最终重算规则

Stage1 的 `path_quality` 是地图级粗估。

Stage2 在输出 `execution_intent` 前，必须基于最终的 `entry_zone` 或 `expected_entry` 再重算一次：
- 若 `intent_mode = immediate`，可用当前可执行价附近重算
- 若 `intent_mode = pullback`，用 `entry_zone_mid` 重算
- 若 `intent_mode = breakout`，用 breakout trigger price 重算

若 Stage2 重算后的 `rr_ratio` 低于门槛，以 Stage2 结果为准。

### B.4 修正样例

#### Path A 样例修正

```json
{
  "id": "path_a",
  "entry_side": "SHORT",
  "first_path_target": { "low": 2040, "high": 2042 },
  "next_path_target": { "low": 2033, "high": 2035 },
  "failure_level": { "low": 2063, "high": 2068 },
  "path_quality": {
    "expected_entry_basis": "explicit_retest_level",
    "expected_entry": 2049.5,
    "quality_target_basis": "next_path_target_mid",
    "quality_target": 2034.0,
    "planned_stop_loss": 2055.8,
    "reward_distance_pct": 0.76,
    "risk_distance_pct": 0.31,
    "rr_ratio": 2.45,
    "obstacles_to_target": [],
    "obstacles_to_stop": ["4h broken support reclaim 2055-2056"],
    "path_resistance": "low",
    "quality_note": "second-leg breakdown trade; stop uses failed breakdown reclaim, not full thesis outer boundary"
  }
}
```

#### Path B 样例修正

```json
{
  "id": "path_b",
  "entry_side": "LONG",
  "entry_zone": { "low": 2039.0, "high": 2046.5 },
  "first_path_target": { "low": 2064, "high": 2068 },
  "failure_level": { "low": 2030, "high": 2033 },
  "path_quality": {
    "expected_entry_basis": "entry_zone_mid",
    "expected_entry": 2042.75,
    "quality_target_basis": "first_path_target_mid",
    "quality_target": 2066.0,
    "planned_stop_loss": 2032.0,
    "reward_distance_pct": 1.14,
    "risk_distance_pct": 0.53,
    "rr_ratio": 2.15,
    "obstacles_to_target": ["bearish FVG 2050-2058", "AVWAP session_open 2066.18"],
    "obstacles_to_stop": [],
    "path_resistance": "medium",
    "quality_note": "reward exceeds risk by >2x; medium path resistance is acceptable"
  }
}
```

---

## 替换条款 C：删除 `since_activation`，改为事件时序约束

本节替换 v1.3.0 中所有 `since_activation` 表述。

### C.1 删除项

删除以下概念：
- `since_activation`

原因：
- 该字段在 v1.3.0 中被承诺，但没有真正进入 schema
- “事件必须发生在当前这次互动之后”的需求是对的，但应以明确的时序约束表达，而不是模糊命名

### C.2 新增时序约束：`event_after_precondition`

在带有 `precondition` 的 path 里，`trigger_events` 可声明：

```json
{
  "requires": {
    "precondition": "price_visited_below",
    "precondition_level": 2035,
    "event_after_precondition": true,
    "trigger_events": {
      "any": [
        {
          "event_type": "exhaustion",
          "subtype": "selling",
          "max_age_minutes": 60,
          "near_level": { "reference": 2035, "max_distance_pct": 0.5 }
        }
      ]
    }
  }
}
```

语义：
- `precondition_satisfied_at`：最近一次满足 precondition 的时间点
- `event_after_precondition = true`：事件的 `confirmed_at` 必须大于等于 `precondition_satisfied_at`

这解决的问题是：
- 不是任何“近期且附近”的 exhaustion 都可用
- 必须是“当前这次 price visit / flush / reclaim”之后出现的确认，才算当前 setup 的触发

### C.3 Stage2 评估顺序

Stage2 对带 precondition 的激活谓词，按以下顺序评估：

1. 找到最近一次满足 precondition 的时间 `precondition_satisfied_at`
2. 对候选 trigger event 检查：
   - `confirmed_at` 是否在 `max_age_minutes` 内
   - `confirmed_price` 是否满足 `near_level`
   - 若 `event_after_precondition=true`，则 `confirmed_at >= precondition_satisfied_at`
3. 三者都满足，事件才算有效

### C.4 修正激活样例数值错误

将 v1.3.0 中错误样例：

```json
{
  "price_above_on_close_15m": 2051,
  "current_close": 2050.5,
  "met": true
}
```

替换为：

```json
{
  "price_above_on_close_15m": 2051,
  "current_close": 2051.5,
  "met": true
}
```

若保留 `current_close=2050.5`，则 `met` 必须为 `false`。

---

## 替换条款 D：`execution_intent` 合同补全

本节替换 v1.3.0 中 `execution_intent` 与执行引擎输入语义。

### D.1 `execution_intent` 必填字段

Stage2 输出的 `execution_intent` 必须包含：

```json
{
  "side": "LONG | SHORT",
  "intent_mode": "pullback | breakout | immediate",
  "entry_zone": { "low": 0.0, "high": 0.0 },
  "zone_mid": 0.0,
  "stop_loss": 0.0,
  "take_profit_1": 0.0,
  "take_profit_2": 0.0,
  "leverage": 0.0,
  "path_id": "path_x",
  "ttl_minutes": 15,
  "max_drift_pct": 0.35,
  "sizing_note": ""
}
```

### D.2 `intent_mode` 语义

- `pullback`
  - 当前价可以在 `entry_zone` 外
  - 执行引擎等待价格回到 `entry_zone` 内执行
  - 适用于 reclaim 后等 retest、break 后等回踩

- `breakout`
  - 执行引擎等待价格突破 `entry_zone` 上沿或下沿的 trigger side 后执行
  - 适用于突破接受类 setup

- `immediate`
  - 当前价已处于 `entry_zone` 内
  - 执行引擎立即按内部规则下单

### D.3 `ttl_minutes` 与 `max_drift_pct`

- `ttl_minutes`
  - 执行意图的最大存活时间
  - 超时未成交则自动取消

- `max_drift_pct`
  - 当前价相对 `zone_mid` 的最大允许偏离
  - 若偏离超出该阈值，说明 setup 节奏已经改变，取消该 intent

### D.4 执行引擎取消规则

执行引擎取消 `execution_intent` 的条件改为：

1. 超过 `ttl_minutes`
2. 当前价相对 `zone_mid` 的偏离超过 `max_drift_pct`
3. 对于 `pullback`：
   - 价格持续远离 `entry_zone` 且未回踩
4. 对于 `breakout`：
   - 价格在 trigger 方向失效，重新回到 zone 的另一侧
5. Stage2 发出新的 intent 覆盖旧 intent

不再使用“当前价格不在 zone 内就取消”这种过于粗糙的规则。

### D.5 修正 `execution_intent` 样例

```json
{
  "decision": "LONG",
  "execution_intent": {
    "side": "LONG",
    "intent_mode": "pullback",
    "entry_zone": { "low": 2039.0, "high": 2046.5 },
    "zone_mid": 2042.75,
    "stop_loss": 2032.0,
    "take_profit_1": 2064.0,
    "take_profit_2": 2075.0,
    "leverage": 2,
    "path_id": "path_b",
    "ttl_minutes": 15,
    "max_drift_pct": 0.35,
    "sizing_note": "normal size; pullback retest entry"
  }
}
```

该样例的含义是：
- Stage2 已确认“该做多”
- 但不是立刻追价，而是等待价格回踩到 `entry_zone`
- 若 15 分钟内没回踩，或价格相对 `zone_mid` 漂移过远，则取消该次执行意图

---

## 替换条款 E：路径质量门槛与 Stage2 最终判定

本节补强 v1.3.0 中的 `path_quality_check`。

### E.1 Stage2 最终开仓顺序

Stage2 的新开仓顺序调整为：

1. `market_tradeable` 检查
2. `thesis_validity_check`
3. path activation / failure 评估
4. trigger checklist
5. hard gate
6. soft gate
7. `path_quality_check`
8. 生成 `execution_intent`

只有前 7 步全部通过，才允许生成 `execution_intent`。

### E.2 `path_quality_check` 输出格式

```json
{
  "path_quality_check": {
    "path_id": "path_b",
    "expected_entry_basis": "entry_zone_mid",
    "expected_entry": 2042.75,
    "quality_target_basis": "first_path_target_mid",
    "quality_target": 2066.0,
    "planned_stop_loss": 2032.0,
    "reward_distance_pct": 1.14,
    "risk_distance_pct": 0.53,
    "rr_ratio": 2.15,
    "effective_obstacles": 2,
    "path_resistance": "medium",
    "sizing_adjustment": "normal_size",
    "pass": true
  }
}
```

### E.3 路径质量门槛保持不变，但以重算后的数值为准

- `rr_ratio < 1.0`：不做
- `1.0 <= rr_ratio < 1.5`：仅在其他条件极强时允许，且必须减仓
- `rr_ratio >= 1.5`：进入 `path_resistance` 规则

若 Stage2 重算结果与 Stage1 的 `path_quality` 不一致，以 Stage2 的重算结果为准。

---

## 实现清单

v1.3.1 对实现层的新增要求：

1. 代码层
   - 为所有事件统一输出 `confirmed_price`
   - 为 `price_location` 输出 `at_edge` / `edge_type`

2. Stage1
   - 输出结构化 `recheck_conditions`
   - 输出 `market_tradeable=false` 时的 no-trade 地图
   - 输出 `path_quality` 的完整合同字段

3. Stage2
   - 每 cycle 消费 `recheck_conditions`
   - 支持 `event_after_precondition`
   - 在生成 `execution_intent` 前重算 `path_quality`
   - 输出补全后的 `execution_intent`

4. 执行引擎
   - 支持 `intent_mode`
   - 支持 `ttl_minutes`
   - 支持 `max_drift_pct`

---

## 最终结论

v1.3.1 的定位是：
- 保留 v1.3.0 已经建立起来的三层 + 执行引擎架构
- 修掉 `market_tradeable` 闭环、RR 数学合同、事件时序合同、执行意图合同这四个最后的实现级缺口

若 v1.3.1 落地实现，并通过：
- 多行情类型理论回放
- shadow mode 对比验证
- path quality / thesis validity / execution fill 质量评估

则可以认为这套 llm 层工作流设计，已经达到“按 [订单流交易员交易流程V1.md](/data/docs/订单流交易员交易流程V1.md) 做高质量交易”的架构充分性要求。
