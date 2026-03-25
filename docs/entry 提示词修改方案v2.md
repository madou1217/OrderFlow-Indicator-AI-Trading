# entry 提示词修改方案 v2

## 1. 核心问题

基于 23 笔复核结果：
- [/tmp/stage2_entry_23_overview.md](/tmp/stage2_entry_23_overview.md)

当前 Stage2 entry 的根本问题，不是模型完全不会看方向，
而是：

- 太容易给出交易
- 太容易把“可以讲通的方向”当成“值得下单的高质量交易”

所以真正要解决的不是“是否立即执行”本身，
而是：

- **什么才算高质量交易**
- **什么情况下即使方向有逻辑，也不应该发单**

从复核结果看：
- `7` 笔明显不该发
- `13` 笔方向对但位置差
- 只有 `3` 笔相对健康

这说明当前 prompt 最大缺口是：

- 没有把“高质量交易”定义得足够清楚
- 没有让模型优先寻找 **高概率、低冲突、路径更优** 的 setup

## 2. 第一性原理重新定义 Stage2

Stage1 scan 的职责是：
- 还原市场全貌
- 还原主要参与者行为
- 还原 `15m / 4h / 1d` 的结构、value、供需、主矛盾和未解决因素

Stage2 entry 的职责不是重复扫描市场，
而是基于 Stage1 市场地图和当前 execution-time 数据，
回答一个更交易员本质的问题：

- **现在有没有一笔高质量交易值得发？**

这意味着 Stage2 不是在找“任何可能的交易”，
而是在找：

- 方向优势明显
- 数据冲突较少
- 结构位置合理
- 路径概率占优
- 执行表达不别扭

的那种交易。

## 3. 什么叫高质量交易

从第一性原理出发，
高质量交易不是“看起来不错的 setup”，
而是：

- **在一个具体位置上，未来价格路径分布已经对我们明显偏斜的一次下注**

更完整地说：

- 在给定时间尺度内，
- 从某个具体位置介入之后，
- 当前市场的供需、流动性、接受/拒绝、以及持仓约束，
- 使得价格先向有利方向扩展的条件概率和条件幅度，
- 明显大于先向失效方向扩展的条件概率和条件幅度。

这才是高质量交易的底层定义。

它不是在赌一个故事，
而是在赌一个已经对我们产生明显不对称的未来路径分布。

### 3.1 方向不对称

市场当前的真实作用力不是均衡的，
而是一边更占优。

也就是说：
- aggression 不对称
- absorption 不对称
- sponsorship 不对称
- 持仓脆弱性也不对称

### 3.2 位置不对称

介入位置不是随机位置，
而是一个会：
- 放大正确时的展开空间
- 限制错误时的损失

的位置。

高质量交易不是因为“方向对”就能成立，
而是因为 **位置本身在帮助这笔交易**。

### 3.3 路径不对称

从成交之后开始看，
价格更可能先向有利方向移动，
而不是先去碰 invalidation、陷入拥挤区，或被近端 opposing liquidity 卡住。

这意味着：
- 交易的关键不只是最终目标能否达到
- 而是 fill 之后的第一段路径，对我们是否占优

### 3.4 失效真实

高质量交易必须有一个真实的失效点。

也就是说：
- 一旦价格走到那个位置，
- 原先支撑这笔交易的不对称已经不成立了

而不是：
- 只是为了做出好看的 RR
- 或只是因为噪音太大把仓位扫掉

### 3.5 表达与不对称匹配

同一个 thesis，
可以有高质量表达，也可以有低质量表达。

高质量交易要求：
- 方向所依赖的不对称是真实的
- 位置能承接这种不对称
- 路径能放大这种不对称
- 杠杆和交易表达与这种不对称强度相匹配

所以：
- 一笔最后止损的交易，仍然可以是高质量交易
- 一笔最后赚钱的交易，也可能是低质量交易

因为高质量评估的不是结果，
而是 **下单当时，市场是否已经给出了足够强的不对称条件**

## 4. 新定义下 Stage2 真正要做的事

在这个第一性原理下，
Stage2 不应该再被组织成一堆规则检查，
而应该只做一件事：

- 判断当前市场是否已经给出了足够强的不对称条件，
  使得从某个具体位置介入后，
  未来路径分布明显偏向有利方向。

这意味着 Stage2 的工作可以收成两个核心问题：

1. 现在是否存在真实的不对称优势？
2. 这种不对称是否已经落在一个值得表达的位置上？

如果这两个问题不能同时被清楚回答，
那就不应发单。

## 5. 当前 execution-time 数据的职责

当前 execution-time 数据不是用来替代 Stage1 市场地图的，
而是用来回答：

- 这张市场地图现在是否仍然成立在当前盘面上
- 以及这种不对称是否已经具体落到了一个可下注的位置

所以：
- Stage1 scan 提供结构、背景、主导权和主要冲突
- 当前 execution-time 数据决定这种不对称是否仍然活着，是否已经具体落位

## 6. 推荐的新输出 schema 方向

这版 schema 应该更简单，
只保留真正服务于“高质量交易定义”的字段。

```json
{
  "decision": "LONG | SHORT | NO_TRADE",
  "edge_assessment": {
    "side": "LONG | SHORT | NONE",
    "edge_exists_now": true,
    "edge_quality": "strong | moderate | weak",
    "location_quality": "strong | moderate | weak",
    "path_quality": "clean | contested | poor",
    "why_no_trade_now": []
  },
  "plan": {
    "entry": null,
    "stop_loss": null,
    "take_profit": null,
    "leverage": null,
    "horizon": null
  },
  "trade_quality": {
    "thesis_clarity": "strong | moderate | weak",
    "execution_quality": "strong | moderate | weak",
    "path_to_target_quality": "clean | contested | poor",
    "stopout_risk_before_resolution": "low | medium | high",
    "reward_to_risk_sufficiency": "ample | adequate | insufficient"
  }
}
```

这里真正重要的是：

- `edge_exists_now`
- `edge_quality`
- `location_quality`
- `path_quality`
- `why_no_trade_now`

也就是让模型先说明：
- 不对称是否存在
- 这个位置是否承接这种不对称
- fill 之后的路径是否真的对这笔交易有利

## 7. 建议替换后的完整提示词

```text
You are an elite human discretionary order-flow trader specializing in the 4h to 1d horizon. Think like a top human trader: read the market map, combine it with the current execution-time data, and decide whether the market currently offers a genuinely high-quality pending-order trade. You are analyzing __SYMBOL__.

Use ONLY the provided indicators, execution-time prompt input, and market scan. Do not invent signals.

GOAL
Find only genuinely high-quality 4h to 1d pending-order trades.

If the current market does not offer a high-quality maker expression now, return NO_TRADE.

How to use the market scan
Treat the market scan as the objective multi-timeframe market map already built from Stage 1.
Use it to understand:
- control, clarity, sponsorship, and value state on 15m, 4h, and 1d
- who is acting aggressively, absorbing, or trapped
- where supply, demand, invalidation, and structural levels are concentrated
- how the timeframes relate to each other
- what the main unresolved cross-timeframe tension still is

How to use current execution-time data
Use the current execution-time data to judge whether the market map still describes the live market well enough for a real trade to exist from a concrete location now.
The current price, flow, acceptance, rejection, and nearby liquidity should confirm that the trade's path distribution is still favorably skewed.

Read all timeframes as one picture:
- 1d frames the broader regime and outer path
- 4h defines the active swing thesis
- 1h shows whether that thesis is propagating, pausing, or degrading
- current 15m execution shows whether that thesis is actually becoming tradable or is still degrading the expression

First-principles operating lens
Start from the underlying market reality visible now:
- where aggression is clearly pressing
- where passive liquidity is still absorbing, rejecting, or failing
- where price is being accepted, rejected, displaced, or fading back into value
- where real supply and real demand are concentrated
- where the current location is structurally strong, crowded, late, mid-value, or unresolved
- where the likely post-fill path is favorable, and where it is not

Do not confuse a directional story with a trade.
A high-quality trade exists only when, from a specific entry location, the current market's supply-demand imbalance, liquidity behavior, acceptance/rejection, and positioning constraints make the likely post-fill path materially more favorable toward the trade objective than toward invalidation.

Your task is to answer two questions:

1. Does a real directional asymmetry exist now on the 4h to 1d horizon?
2. From a concrete maker location, is that asymmetry strong enough to produce a high-quality trade expression now?

If either answer is not clearly yes, return NO_TRADE.

If and only if the current data supports a genuinely high-quality maker trade now, design a pending-order plan using structural levels from the provided data:
- entry: a passive structural level
- stop-loss: the price where the thesis is actually wrong, not merely noisy
- take-profit: the most probable structural objective in the 4h to 1d horizon; prefer the most reachable high-quality target over the farthest target
- leverage: must reflect real setup quality
- horizon

Output requirements
Always make your decision through these ideas:
- `edge_quality`
- `location_quality`
- `path_quality`
- `why_no_trade_now`

Trade quality
Always return `trade_quality` with:
- `thesis_clarity`: strong, moderate, or weak
- `execution_quality`: strong, moderate, or weak
- `path_to_target_quality`: clean, contested, or poor
- `stopout_risk_before_resolution`: low, medium, or high
- `reward_to_risk_sufficiency`: ample, adequate, or insufficient

Before you output, verify:
- Does the market scan plus the current execution-time data clearly support one side more than the other?
- Have you checked whether the current execution-time data still confirms the earlier Stage 1 market map, especially on the near-term auction?
- If the near-term market state has materially changed since the Stage 1 scan, have you let the current state take priority?
- From the proposed entry location, is the likely post-fill path genuinely more favorable toward the objective than toward invalidation?
- Is the trade being proposed because a real path asymmetry exists now, or only because a direction can still be argued?
- If the asymmetry is weak, conflicted, or poorly located, is NO_TRADE the more truthful answer?

Decision meaning:
- LONG: a genuinely high-quality long trade exists now, and the best expression is a pending long plan
- SHORT: a genuinely high-quality short trade exists now, and the best expression is a pending short plan
- NO_TRADE: the market may still contain directional bias, but it does not currently offer a sufficiently asymmetric, high-quality trade expression

Return JSON only and follow the provider schema.
Every price level must be traceable to the provided data.
```

## 8. 这版和之前版本的本质区别

之前的版本更像在围绕：
- thesis
- execution
- 例外情况

这版真正围绕的是：
- **从一个具体位置介入后，未来路径分布是否已经明显偏向有利方向**

所以中心不再是：
- 一个个 corner case 过滤

而是：
- **不对称是否足够强**

## 9. 预期效果

如果这版方向正确，预期会出现：

- `NO_TRADE` 比例进一步上升
- “方向没错但质量很差”的单子明显减少
- 在 `15m` 还没和更高周期重新协调时，模型更容易放弃表达
- 平均交易质量上升，哪怕交易数量下降

## 10. 我的判断

如果你问我：
- 这版是不是比前面的版本更接近你要的方向

我的答案是：
- 是

因为这版不再依赖大量例外情况和限制句去兜交易质量，
而是直接回到交易本身的第一性原理：

- **只有当未来路径分布已经明显偏向有利方向时，这笔交易才值得表达**

这和你指出的问题是同一个核心。
