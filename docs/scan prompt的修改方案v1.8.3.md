# Scan Prompt & Schema 修改方案 v1.8.3

这版是在 `v1.8.2` 的基础上做一轮很轻的语义微调。

不改 schema 结构，不加新的限制规则，不重写整份 prompt。

目标现在有两个：

- 让 Stage 1 scan 对几个容易被“写重了”的字段，语义更贴近真实市场状态
- 在不减少结构和市场深度的前提下，压短返回文案，减少同层重复

结合 `2026-03-24T10:30:00Z` 和 `2026-03-24T11:00:00Z ETHUSDT` 的真实输出，当前最值得微调的不是结构，而是下面两类问题：

1. 几个关键字段的语义边界
2. 描述字段的文案长度和重复度

其中字段语义上最值得微调的是下面 3 个字段：

1. `sponsorship_state`
2. `trapped_side`
3. `role_observations`

---

## 第一部分：v1.8.3 要解决的真实问题

### 1. `sponsorship_state` 仍然略偏乐观

这次样本里，`1d` 被写成：

- `control_side = buyers`
- `control_clarity = mixed`
- `sponsorship_state = active`

但同一份 scan 里也同时写了：

- price 仍在 `1d PVS value` 内
- 价格仍低于 `AVWAP`
- 上方 `2160-2173` 仍有明显 overhead supply

这说明：

- sponsorship 确实存在
- 但 sponsorship 的质量并不干净

问题不在于模型“看错了方向”，而在于：

- `active` 这个词太像“当前 sponsorship 很顺、很稳”

所以 v1.8.3 要做的，不是加限制，而是把 `sponsorship_state` 的市场语义再说得更细一点。

---

### 2. `trapped_side` 仍然容易被写重

这次样本里，`15m` 和 `4h` 都写了：

- `trapped_side = buyers`

但从输入里更像能确认的是：

- buyers 处于不利位置
- higher price 失去 acceptance
- 当前更 vulnerable

这还不一定等于：

- 已经形成结构性 trapped position
- 已经出现明确 forced unwind 风险

所以 v1.8.3 要进一步说明：

- `trapped_side` 是一种更强的市场状态
- 它不是“弱势方”
- 也不是“当前被压的一方”

---

### 3. `role_observations` 仍然略带 narrative 味

这次输出里的 `role_observations` 整体已经比旧版好很多，但仍有少数句子会往：

- “这是 crowd”
- “这是 higher-timeframe sponsorship”
- “这是 passive liquidity 在做什么”

这种角色叙事上滑。

问题不是 role 框架本身，而是：

- `observed_behavior` 有时还不够 evidence-first

所以 v1.8.3 要把这层语义再收一圈：

- `role_observations` 是观察层，不是市场故事层

---

## 第二部分：v1.8.3 的修改原则

### 1. 不加硬规则

v1.8.3 不做这种改法：

- 不增加 “必须 / 不得 / 至少”
- 不增加额外 gating
- 不强制模型输出更多字段

### 2. 只把字段到底代表什么说得更清楚

也就是：

- `sponsorship_state` 描述的是 sponsorship 的质量
- `trapped_side` 描述的是结构性 entrapment
- `role_observations` 描述的是 evidence-first 的市场观察

### 3. 仍然保持 Stage 1 的职责边界

Stage 1 仍然只负责：

- 扫描市场
- 扫描参与者行为
- 输出客观市场地图

不替 Stage 2 做：

- trade expression
- entry recommendation
- no-trade judgment

### 4. 压短文案，但不压结构

v1.8.3 不通过删 block、删 timeframe、删 cross-market 来缩短输出。

它采用的方式是：

- 压短 `observed_behavior`
- 压短 `reason`
- 压短 `fragility_summary`
- 压短 `main_tension`
- 压短 `main_unresolved_factors`

同时减少同层重复：

- `key_levels.reason` 只写这个 level 的结构身份
- `dominant_*_zone.reason` 只写这个 zone 为什么成立
- `role_observations` 只写观察到的行为，不再顺带做 timeframe summary

---

## 第三部分：关键字段的语义微调

### 3.1 `sponsorship_state`

#### v1.8.2 的方向是对的

`sponsorship_state` 已经不是简单的流量方向字段，而是在描述：

- 当前 observed flow 是否正在被价格和结构承接

#### v1.8.3 要补清楚的地方

`sponsorship_state` 不只是“有没有 sponsorship”，还在描述：

- sponsorship 的质量
- sponsorship 与当前结构位置是否一致

也就是说：

- `active`
  - sponsorship 仍在推动当前 timeframe 的结构延续
  - price 仍在保有与这段 sponsorship 匹配的结构位置

- `fragile`
  - sponsorship 仍可见
  - 但价格已经开始重新回到 value、重新贴近 opposing structure、或尚未完成 clean acceptance

- `fading`
  - sponsorship 的推动能力正在减弱
  - 价格与结构对 sponsorship 的承接已经明显变差

- `unresolved`
  - sponsorship 的支持与削弱证据同时存在
  - 当前还不能干净归类其质量

#### 核心思想

`sponsorship_state` 描述的是：

- flow 被市场接住得怎么样
- 而不是 flow 有没有出现

---

### 3.2 `trapped_side`

#### v1.8.2 的方向也是对的

`trapped_side` 已经不再被定义成：

- 当前不占优的一边

#### v1.8.3 要再补清楚的地方

`trapped_side` 描述的是：

- 某一侧原本占有结构位置
- 但这个位置已经失去
- 并且市场已经显示出 failed acceptance、failed continuation、或越来越被动的持仓处境

它更接近：

- structural entrapment
- position failure
- forced exit risk

而不是：

- simple weakness
- local disadvantage
- overhead resistance
- temporary pressure

#### 核心思想

如果一个侧别只是：

- 位置不够好
- 当前被压制
- 更容易受伤

那更像：

- vulnerable
- pressured
- degraded

而不一定已经是 `trapped_side`。

---

### 3.3 `role_observations`

#### 问题不在 role，而在表达方式

保留：

- `large_directional_flow`
- `higher_timeframe_sponsorship`
- `crowd_behavior`
- `passive_liquidity`

这个框架本身没有问题。

v1.8.3 要微调的是：

- `observed_behavior` 的表达方式

#### 更合适的语义

`role_observations` 应该优先写成：

- 数据支持的市场观察
- 对当前读法有帮助的行为事实

而不是：

- 解释这个角色“想做什么”
- 用一整句 market story 来概括

#### 更理想的风格

更像：

- futures-led sell pressure is dominant in the immediate auction
- spot is not confirming the latest downside push
- overhead passive liquidity is concentrated near 2160-2163

而不是：

- crowd is doing ...
- institutions are trying to ...
- passive liquidity wants to ...

#### 核心思想

`role_observations` 仍然是：

- observation layer

不是：

- participant motive fiction

---

## 第四部分：输出文案风格的微调

### 4.1 哪些字段要压短

以下字段建议统一改成：

- market-native
- evidence-first
- short-form

主要包括：

- `flow_map.role_observations[].observed_behavior`
- `structure_map.dominant_demand_zone.reason`
- `structure_map.dominant_supply_zone.reason`
- `structure_map.key_levels[].reason`
- `validation.fragility_summary`
- `cross_timeframe_structure.main_tension`
- `cross_timeframe_structure.main_unresolved_factors`

### 4.2 建议的表达方式

从：

- 一整句解释
- 一个字段里同时塞事实、判断、结论

改成：

- 短句
- 短语式
- 先写市场事实，再写结构身份

例如：

- `futures-led selling at upper bracket`
- `ask liquidity clustered 2160-2163`
- `4h demand reclaimed after dip below value`
- `PVS/TPO disagreement on 15m value placement`

### 4.3 同层去重原则

同一个 timeframe 里，不要让同一件事重复出现在：

- `role_observations`
- `dominant_*_zone.reason`
- `key_levels.reason`
- `validation.supporting_facts`
- `validation.conflicting_facts`
- `fragility_summary`

更具体地说：

- `key_levels.reason`
  - 只写 level 的结构身份
  - 不再复述完整市场读法

- `dominant_*_zone.reason`
  - 只写 zone 为什么是当前主供给 / 主需求
  - 不再把 validation 里的冲突重复写进去

- `role_observations`
  - 只写观察到的行为
  - 不再顺带总结整个 timeframe

---

## 第四部分：v1.8.3 对提示词的具体修改

这版只改 `FIELD MEANINGS` 和 `BEFORE YOU OUTPUT` 两个局部段落。

### 1. `FIELD MEANINGS` 建议替换为

```text
FIELD MEANINGS

`sponsorship_state`
- This describes whether observed flow is being accepted and structurally carried by price on that timeframe, and how clean or fragile that sponsorship currently is.

`trapped_side`
- Use this when one side has already lost advantageous structural positioning and the market is showing failed acceptance, failed continuation, or increasingly forced exit risk for that side.

`path_map`
- `first_objective_ref` is the nearest structural magnet if the current read continues.
- `first_barrier_ref` is the first meaningful opposing structure likely to resist or degrade that path.
- These are not the same thing.

`role_observations`
- Keep `observed_behavior` evidence-first and market-native.
- Describe observable behavior and market effect, not hidden motives or participant stories.

`validation`
- `supporting_facts` should record the main facts that genuinely support the current read.
- `conflicting_facts` should record materially relevant disagreements that make the read less clean.
- If PVS and TPO disagree in a way that changes the read, that belongs in `conflicting_facts`.
- If spot and futures diverge in a way that changes the read, that also belongs in `conflicting_facts` or `fragility_summary`.
```

### 2. `BEFORE YOU OUTPUT` 建议替换为

```text
BEFORE YOU OUTPUT, VERIFY

- Did you keep each timeframe grounded in observable control, value, structure, flow, and validation?
- If the market is mixed, conflicted, or unclear, did you say so explicitly?
- Did you keep `active_range` tied to the live rotation bracket?
- Does `sponsorship_state` describe the quality of price-accepted flow on that timeframe, rather than just the presence of directional flow?
- If you used `trapped_side`, does it reflect real structural entrapment or growing forced-exit risk, rather than simple weakness or temporary pressure?
- Are `role_observations` written as evidence-first market observations rather than participant stories?
- Does `path_map` clearly distinguish structural objective from structural barrier?
- Did you surface materially relevant PVS/TPO or spot/futures disagreement inside `validation` when they changed the read?
```

---

## 第五部分：是否修改 schema

v1.8.3 不建议改 schema 结构。

如果要同步代码层，只需要：

- 保持 `scan_v1_8_2` 结构不变
- 更新 prompt 文案
- 如有需要，微调少量 `description`

也就是说：

- 这是一次 prompt 语义升级
- 不是 schema 版本升级

---

## 第六部分：一句话结论

`v1.8.3` 的目标不是让 Stage 1 更严格，而是让它更准确地表达：

- sponsorship 的质量
- trapped 的真正市场含义
- role observations 的观察属性

这样可以在不增加限制规则的前提下，让 scan 输出更贴近真实市场状态，也更不容易把局部 vulnerability 误写成更强的市场结论。
