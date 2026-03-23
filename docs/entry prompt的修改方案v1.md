# Entry Prompt 修改方案 v1

**状态**：草案（待复核）
**适用阶段**：LLM Stage 2 `entry`
**目标**：让 entry 只服务一个目标：

> **在当前信息下，只构建一笔从 4h 到 1d 维度看具有明显正期望、且大概率值得参与的交易。**

---

## 一、问题定义

当前 entry prompt 的实际目标函数更接近：

- 在当前结构里找到一个 dominant opportunity
- 只要能画出 coherent pending-order plan，就倾向给出交易
- 把 `NO_TRADE` 放在“没有 thesis”或“没有 plan”的兜底位置

这会让模型更像：

- 候选交易计划生成器

而不是：

- 高质量交易机会选择器

这会带来一个典型问题：

- 模型经常能给出一笔“结构上讲得通”的单
- 但这笔单不一定明显优于不交易
- 最后要靠 `V gate` / `RR gate` 替模型完成“这笔不值得做”的筛选

换句话说，当前系统并不是“看不懂交易”，而是：

- **太容易把“可以表达的交易”当成“值得参与的交易”**

---

## 二、从日志观察到的现象

以 `2026-03-22 05:03 UTC` 到当前这段样本为例，entry 最终响应共 **8 笔**：

- `SHORT`：5
- `LONG`：3
- `NO_TRADE`：0

这 8 笔后续结果：

- 真正执行：3
- 被 `V gate` 拦截：4
- 被 `RR gate` 拦截：1

也就是：

- **8 笔里 0 笔 `NO_TRADE`**
- **5 笔需要靠执行层 gate 拦掉**

这说明当前 prompt 很可能在系统性地驱动模型：

- “找出当前最好的交易表达”

而不是：

- “先判断这笔交易是否明显值得做，再决定是否输出计划”

### 2.1 更细一点的观察

这批样本里，确实存在几笔质量较高的交易：

- `2026-03-22 11:50 SHORT`
- `2026-03-22 13:20 SHORT`

但也存在多笔典型的边际单：

- `2026-03-22 13:04 LONG`
- `2026-03-22 23:49 SHORT`
- `2026-03-23 02:50 LONG`
- `2026-03-23 03:06 LONG`
- `2026-03-23 03:28 SHORT`

这些单往往具备：

- 有 thesis
- 有结构位
- 有 entry / sl / tp

但并不具备：

- 明显优于不交易的 forward edge

---

## 三、第一性原理目标函数

entry 不应默认“找到一笔交易”。

它真正要回答的问题应该是：

1. 当前结构是否存在一个清晰的 4h 到 1d 方向 edge？
2. 这个 edge 是否已经具备可执行的入场结构，而不是只有方向观点？
3. 这笔交易从现在开始的 reward-to-risk 和 path quality，是否明显优于“不交易”？

因此 entry 的目标不是：

- 返回当前最好的交易计划

而是：

- **判断当前是否存在一笔值得参与的交易；只有在答案是 yes 时，才返回计划**

---

## 四、Prompt 设计原则

### 4.1 核心原则

- 不默认需要返回交易
- 不把“可描述的结构”直接等同于“值得参与的交易”
- 不把 `NO_TRADE` 只定义成“没有 thesis”或“没有 plan”
- 必须把“不交易”视为与 `LONG / SHORT` 并列竞争的真实决策

### 4.2 交易 worth taking 的定义

所谓“值得做”的交易，不是：

- 有方向
- 有价位

而是同时满足：

- 有清晰的 4h 到 1d directional thesis
- 有一个让 thesis 真正变得可执行的 entry
- 该 entry 得到当前 `15m` 执行结构的支持，而不是被 `15m` 主动对抗
- 通往 target 的预期路径，明显优于通往 stop 的风险路径

也就是说：

- **值得做，不等于能做**

---

## 五、Prompt 文案改造方案

以下是建议替换/重写后的 entry prompt 关键部分。

### 5.1 Goal

```text
GOAL
Evaluate whether the current structure offers a 4h to 1d pending-order trade
worth taking — and if it does, return the plan.

A trade is worth taking when the structure provides:
- a clear directional thesis across the 4h to 1d horizon
- an entry level where the thesis becomes executable and 15m supports execution in that direction
- a target path whose expected move is meaningfully larger than the path to stop,
  with fewer structural obstacles between entry and target than between entry and stop

If these conditions are not met, the correct answer is NO_TRADE.
You are not required to find a trade every time you are called.
```

### 5.2 Self-check 补充建议

仅改 `GOAL` 还不够，建议在 `Before you output, verify` 中新增一条直接呼应目标函数的自检：

```text
- If you are choosing LONG or SHORT, would you take this trade with real capital now —
  or are you choosing a direction only because you can construct a plan from the structure?
```

这条的作用不是限制模型，而是强制它区分两件事：

- 这笔交易“能不能被结构表达出来”
- 这笔交易“是否真的值得用真实资金去做”

它直接对应本方案最核心的转变：

- 从“能构建 plan 就输出”
- 改成“值得做才输出”

### 5.3 为什么不用“70% 概率”写进 Goal

不建议直接把 `70%` 这样的数字写进 prompt，原因是：

- LLM 并不天然擅长输出经过校准的真实胜率
- 写死概率容易把模型推向：
  - 更近的 `TP`
  - 更保守的小目标
  - 看起来命中率高、但未必期望收益高的小单

因此更好的做法是：

- 用 **worth taking / expected value / cleaner path** 这类语言定义目标函数
- 不要求模型给伪精确概率

---

## 六、Decision Meaning 的调整建议

现有 `NO_TRADE` 定义偏弱：

- 只有在“没有 clear thesis”或“没有 coherent plan”时才容易触发

建议改成更贴近目标函数的版本：

```text
Decision meaning:
- LONG: the current structure offers a 4h to 1d long trade worth taking, and the best expression is a pending long plan.
- SHORT: the current structure offers a 4h to 1d short trade worth taking, and the best expression is a pending short plan.
- NO_TRADE: a thesis or a possible structure may exist, but the current setup is not worth taking because the edge, execution quality, or reward-to-risk is not strong enough.
```

这里最重要的是：

- `NO_TRADE` 不再只对应“没有 thesis”
- 也对应：
  - edge 不够强
  - execution 不够干净
  - reward-to-risk 不够好

---

## 七、JSON 自检字段改造方案

不建议新增“概率桶”字段，例如：

- `0-25%`
- `25-50%`
- `50-75%`
- `75-100%`

原因：

- 这种概率自评很容易失真
- 模型也容易 game 这个输出
- 它更像后验标签，不像可审计的事实层判断

### 7.1 去除原有自检字段

当前 entry 已经有一组旧的自检字段：

```json
{
  "thesis_flow_alignment": "aligned | mixed | opposed",
  "entry_readiness": "ready | developing | not_ready",
  "entry_exposure": "favorable | vulnerable | poor",
  "key_condition": "..."
}
```

这组字段虽然有用，但它本质上仍然更像：

- 方向一致性检查
- 入场成熟度检查
- 局部暴露质量检查

它们还没有直接围绕下面这个目标来设计：

> **这笔交易是否是一个由数据支撑、且大概率值得参与的盈利交易？**

因此，本方案建议：

- **去除原有的 entry 自检字段**
- **改用一组直接描述交易 edge 质量的字段**

### 7.2 推荐的新自检结构：`trade_quality`

推荐改成：

```json
"trade_quality": {
  "thesis_clarity": "strong | moderate | weak",
  "execution_quality": "strong | moderate | weak",
  "path_to_target_quality": "clean | contested | poor",
  "stopout_risk_before_resolution": "low | medium | high",
  "reward_to_risk_sufficiency": "ample | adequate | insufficient"
}
```

这组字段的设计逻辑是：

- `thesis_clarity`
  - 高周期方向 edge 到底清不清楚
- `execution_quality`
  - 当前 entry 是否真的是高质量可执行位置
- `path_to_target_quality`
  - 从 entry 到 target 的路是干净、受阻，还是很差
- `stopout_risk_before_resolution`
  - 在 thesis 兑现前，当前 stop 是否更可能先被噪音或局部反向波动打掉
- `reward_to_risk_sufficiency`
  - 当前目标空间相对于失效空间是否足够支撑这笔交易
  - 重点不是方向是否正确，而是这笔交易是否拥有足够大的收益空间去配得上当前 risk

### 7.3 为什么这组字段更符合目标函数

如果目标是：

> **构建一个数据支撑的大概率可盈利的交易**

那么最关键的不是：

- 模型是否觉得“方向一致”
- 模型是否觉得“可以构建 plan”

而是：

- thesis 是否清楚
- entry 是否值得用真钱做
- 去 target 的路是否比去 stop 的路更干净
- thesis 兑现之前，是否大概率先被打掉
- 目标空间相对于失效空间，到底是否足够

这组字段正好直接回答这 5 件事。

相比之下，旧字段的问题在于：

- 它们更像“结构是否勉强成立”
- 而不是“这笔交易值不值得做”

### 7.4 为什么不先回到概率过滤

即使现在目标已经明确成“大概率可盈利交易”，我仍然不建议直接让模型输出：

- `0-25%`
- `25-50%`
- `50-75%`
- `75-100%`

因为：

- 这种概率很难校准
- 它容易被 prompt wording 影响
- 它容易变成“看起来很精确”的伪数字

而 `trade_quality` 这组字段是：

- 可审计的
- 和数据直接对应的
- 更适合后续做观察、过滤和复盘

---

## 八、推荐落地顺序

### 第一步：先改 prompt 的 GOAL 与 NO_TRADE 定义

这是根因修正，优先级最高。

### 第二步：把 entry 自检层从旧的 `decision_context` 切到新的 `trade_quality`

这里的意思是：

- 不再围绕“alignment / readiness / exposure”组织 entry 自检
- 改成直接围绕“这笔交易的 edge 质量”来组织自检

### 第三步：观察修改后的真实输出一段时间

重点观察：

- `NO_TRADE` 是否增加
- 被 `V gate` / `RR gate` 拦截的比例是否下降
- 真正执行出去的交易质量是否提升

### 第四步：若观察后仍然需要执行层过滤，再决定是否基于 `trade_quality` 增加代码门槛

这一层当前不在本方案里直接落地，先不写死。

### 第五步：保留现有 `V gate / RR gate`

它们仍然是必要的客观底线，不应移除。

---

## 九、最终建议

如果目标是：

> **构建大概率值得参与、并且期望收益明显为正的交易**

那么最优路径不是：

- 让模型继续照常给单，再给自己打“概率分数”

而是：

1. **先改 entry 的目标函数**
2. **再把 entry 自检层改成直接描述交易 edge 质量的 `trade_quality`**
3. **先观察修改后的真实输出一段时间**
4. **若有必要，再决定是否基于 `trade_quality` 增加执行层门槛**
5. **最后继续用 RR/V gate 做客观兜底**

一句话总结：

- **entry 应该先回答“这笔交易值不值得做”，再回答“怎么做”**
- **entry 的自检层也应该直接回答“这笔交易的 edge 到底强不强”，而不是继续停留在旧的结构一致性标签上**
