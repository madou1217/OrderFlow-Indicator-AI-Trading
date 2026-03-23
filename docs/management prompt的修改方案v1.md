# Management Prompt 修改方案 v1

**状态**：草案（待复核）
**适用阶段**：LLM Stage 2 `management`
**目标**：让 management 只服务一个目标：

> **在当前信息下，选择能最大化这笔已开仓交易期望收益、并同时控制回撤与失效风险的最佳动作。**

---

## 一、问题定义

当前 management prompt 的实际目标函数更接近：

- 原交易 thesis 是否仍然成立
- 当前 `SL` 是否仍然位于结构失效位之外
- 当前恶化是否只是应当容忍的 `noise / caution`

这会让模型更像：

- 高周期 thesis 审核器

而不是：

- 从“现在开始”重新评估这笔仓位最优动作的盈利/风险优化器

这会带来一个典型问题：

- 模型已经承认：
  - 当前不是好的 fresh entry
  - `15m` 反向风险正在增强
  - 当前仓位可能先被短周期噪音扫掉
- 但它仍然返回 `VALID / HOLD`

根因不是模型看不到风险，而是 prompt 允许它得出：

- “只要高周期 thesis 还没正式失效，就继续持有”

而不是要求它回答：

- “从现在开始，继续原样持有，是否仍然是这笔仓位的最佳盈利/风险动作？”

---

## 二、第一性原理目标函数

management 不应维护原判断，而应从当前时点重新优化仓位。

它要回答的问题应该是：

1. 从现在开始，这笔仓位的高周期 edge 还剩多少？
2. 近 `15m` 到 `1h` 的反转、挤压、回撤风险是否足以先打掉当前表达？
3. 继续原样持有，是否仍然是当前最佳的盈利/风险选择？
4. 如果不是，最优动作是：
   - `HOLD`
   - `REDUCE`
   - `CLOSE`
   - `ADJUST`
   - `ADD`

因此 management 的目标不是：

- 判断原 thesis 是否“还 valid”

而是：

- **在当前信息下，为这笔已存在仓位选择最优动作**

---

## 三、Prompt 设计原则

### 3.1 核心原则

- 不维护原交易
- 不默认“thesis 未失效 = 持有正确”
- 不把 `15m` 风险只当成是否应容忍的噪音问题
- 必须从“现在开始”的 forward edge 重新决策

### 3.2 动作优先级原则

模型应优先比较：

- 原样持有
- 主动减仓
- 主动平仓
- 调整 `tp/sl`
- 加仓

其中，“原样持有”不是默认动作，而只是候选动作之一。

---

## 四、Prompt 文案改造方案

以下是建议替换/重写后的 management prompt 结构。

### 4.1 开头定位

```text
You are an institutional order-flow position reviewer optimizing a live __SYMBOL__ futures position.
Use ONLY the provided indicators and market scan. Do not invent signals.
```

### 4.2 Goal

```text
GOAL
Choose the action that maximizes expected value from now for the existing position,
while controlling drawdown and thesis-failure risk.

Evaluate the position from the current state forward.
```

### 4.3 What You Receive

```text
WHAT YOU RECEIVE
- Core price and order-flow indicator snapshot (real-time)
- Real-time market scan: 15m, 4h, 1d trend direction, signal agreement, and structural range
- Position context: effective entry, tp, sl, size, leverage, direction, horizon
```

### 4.4 Before You Output

```text
BEFORE YOU OUTPUT
1. From now, what is the current 4h to 1d directional edge of this position?
2. Over the next few 15m bars, what is the most important reversal, squeeze, or stop-out risk against that edge?
3. Is keeping the position unchanged still the best current profit-risk choice?
4. Is the current SL likely to be hit by 15m to 1h noise before the thesis has enough time to resolve?
5. Does the current TP still maximize expected value, or has the reward profile become too conservative, too stretched, or obsolete?
6. If the position is in profit, should that profit now be protected?
7. If the position is in loss, is continued holding still superior to cutting risk now?
8. If, relative to reducing, closing, or adjusting, the existing position no longer has a meaningful edge from now,
   does holding unchanged still satisfy the GOAL?
9. To achieve the GOAL, is there a better action from now than keeping the position unchanged?
10. If short-term price action is more likely to produce a counter-move, balance, or pullback first,
    would protecting current profit, reducing risk, and waiting for a better re-entry satisfy the GOAL better than holding unchanged?
```

### 4.5 Decision Meaning

```text
Decision meaning:
- HOLD: keeping the position unchanged is the best current profit-risk choice.
- REDUCE: the thesis may still exist, but lowering exposure is better than holding full size unchanged.
- CLOSE: exiting now has higher expected value than continuing to hold.
- ADJUST: the position should remain open, but tp/sl must change because unchanged levels are no longer optimal.
- ADD: increasing exposure improves expected value more than simply holding, and current structure supports it.
```

### 4.6 代码落地状态

本方案对应的动作型决策已经在代码层落地：

- `HOLD`
- `REDUCE`
- `CLOSE`
- `ADJUST`
- `ADD`

已同步修改：

- management prompt
- management response schema
- management decision parser
- runtime / execution 映射逻辑

相关位置：

- [decision.rs](/data/systems/llm/src/llm/decision.rs#L222)
- [provider.rs](/data/systems/llm/src/llm/provider.rs#L1590)
- [medium_large_opportunity.txt](/data/systems/llm/src/llm/prompt/management/medium_large_opportunity.txt)
- [big_opportunity.txt](/data/systems/llm/src/llm/prompt/management/big_opportunity.txt)

其中：

- parser 兼容新的动作型决策，也兼容旧的 `VALID / INVALID / ADJUST` 输入映射
- schema 已切换到新的动作型 `decision`
- `decision -> params` 的关系已通过 schema + parser 共同约束

换句话说，这已经不是“只改 prompt”的方案，而是 prompt、schema、parser、runtime 一起切换后的实现状态。

---

## 五、JSON Schema 改造方案

建议将 `management_context` 精简为：

```json
"management_context": {
  "direction_state": "aligned | challenged | reversed",
  "near_term_risk_15m": "low | medium | high",
  "sl_survival_risk": "low | medium | high",
  "tp_state": "keep | revise_closer | revise_farther | obsolete",
  "key_condition": "string"
}
```

### 5.1 字段语义

- `direction_state`
  - 当前仓位方向在 `4h-1d` 语境下仍然对齐、受到挑战，还是已经反转

- `near_term_risk_15m`
  - 近 `15m` 是否存在高概率反转/挤压风险

- `sl_survival_risk`
  - 在 thesis 有时间兑现之前，当前 `SL` 是否更可能先被噪音碰到

- `tp_state`
  - 当前 `TP` 是合理、太近、太远，还是已经失效

- `key_condition`
  - 当前最重要的有效性/失效性条件

### 5.2 为什么不再使用 `unchanged_hold_is_best`

在本方案里，`decision` 已经直接表达动作：

- `HOLD`
- `REDUCE`
- `CLOSE`
- `ADJUST`
- `ADD`

只要动作型 `decision` 真正落地，`management_context` 就不需要再额外放一个：

- `unchanged_hold_is_best`

因为那会和 `decision=HOLD` 本身形成语义重复。

换句话说：

- 如果最终动作是 `HOLD`，就已经隐含表示“继续原样持有是最优动作”
- schema 更适合保留解释字段，而不是再加一个重复性的 gate 字段

---

## 六、Params 设计建议

为了匹配新的动作语义，建议把 decision 与 params 的关系改成：

### 6.1 HOLD

```json
{
  "decision": "HOLD",
  "params": {
    "close_price": null,
    "adjust_fields": null,
    "qty_ratio": null,
    "new_tp": null,
    "new_sl": null
  }
}
```

### 6.2 REDUCE

```json
{
  "decision": "REDUCE",
  "params": {
    "close_price": null,
    "adjust_fields": null,
    "qty_ratio": 0.25,
    "new_tp": null,
    "new_sl": null
  }
}
```

### 6.3 CLOSE

```json
{
  "decision": "CLOSE",
  "params": {
    "close_price": null,
    "adjust_fields": null,
    "qty_ratio": null,
    "new_tp": null,
    "new_sl": null
  }
}
```

### 6.4 ADJUST

```json
{
  "decision": "ADJUST",
  "params": {
    "close_price": null,
    "adjust_fields": ["tp", "sl"],
    "qty_ratio": null,
    "new_tp": 2028.1,
    "new_sl": 2061.2
  }
}
```

### 6.5 ADD

```json
{
  "decision": "ADD",
  "params": {
    "close_price": null,
    "adjust_fields": null,
    "qty_ratio": 0.25,
    "new_tp": null,
    "new_sl": null
  }
}
```

---

## 七、与当前 Prompt 的根本差异

当前 prompt 的内核是：

- thesis 是否仍 valid
- 若 valid，则默认保持

本方案的内核是：

- **从现在开始，哪一个动作的期望收益最高？**

因此它允许出现以下情况：

- thesis 尚未正式反转
- 但继续原样持有已经不是当前最优动作
- 那么正确答案应当是：
  - `REDUCE`
  - `ADJUST`
  - `CLOSE`

这正是 management 应当服务盈利目标、而不是服务“维护原 thesis”的地方。

---

## 八、预期收益

如果按本方案改造，management 的行为应当更接近：

- 看见 `15m` 强风险时，不再机械把它归入“应该容忍”
- 可以在 thesis 尚未正式死亡前，先优化利润实现和风险暴露
- 对“现在不是好 fresh entry”的判断，能够真正传导成：
  - 不再 `HOLD`
  - 而是 `REDUCE / ADJUST / CLOSE`

这会让 management 从：

- 高周期 thesis 审核器

变成：

- **已开仓交易的盈利-风险动态优化器**

---

## 九、建议落地顺序

1. 先改 prompt 目标函数与 decision meaning
2. 再改 `management_context` schema
3. 再同步修改 management decision parser / runtime / execution 映射
4. 最后再根据新输出分布，观察是否需要进一步细化字段

建议先做目标函数和动作语义切换，不要先加大量 corner case 规则。
