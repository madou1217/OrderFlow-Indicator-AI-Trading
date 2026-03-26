# Scan Prompt & Schema 修改方案 v3.4.0

## 0. 版本定位

`v3.4.0` 不是在 `v3.3.0` 上继续做 deterministic ownership 下沉。

这版只解决一个更根本的问题：

- **对于一套目标是做 `4h-1d` 高质量交易的系统，`stage1 scan` 是否有必要保留完整的 `15m` 市场扫描？**

本版结论是：

- **没有必要保留完整 `15m full scan`**
- **应将 `15m` 从 full market parse 降级为 execution-layer context**

也就是说：

- `4h / 1d` 继续作为 `stage1` 的完整市场解析对象
- `15m` 不再与 `4h / 1d` 同等级扫描
- `15m` 只保留对 `stage2` 做 `entry / pending / management` 真正必要的执行层信息

---

## 1. 最高原则

本版完全服从下面这个最高原则：

- 不降模型输出质量是第一需求
- 不为了减少模型思考时长而降低输出质量
- 不用考虑工作量

因此，本版不是为了“少一个 timeframe 所以更快”，而是为了：

- **只让 `stage1` 产出对 `4h-1d` 交易真正必要的市场解析**
- **删掉与任务边界不匹配、且与 `stage2` 原始数据重复的那部分 `15m` full scan**

---

## 2. 第一性原理下，Stage1 和 Stage2 的职责

## 2.1 Stage1 的职责

`stage1 scan` 的职责是：

- 解析市场
- 解析主要参与者的目的和行为
- 为 `stage2` 提供可直接消费的市场地图

关键点在于：

- 这里的“市场地图”必须服务于**最终交易目标**
- 当前系统的最终交易目标是 **`4h-1d` 交易**

所以 `stage1` 应当优先回答的是：

- `4h / 1d` 的市场结构是什么
- `4h / 1d` 的主参与者在试图让市场做什么
- 当前主要 tension / invalidation / path objective 是什么

## 2.2 Stage2 的职责

`stage2` 的职责是：

- 读 `stage1` 的市场解析
- 结合当前 execution-time 数据
- 给出：
  - 方向
  - `entry`
  - `stop_loss`
  - `take_profit`
  - `leverage`

因此：

- `stage2` 本身就需要大量 near-price / current execution data
- `15m` 在这套系统里的主要价值，不是定义长期 thesis
- `15m` 的主要价值，是判断：
  - 当前表达是否可执行
  - 当前是否容易先被扫掉
  - 当前局部 auction 是在支持 thesis，还是在破坏 thesis

---

## 3. 根本判断：15m full scan 不符合任务边界

如果按第一性原理去问：

- **`15m` 的完整市场扫描，是否是 `4h-1d` 交易所不可替代的核心市场解析？**

答案是：

- **不是**

原因：

1. `4h / 1d` 才定义这笔交易的 thesis
2. `15m` 并不决定 `4h-1d` thesis 是否存在
3. `15m` 更像 execution / timing / sweep-risk / micro-acceptance 层
4. `stage2` 本身已经获得大量 `15m` 原始数据

这说明：

- 在 `stage1` 中保留一套和 `4h / 1d` 同重量的 `15m full scan`
- 本质上是在让模型把 execution layer 也当成 thesis layer 去扫描

这不符合任务边界。

---

## 4. 当前设计为什么会重复

当前系统里，`stage1` 和 `stage2` 都在消费大量 `15m` 信息：

- `stage1` 里：
  - `timeframes.15m.structure_parse`
  - `timeframes.15m.state_parse`
  - `timeframes.15m.participant_parse`
  - `timeframes.15m.evidence_trace`

- `stage2 entry / pending / management` 里：
  - `15m footprint`
  - `15m orderbook`
  - `15m price volume structure`
  - `15m cvd`
  - `15m kline history`
  - `15m atr_context`
  - 以及 execution-time 当前价与当前局部行为

这意味着：

- `stage1` 已经先对 `15m` 做了一次完整解释
- `stage2` 又拿着大量 `15m` raw data 再解释一次

这不是理想职责分工。

在不降质量的前提下，更合理的做法是：

- `stage1` 不再产出 `15m full scan`
- 改为产出一个**轻量 execution context**
- `stage2` 再结合自己已有的 `15m` 原始数据完成执行决策

---

## 5. v3.4.0 的核心结论

## 5.1 保留什么

保留：

- `4h` full scan
- `1d` full scan
- `cross_timeframe_parse`

因为这些是真正定义 `4h-1d` 交易 thesis 的部分。

## 5.2 删除什么

删除：

- `timeframes.15m` 的完整 full scan

也就是不再保留一套完整的：

- `structure_parse`
- `state_parse`
- `flow_parse / flow_override`
- `participant_parse`
- `evidence_trace`

## 5.3 用什么替代

用一个更轻的对象替代，例如：

- `execution_context_15m`

它不再回答“15m 市场是什么”，而只回答：

- 当前最近的微观支撑 / 压制在哪里
- 当前是否有明显的 15m sweep / rejection / late-entry 风险

---

## 6. 15m 应改成什么：execution-layer context

## 6.1 设计原则

`execution_context_15m` 的目标不是复刻一个小号 `scan`。

它只做两件事：

1. 告诉 `stage2` 当前最近的 micro constraint / support 在哪
2. 告诉 `stage2` 当前最重要的 15m 执行风险是什么

## 6.2 推荐 schema

```json
"execution_context_15m": {
  "micro_auction_state": "acceptance|rejection|reentry|balance|expansion|unresolved",
  "nearest_executable_support": {
    "low": 2144.18,
    "high": 2146.03,
    "reason": "fresh bullish absorption under local value"
  },
  "nearest_executable_resistance": {
    "low": 2148.31,
    "high": 2149.81,
    "reason": "value high capped by recent bearish absorption"
  },
  "entry_sweep_risk_15m": "low|medium|high",
  "micro_invalidation_risk": "low|medium|high",
  "execution_note": "string"
}
```

约束：

- `execution_note` 只允许一句
- 不再允许单独展开完整 `structure_lifecycle`
- 不再允许完整 `evidence_trace`

## 6.3 `thesis_effect` 应放到 `cross_timeframe_parse`

`thesis_effect` 不应留在 `execution_context_15m`。

原因：

- 它不是 `15m` 自身的孤立属性
- 它表达的是：`15m` 当前行为相对于 `4h-1d thesis` 的关系
- 这本质上是跨层判断，不是单层 execution field

因此更合适的做法是：

- 从 `execution_context_15m` 删除 `thesis_effect`
- 在 `cross_timeframe_parse` 中新增一个轻量字段，例如：

```json
"cross_timeframe_parse": {
  "execution_alignment_15m": "supports|degrades|blocks|conflicted|unclear"
}
```

这会让职责更清晰：

- `execution_context_15m` 只描述 15m 自身的微观执行状态
- `cross_timeframe_parse.execution_alignment_15m` 描述 15m 与 4h-1d thesis 的关系

---

## 7. 为什么这不会降低质量

本版的关键不是“少一个 timeframe”，而是：

- **不让 `stage1` 重复做 `stage2` 已经会做的 15m 解释**

如果 `stage2` 已经拥有足够的 `15m` 原始数据，那么保留完整 `15m scan` 的价值主要只剩两种：

1. 提供一个压缩后的 micro summary
2. 提供 `4h/1d` thesis 与 `15m` execution 的连接

这两件事并不需要一套完整 full scan。

只要下面几点仍然成立，质量不会下降：

- `4h / 1d` full scan 仍完整保留
- `stage2` 继续保留原有 `15m` 原始数据输入
- `15m` execution context 仍保留：
  - micro_auction_state
  - nearest support / resistance
  - sweep risk
- `cross_timeframe_parse` 仍保留 `execution_alignment_15m`

也就是说：

- `4h / 1d` 负责“这是什么交易”
- `15m execution_context` 负责“现在这个交易能不能表达”
- `stage2` 再结合原始 `15m` 数据给出最终 `entry / sl / tp / leverage`

这和当前任务边界是一致的。

---

## 8. 为什么不能直接完全删除 15m

本版不建议把 `15m` 从 `stage1` 完全删掉。

原因是：

- `stage1` 的职责不仅是解析 thesis，也要解析参与者
- `stage2` 虽然有 raw `15m` 数据，但它仍然需要一个**压缩后的 execution-level interpretation**
- 如果 `stage1` 完全不提供任何 `15m` 解释，`stage2` 就要重新从 raw data 完整重建：
  - 当前 micro auction state
  - 当前局部 constraint
  - 当前 15m 是否支持还是破坏 thesis

这会把推理负担重新压回 `stage2`

所以：

- **删 full scan：合理**
- **删所有 15m parse：不建议**

---

## 9. 对 prompt 的影响

## 9.1 Stage1 prompt

Stage1 prompt 不再要求：

- 对 `15m` 输出完整 five-layer parse

而是改成：

- `4h` 和 `1d` 输出完整 parse
- `15m` 只输出 `execution_context_15m`

也就是说，prompt 里不再把 `15m` 和 `4h / 1d` 放在同一组 output contract 里。

同时必须加一条硬约束，防止模型借 `execution_note` 变相恢复 `15m full scan`：

```text
execution_context_15m is not a market parse.
Do not analyze 15m structure, participant narrative, or flow narrative beyond the schema fields.
Only fill the execution_context_15m fields.
Keep execution_note to one short sentence only when it adds execution-critical context.
```

## 9.2 Stage2 prompt

Stage2 prompt 不需要再从 `stage1` 的 `15m full scan` 里读取 thesis。

它读取的是：

- `4h / 1d` full scan thesis
- `execution_context_15m`
- `cross_timeframe_parse.execution_alignment_15m`
- 自己已有的 `15m` raw execution data

这更符合它的任务：

- 从 durable thesis + live execution data 里做交易表达

---

## 10. 对 schema 的影响

## 10.1 当前 schema

当前：

```json
"timeframes": {
  "15m": { "full scan" },
  "4h": { "full scan" },
  "1d": { "full scan" }
}
```

## 10.2 v3.4.0 建议 schema

改成：

```json
"timeframes": {
  "4h": { "full scan" },
  "1d": { "full scan" }
},
"execution_context_15m": {
  "...": "..."
},
"cross_timeframe_parse": {
  "execution_alignment_15m": "supports|degrades|blocks|conflicted|unclear",
  "...": "..."
}
```

这样 schema 语义就清晰了：

- `timeframes` 只放 thesis timeframes
- `15m` 明确是 execution layer

---

## 11. 预期收益

这版的收益来源不是 deterministic remap，而是：

- 删掉一整套 `15m full scan` 输出与对应推理
- 同步收缩 `15m` 输入侧的 path detail
- 减少 `stage1` 对 `15m` 的 contextual summarization 负担
- 降低 schema 体积
- 降低 output token
- 降低 `stage1` 在 micro execution 层上的重复分析

这版不适合给出精确节省分钟数，但方向很明确：

- 比继续优化 `flow_parse` 的收益更结构性
- 因为它删除的是一个完整 timeframe 的 full parse

---

## 12. 输入侧也应同步裁剪

如果 `15m` 在 `stage1` 中已经被重新定义为 execution layer，
那么输入侧也应同步反映这个边界。

否则会出现一种不匹配：

- 输出侧只要求一个轻量 `execution_context_15m`
- 输入侧却仍然保留接近 full scan 的 `15m` 细节

这会让模型继续把 `15m` 当成 thesis-timeframe 去读。

## 12.1 当前最安全的输入裁剪

当前最适合先做、且质量风险最低的一刀是：

- `PATH_15M_DETAIL_BARS: 12 -> 6`

原因：

- `12` 根 `15m` bar = 约 `3` 小时
- 对 execution layer 来说，这已经偏向 micro background，而不只是当前 execution context
- `6` 根 `15m` bar = 约 `1.5` 小时
- 对下面这些字段已经足够：
  - `micro_auction_state`
  - `nearest_executable_support`
  - `nearest_executable_resistance`
  - `entry_sweep_risk_15m`
  - `micro_invalidation_risk`

## 12.2 当前建议修改

当前常量：

```rust
const PATH_15M_DETAIL_BARS: usize = 12;
```

建议改成：

```rust
const PATH_15M_DETAIL_BARS: usize = 6;
```

## 12.3 这个裁剪的含义

它影响的是：

- `path_newest_to_oldest.latest_15m_detail.bars_newest_to_oldest`
- 以及该块 `summary` 的统计窗口

也就是说，以下摘要将从“最近 12 根 closed 15m bars”变成“最近 6 根 closed 15m bars”：

- `bars_count`
- `net_change_pct`
- `range_high`
- `range_low`
- `range_pct`

这在 `v3.4.0` 的 execution-layer 定义下是可接受的。

原因：

- 这些字段不再承担 15m full market context 的职责
- 它们只需要服务最近一段 micro execution state

## 12.4 为什么只先改这一刀

这版不建议同时去动：

- `bracket_board`
- `15m events`
- `15m footprint clusters`

原因：

- 这些块对 `nearest_executable_support / resistance`
- 以及 `entry_sweep_risk_15m`
- 仍然可能提供高价值局部证据

相比之下，`PATH_15M_DETAIL_BARS: 12 -> 6` 是一个：

- 体积收益明确
- 职责边界清晰
- 质量风险最低

的输入裁剪点。

---

## 13. 风险与边界

## 13.1 风险

本版最大的风险不是“信息不够”，而是：

- `execution_context_15m` 如果设计得太薄，`stage2` 会失去一个高质量 micro summary

所以这版的关键不是“尽量删”，而是：

- **只删掉 `15m full scan` 里 thesis-level 的重复部分**
- **保留 execution-level 最小必要表达**

## 13.2 明确不做

本版不做：

- 不动 `4h / 1d` full scan
- 不动 `cross_timeframe_parse`
- 不删除 `stage2` 里的 `15m` raw 数据
- 不把 `15m` 从整个系统里彻底移除

---

## 14. 最终结论

按第一性原理重新定义后，结论非常明确：

- `stage1` 的任务是为 `4h-1d` 交易服务
- `4h / 1d` 是 thesis layer
- `15m` 是 execution layer

因此：

- **`stage1` 没必要保留完整 `15m full market scan`**
- **`15m` 应从 full scan 降级为 `execution_context_15m`**

这是本版唯一核心结论。

它的价值在于：

- 不改变系统最终目标
- 不要求降低输出质量
- 只是把 `15m` 放回它在这套交易系统中真正应该处于的位置
