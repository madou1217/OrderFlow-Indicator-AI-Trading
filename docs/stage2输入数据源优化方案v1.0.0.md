# Stage2输入数据源优化方案 v1.0.0

## 1. 背景

当前 `Stage2A/2B/2C` 的输入里，仍然混有一部分 runtime 为了调度与执行方便而附带的字段。

从职责边界看，这些字段里有一部分不应该进入 `Stage2` 的推理上下文：

- `hard_invalidation`
- `candidate_event`
- `path_runtime_state` 中承载本地解释的字段
- `latest_15m_trigger_facts` 这种过于粗糙的触发层打包
- `Stage2A.previous_tactical_plan`
- `Stage2A.tactical_position_slice`

这些字段的问题不在于“完全没用”，而在于它们会把 runtime 的调度语义、执行语义、本地预解释语义，混进 `Stage2` 的独立判断。

## 2. 本次已经落地的调整

本次代码已经先完成一件最重要的事：

- `hard_invalidation` 已经从 `Stage2` prompt 输入中移除。
- 机械止损型硬熔断仍然保留在 execution / watcher 层。
- 当 watcher / execution 层发现 `failure_level_breached` 时，会立即触发一次 `Stage1` 重建 path。
- 如果这次 `Stage1` 重建尚未完成，则本轮直接停止使用旧 path 进入 `Stage2`。

这次调整的目标是：

- 不让 `Stage2A` 被 runtime 预先告知“这条 path 已经硬失效”
- 但执行层仍然保留最基本的机械安全熔断

## 3. 第一性原理下的职责划分

### 3.1 Stage1

`Stage1` 只负责输出：

- 当前是否存在主导战略 path
- path 的 thesis
- path 的 `strategic_activation_level`
- path 的 targets
- path 的 `failure_level`
- path 的 `reevaluation_trigger`

`Stage1` 不负责战术入场点，也不负责仓位管理或挂单管理。

### 3.2 Stage2A

`Stage2A` 只负责两件事：

- 审核 `Stage1` 给出的 path 当前是否仍然成立
- 在 path 仍然成立时，给出当前市场条件下最优的 `entry / sl`

因此 `Stage2A` 不应该读取 runtime 的本地判定结论，也不应该读取执行层语义字段。

### 3.3 Stage2B

`Stage2B` 只负责：

- 基于最大收益目的管理当前持仓
- 判断加仓、减仓、平仓、移动止损、调整止盈
- 所有动作都以 watcher 条件触发的方式输出

### 3.4 Stage2C

`Stage2C` 只负责：

- 基于最大收益目的管理当前挂单
- 判断保留、取消、替换入场、更新成交后 bracket 模板
- 所有动作都以 watcher 条件触发的方式输出

### 3.5 Watcher / Execution

watcher / execution 层只负责：

- 监控 `Stage2A/2B/2C` 输出的条件是否命中
- 执行条件动作
- 保留机械硬熔断

watcher 不负责替 `Stage2A` 做战略审计。

## 4. 推荐的 Stage2 新输入结构

推荐把 `Stage2` 的输入拆成三层专用数据，而不是继续使用目前的混合输入。

### A. 冻结战略上下文层

这一层从 `Stage1` carry 过来，不实时重算。

用途：

- 只回答“这个 path 的大前提还在不在”
- 不作为实时 trigger

建议纳入：

- `i1 price_volume_structure`: `4h`
- `i4 liquidation_density`: `4h`
- `i18 AVWAP`: `Stage1` 选出的 anchor
- `i20 TPO`: `4h/1d` 的 `POC / VAH / VAL / IB / single prints`
- `i21 RVWAP sigma bands`: `4h`
- `i23 EMA trend regime`: `4h/1d`
- `i25 open_interest`: `4h`
- `i26 long_short_ratios`: `4h`
- `i27 ATM IV / RR-skew`: `1d regime`

原则：

- 这一层是战略背景，不是入场 trigger。
- `TPO` 的 session 定义本来就是 `4h/1d`。
- `EMA regime` 也是高周期过滤，不应被 `Stage2` 当作实时触发器。

### B. 15m 入场定位层

用途：

- 只回答“现在是不是已经到了一个值得出手的 zone”

建议纳入：

- `i1 PVS`: `15m`
- `i4 liquidation_density`: `15m`
- `i18 AVWAP`: 当前价格到各 anchor 的距离
- `i21 RVWAP sigma bands`: `15m`
- `i14 CVD pack`: `15m`
- `i3 divergence`: `15m`
- `i17 VPIN`: `15m`
- `i2 footprint`: `15m summary`

### C. 5m 持续性确认层

用途：

- 只回答“信号是不是具备持续性，而不是只闪一下”

建议纳入：

- `i5 orderbook_depth`: `5m summary`
- `i14 CVD pack`: `5m`
- `i25 OI`: `5m`
- `i26 ratios`: `5m`
- `i19 kline_history`: `5m`   #特别注意，该指标通过1m聚合，不从上游获取

## 5. 对 Stage2A 的删减建议

如果采用新的 A/B/C 输入结构，`Stage2A` 推荐删除以下字段：

- `latest_15m_trigger_facts`
- `path_runtime_state`
- `candidate_event`
- `previous_tactical_plan`
- `tactical_position_slice`

`Stage2A` 推荐保留：

- `stage1_output`
- `strategic_context_frozen`
- `entry_location_context_15m`
- `continuity_confirmation_context_5m`
- `state_guardrail_snapshot`
- `driver_guardrail_snapshot`
- `options_guardrail_snapshot`
- `account`

原因：

- `Stage2A` 的核心职能是审核 path 和给出最优 `entry/sl`
- 它不应该被 runtime 的调度理由、执行历史、局部预解释所污染

## 6. 对 Stage2B / Stage2C 的保留建议

`Stage2B/2C` 与 `Stage2A` 不完全一样。

它们仍然需要保留本层专属上下文：

### Stage2B 仍需保留

- 当前持仓上下文
- 上一次持仓管理计划

### Stage2C 仍需保留

- 当前挂单上下文
- 上一次挂单管理计划

原因：

- `Stage2B/2C` 是管理层，不是纯审核层
- 它们必须知道自己当前到底在管理哪一笔仓位 / 哪一笔挂单

但即便如此，`Stage2B/2C` 也应该改吃新的 A/B/C 输入层，而不是继续依赖 `latest_15m_trigger_facts + path_runtime_state + candidate_event` 这类老式混合输入。

## 7. 推荐迁移顺序

### 第一步

已完成：

- 从 `Stage2` prompt 输入中移除 `hard_invalidation`
- 保留 watcher / execution 机械硬熔断
- 硬熔断命中后立即触发 `Stage1` 重建 path

### 第二步

重构 `Stage2A` 输入：

- 引入 `A/B/C` 三层结构
- 移除 `latest_15m_trigger_facts`
- 移除 `path_runtime_state`
- 移除 `candidate_event`
- 移除 `previous_tactical_plan`
- 移除 `tactical_position_slice`

### 第三步

重构 `Stage2B/2C` 输入：

- 保留各自管理对象上下文
- 替换掉老的混合 trigger 输入
- 改为统一读取 `A/B/C` 三层结构

## 8. 最终目标

最终希望形成的结构是：

- `Stage1` 给 path
- `Stage2A` 审核 path 并给最优 `entry/sl`
- `Stage2B` 管持仓
- `Stage2C` 管挂单
- watcher / execution 只做条件执行与机械硬熔断

这样每一层的职责才是单一、清晰、可维护的。
