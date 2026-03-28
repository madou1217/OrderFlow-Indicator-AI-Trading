# LLM层工作流的修改落地代码方案 v1

基于 [llm层工作流的修改方案v2.0.0.md](/data/docs/llm层工作流的修改方案v2.0.0.md) 与当前 `systems/llm` 真实代码链路的逐段 review 形成。

这是一份代码实施蓝图，不是交易逻辑讨论稿。

本次改造的第一导向只有一个：

**让系统更接近顶级订单流交易员的真实工作流，从而做出更高质量的交易。**

因此本方案明确拒绝以下取向：
- 以减少工作量为导向保留旧链路
- 为了兼容旧 schema 而牺牲新工作流边界
- 把本该由 watcher 或执行层做的机械监测，继续塞给 Stage2
- 把本该由 Stage1 做的战略判断，下放给 15m 临场组件

如果当前代码与 [llm层工作流的修改方案v2.0.0.md](/data/docs/llm层工作流的修改方案v2.0.0.md) 冲突，以 `v2.0.0` 为准。

---

## 1. 第一性原理与硬约束

本次重构必须服从以下内核，不允许实现层偷换：

1. 唯一允许的决策顺序是：
   `位置 -> 状态 -> 驱动 -> 触发 -> 执行`

2. `Stage1` 只负责：
   - `3D / 1D / 4H` 地图
   - 唯一战略主剧本
   - 唯一战略 path
   - `4H / 1D` 驱动归因

3. `Stage2` 只负责两件事：
   - 先审计当前战略 path 是否还活着
   - 再在 path 不变前提下，设计更好的 tactical entry plan

4. `Stage2` 绝不允许：
   - 重选主剧本
   - 改主方向
   - 放宽 `Stage1.failure_level`
   - 激活 `failure_switch`
   - 输出 `WAIT`

5. watcher 必须接管：
   - 持续监测
   - 候选事件生成
   - 初筛
   - 主 entry / 备选 re-entry 的执行边际
   - 同一 `15m` 窗口内的二次入场次数控制

6. `Stage2` 的软否决必须收紧到原始内核：
   - `extreme_location`
   - `reverse_confirmation`
   - `driver_change`
   只有三者同时成立，才允许在硬失效前请求 `Stage1` 重评

7. 只要 `Stage2` 认可 path 还活着，后续“等、盯、试、复试、执行”的边际必须交回 watcher。

8. 仓位管理默认转回代码层，围绕 `Stage1.management_plan` 与结构化驱动恶化信号执行，不再与 `Stage2` 混在一起。

9. `telegram / x` 通知保留，但不得再绑在旧 Stage2 `WAIT/EXECUTE` 语义上。

10. 这是一次重构，不是打补丁。
    与新链路冲突的旧代码必须删除，不做“逻辑已经绕过所以先留着”的妥协保留。

11. `Stage1 / Stage2` 的职责边界已经完全换代。
    这意味着 JSON schema、prompt input、parser、provider schema 都必须重做，不能在 `v1.3.1` 的旧合同上继续打补丁。

---

## 2. 当前代码 review 结论

当前 `systems/llm` 主链已经是 workflow-only，但仍然是 `v1.3.1` 时代的链路，不是 `v2.0.0` 需要的链路。

### 2.1 当前主链真实形态

当前 `app/runtime.rs` 的真实顺序仍然是：

`1m minute_bundle -> Stage1(按4h/refresh) -> Stage2(按每次 bundle 评估) -> 直接执行 execution_intent -> 直接处理 management_actions`

这意味着：
- 还没有独立的 `watcher / candidate engine`
- `Stage2` 还是定时决策器，不是事件驱动审计器
- 还没有“path 活着则交给 watcher”的边界
- 还没有 `primary_entry_plan / secondary_entry_plan`
- 还没有 `path_review_candidate / entry_candidate` 事件模型

### 2.2 当前代码与 v2.0.0 的主要偏差

1. `runtime.rs`
- 仍然在一次 `bundle invoke` 中完成 Stage1、Stage2、执行、管理
- 没有独立 watcher 状态机
- 仍然把 Stage2 作为直接执行入口

2. `workflow/schema.rs`
- 仍是旧合同
- `Stage1Output` 没有 `risk_grade`
- `Stage2Decision` 仍是 `WAIT / EXECUTE / REQUEST_STAGE1_REEVALUATION`
- 仍有 `ExecutionIntent`
- 没有 `tactical_entry_plan`
- 没有 `primary_entry_plan / secondary_entry_plan`
- 没有 `CandidateEvent / PathRuntimeState / TacticalPlanState`

3. `workflow/stage2.rs`
- 仍在计算旧的 `Stage2RuntimeEvaluation`
- `reevaluation_trigger_hit` 仍是宽松的任一信号命中
- 仍由代码侧直接导出 `allow_execute`
- Stage2 仍围绕 `hard_gate / soft_gate / execute` 展开

4. `workflow/parser.rs`
- 仍强绑定旧 Stage2 输出
- 仍要求模型回显 `hard_gate / soft_gate`
- 仍要求 `EXECUTE` 时给 `execution_intent`
- 没有 path 审计优先的双阶段校验

5. `workflow/code_layer.rs`
- 仍只产出一个巨大的 `IndicatorSummary`
- 没有战略层与战术层拆分
- 没有 `state_guardrail_snapshot`
- 没有 `driver_guardrail_snapshot`
- 没有 `tactical_position_slice`
- 没有 `candidate_event` 所需的代码侧事实对象
- `options_surface` 目前只是原样透传到 `aux_context`
- 没有 `4H / 1D` 战略辅助摘要
- 没有 `Stage2` 可消费的 `options_guardrail_snapshot`
- 没有任何与新增 `i27` 期权指标对应的正式归一与压缩逻辑

6. `workflow/state.rs` 与 `workflow/persistence.rs`
- 状态过薄
- 只记了 `pending_stage1_refresh_reason` 和 `last_stage1_ts`
- 不足以承载 path lifecycle、tactical plan、尝试次数、15m 窗口、候选事件去重

7. `execution/intent_adapter.rs` 与 `execution/binance.rs`
- 仍是单次 `ExecutionIntent`
- 仍没有 watcher 选出的具体 `entry_plan`
- 仍没有 dual-entry / same-15m retry contract

8. `workflow/management.rs`
- 目前只是旧 `execution_intent` 的 snapshot helper
- 还不是代码侧管理引擎

9. `llm/prompt/workflow_stage1/base.txt` 与 `workflow_stage2/base.txt`
- 仍是旧 prompt 职责
- `Stage1` 仍写成 `4H/1D map`
- `Stage2` 仍写成 `WAIT / EXECUTE / REQUEST_STAGE1_REEVALUATION`

10. `llm/workflow_provider.rs`
- 仍在生成旧 JSON schema
- 没有 Stage2 新的 `PATH_CONFIRMED + tactical_entry_plan` 合同

11. `v1.3.1` 的历史合同残留还深度嵌在代码里
- `runtime_contract`
- `hard_gate / soft_gate`
- `execution_intent`
- `management_actions`
- `WAIT / EXECUTE`
- `Stage2RuntimeEvaluation`
- `stage2_refresh_minutes`
- 这些都说明当前代码仍在按旧 Stage2 思路组织，不是 `v2.0.0` 的 `path auditor + tactical plan` 链路

12. 当前 `llm` 的 RabbitMQ 摄入链路仍然只有：
- `q.llm.ind.minute`
- 绑定 `x.ind`
- `routing_key = bundle.1m.*`
- 也就是当前 `llm` 只消费 `ind.minute_bundle`
- 没有独立消费 `evt.*` 的 watcher 事件流
- 也没有独立的低延迟价格流

13. 上游 `indicator_engine` 已经具备本次改造需要的大部分原始指标产出能力，但 `llm` 侧尚未正式接好：
- `i27 -> options_surface` 已存在，且输出 `5m / 15m / 4h / 1d / 3d`
- `i25 -> open_interest` 已存在，且输出 `5m / 15m / 4h / 1d / 3d`
- `i26 -> long_short_ratios` 已存在，且输出 `5m / 15m / 4h / 1d / 3d`
- `i18 -> avwap` 已存在 `7d lookback`，并输出 `15m / 1h / 4h / 1d / 3d`
- 但 `llm` 代码层当前仍未把这些能力重组为 `Stage1 strategic summary / Stage2 tactical slice / watcher facts`

14. 当前 minute bundle 的多窗口指标不能只看顶层 `window_code`
- `indicator_engine` 在 bundle 组装时会优先把“主窗口”放到顶层 `window_code`
- 但真正的多窗口内容在 `payload.by_window` 或 `payload.series_by_window`
- 如果 `llm` 新 code layer 继续把顶层 `window_code` 当成真实时框来源，就会误判 `open_interest / long_short_ratios / options_surface / avwap` 是否具备 `4h / 1d / 3d`

15. `kline_history` 当前只正式产出：
- `1m`
- `15m`
- `4h`
- `1d`
- 还没有 `3d` 原始 bars
- 因此如果新版 `Stage1` 最终决定需要直接消费原始 `3d` bar 序列，而不仅仅依赖 `3d` 结构化指标，上游 `indicator_engine` 必须补 `i19 kline_history` 的 `3d` 输出

### 2.3 结论

这不是“局部补齐字段”能解决的问题。

必须重构的核心不是某一个 prompt，而是整个工作流分层：

`Stage1 -> watcher/candidate engine -> Stage2 -> watcher execution -> management engine`

---

## 3. 目标架构

### 3.1 重构后的唯一允许主链

```text
minute_bundle / realtime_feed
    ->
code layer
    ->
Stage1 strategic engine
    ->
watcher / candidate engine
    -> path_review_candidate / entry_candidate -> Stage2 tactical auditor
    -> hard_invalidation -> Stage1 refresh
    -> approved tactical plan -> concrete entry selection -> execution engine
    -> position lifecycle -> management engine
    -> telegram / x / journal
```

### 3.2 各层唯一职责

`code layer`
- 压缩与标准化数据
- 产出战略输入、战术切片、guardrail snapshot、watcher predicate facts

`Stage1`
- 输出唯一战略主剧本
- 输出唯一战略 path
- 输出 `risk_grade`
- 输出战略级 `reevaluation_trigger`

`watcher / candidate engine`
- 持续监测 path 是否接近、是否硬失效、是否满足候选送审条件
- 维护 tactical plan 生命周期
- 控制同一 `15m` 窗口内最多 `2` 次“实际成交后被打掉”的尝试

`Stage2`
- 第一身份：`path auditor`
- 第二身份：`tactical entry designer`
- 只输出：
  - `PATH_CONFIRMED`
  - `REQUEST_STAGE1_REEVALUATION`

`execution engine`
- 只做执行
- 不做战略判断
- 不做 path audit

`management engine`
- 只做代码侧管理
- 不再让 Stage2 输出管理动作

---

## 4. 重构策略

### 4.1 这是破坏式重构

本次实施按以下策略进行：

1. 不保留与新合同冲突的旧 Stage2 决策语义
2. 不保留旧的 `WorkflowRuntimeContract -> allow_execute -> execution_intent` 主链
3. 不保留旧的 `Stage2.management_actions` 主链
4. 不保留旧 persistence 对新 state 文件的隐式兼容
5. 不保留旧 prompt/schema 作为 fallback

### 4.2 状态文件采用版本化切换

当前状态文件与新链路不兼容。

实施方案采用以下规则：
- 新链路状态文件使用新的版本化命名或新目录，例如 `workflow_v2`
- 部署时不读取旧 `stage1_output / workflow_state / entry_snapshot` 文件
- 如需平滑切换，允许在上线脚本中显式清空旧 workflow state

原因很简单：
- 旧 `entry_snapshot` 是单一 `execution_intent` 合同
- 新链路需要 `strategic state + tactical plan state + watcher runtime state`
- 强行向后兼容只会把错误状态带入新系统

### 4.3 文件级职责必须拆开

当前 `schema.rs / parser.rs / code_layer.rs / stage2.rs / runtime.rs` 都过于肥大。

重构后必须做到：
- 单个文件只承担单一职责
- `runtime.rs` 只做 orchestration
- schema 与 parser 分离
- strategic 与 tactical 分离
- watcher 与 execution 分离
- management 与 Stage2 分离

---

## 5. 目标代码结构

推荐结构如下。

```text
systems/llm/src/workflow/
  mod.rs
  state.rs
  persistence.rs
  predicate.rs
  management.rs
  watcher.rs
  candidate.rs
  stage1.rs
  stage2.rs
  contracts/
    strategic.rs
    tactical.rs
    runtime.rs
  parser/
    stage1.rs
    stage2.rs
  code_layer/
    mod.rs
    strategic.rs
    tactical.rs
    guardrail.rs
    candidate.rs
```

说明：
- `contracts/strategic.rs`：Stage1 输入输出合同
- `contracts/tactical.rs`：Stage2 输入输出合同、entry plan 合同
- `contracts/runtime.rs`：watcher/runtime state 合同
- `parser/stage1.rs`：只解析 Stage1
- `parser/stage2.rs`：只解析 Stage2
- `code_layer/strategic.rs`：只构建战略层摘要
- `code_layer/tactical.rs`：只构建战术切片
- `code_layer/guardrail.rs`：只构建状态/驱动 guardrail
- `code_layer/candidate.rs`：只给 watcher 提供确定性候选事实

`workflow/schema.rs` 和 `workflow/parser.rs` 不应继续保留为大杂烩文件。
如果短期需要兼容编译路径，可以先保留为 re-export facade，但完成迁移后应删除。

---

## 6. 具体实施改造方案

## 6.1 数据源与 code layer 改造

### 6.1.1 目标

把当前单一 `IndicatorSummary` 改造成三类输出：

1. `StrategicIndicatorSummary`
- 给 Stage1

2. `TacticalReviewInputSlice`
- 给 Stage2

3. `WatcherFacts`
- 给 watcher / management / execution

### 6.1.2 数据源硬要求

`AVWAP`
- 最少保留：`7D / 3D / 1D / 4H`

`RVWAP sigma bands`
- 最少保留：`15m / 4H / 1D`

`options_surface`
- 必须保留为 `aux_context`
- 但必须额外压成 `4H / 1D` 可消费的战略辅助摘要供 `Stage1` 使用
- 不允许继续只作为“存在于 payload 里但没人正式消费”的边缘字段
- 上游若以新增指标 `i27` 提供期权面数据，代码层必须先把 `i27 -> options_surface` 归一成逻辑名称，再进入 workflow 合同；不得把 `i27` 这种编号直接泄露到 Stage1/Stage2 schema

`EMA regime`
- 最少保留：`3D / 1D / 4H`

`OI / long_short_ratios`
- 只接确认后的 `5m` 规范桶
- 允许边界后晚到
- 作为状态层，不作为秒级触发器

`divergence`
- 只保留去趋势、显著性过滤后的有效事件
- 必须带 `event_available_ts`
- 不允许用“原始 CVD 看起来像背离”替代

`trigger` 事件
- 必须都带：
  - `confirmed_at`
  - `confirmed_price`
  - `side`
  - `spot_confirm`

`kline_history`
- 只作为数据载体，不单独打分

### 6.1.2.1 上游数据源现状判断

这里先给出结论，避免后面把“上游没数据”和“`llm` 没接好数据”混为一谈。

当前仓库中的 `orderflow-indicator-engine` 对应实现目录是：
- `/data/systems/indicator_engine`

按第一性原理复核后的判断如下：

1. 对 `Stage1` 来说，当前上游已经具备大部分原始战略输入
- `3D / 1D / 4H` 的 `price_volume_structure`
- `liquidation_density`
- `AVWAP`
- `TPO market profile`
- `RVWAP sigma bands`
- `EMA trend regime`
- `FVG`
- `funding`
- `VPIN`
- `open_interest`
- `long_short_ratios`
- `CVD pack`
- `divergence`
- `whale_trades`
- 以及新增的 `i27/options_surface`

2. 对 `Stage2` 来说，当前上游也已经具备大部分原始战术输入
- `15m` 触发层事件
- `5m` 规范化的 `OI / ratio`
- `4H / 1D` 的状态和驱动快照
- `15m RVWAP`
- `orderbook_depth / footprint / absorption / initiation / exhaustion / high_volume_pulse`

3. 真正的缺口主要不在“有没有指标”，而在“数据组织方式”和“消费方式”
- `llm` 还在吃旧的 `minute_bundle -> old Stage1 -> old Stage2`
- `options_surface` 仍然只是 raw payload 透传
- `Stage2` 还没有事件驱动输入对象
- watcher 还没有自己的事件消费链

4. 当前 minute bundle 可以继续作为 `Stage1` 的基础输入来源
- 它足够承载新的 `Stage1 strategic summary`
- 也足够给 `Stage2` 提供基础战术切片
- 但不够承载 `watcher` 需要的“实时等待、事件驱动复核、秒级执行监测”

5. 当前 `indicator_engine` 已经存在 `evt.{indicator_code}.{symbol}` 的 snapshot fanout
- 这意味着我们不是从零发明 watcher 事件流
- 但必须把它正式接入 `llm` 新链路
- 不能继续停留在“仓库里有 fanout 代码，但 `llm` 实际没消费”

### 6.1.2.2 需要修改上游吗？

结论分三层：

1. 为了拿到 `i27/options_surface` 本身，不需要额外新增指标
- 上游已经有 `i27`
- 上游也已经按逻辑名 `options_surface` 对外输出
- 这里的工作重点是 `llm` 侧做正式归一、压缩和合同化消费

2. 为了让新 `Stage1 / Stage2` 真正跑起来，需要做上游 / MQ 侧改造
- 当前 `llm` 只消费 `bundle.1m.*`
- 新 watcher 必须新增独立消费者，至少接入 `x.ind` 上的 `evt.*.{symbol}` 一类事件流
- 如果不把 watcher 的事件流接进来，`Stage2` 仍然会退化成“按 bundle 定时跑一次”的旧模式

3. 为了实现你要求的“更快等待和更快入场”，仅靠当前 `bundle.1m.*` 和 `evt.*` 还不够
- `bundle.1m.*` 是分钟级
- `evt.*` 当前本质上也是 snapshot fanout，不是秒级价格流
- 如果 watcher 真的要承担秒级价格监测、执行窗口判定、主入场/备选 re-entry 的低延迟执行，则需要新增低延迟价格源
- 这通常意味着要从 MQ topology 或上游市场数据链路新增：
  - watcher 专用实时价格队列
  - 或直接接入更低延迟的 `x.md.live` / 等价实时价格流

4. `kline_history` 是否要改上游，取决于我们最终是否要求 Stage1 直接看原始 `3d` bars
- 如果 `Stage1` 只依赖 `3d` 的结构化指标地图，那么上游现状基本够用
- 如果 `Stage1 prompt/schema` 最终明确要求 raw `3d` kline context，则必须扩 `indicator_engine` 的 `i19 kline_history`

### 6.1.2.3 对实施方案的硬结论

本次改造不能写成“只改 `systems/llm` 就够了”。

必须把工作拆成两条并行线：

1. `systems/llm` 重构
- 新 contracts
- 新 code layer
- 新 Stage1 / Stage2
- 新 watcher / candidate engine
- 新 execution / management chain

2. 上游与 MQ 拓扑改造
- 验证并正式接入 `evt.*` fanout
- 为 watcher 新增独立 queue / consumer
- 若要秒级等待与更快执行，新增低延迟价格流
- 若要 raw `3d` bars 进入 Stage1，再扩 `kline_history`

### 6.1.3 分层输出规则

`StrategicIndicatorSummary`
- 位置层：全量战略地图
- 状态层：全量战略状态
- 驱动层：全量战略驱动
- 触发层：只允许最近一段与 `1D / 4H` 关键位绑定的已确认高质量摘要
- 辅助层：`options_surface` 的 `4H / 1D` 战略摘要，用于补充 `risk_grade / target corridor / failure envelope` 收敛

`TacticalReviewInputSlice`
- 当前 path corridor 周边的 `1D / 4H` 关键位
- 当日 `TPO POC / IB / Single Print`
- `15m RVWAP ±σ`
- 与当前 path 真正相交的 `4H / 1D AVWAP`
- 若 `7D / 3D AVWAP` 已进入当前 path corridor，也必须纳入
- 最新 `15m` 触发事实：
  - footprint
  - orderbook_depth
  - absorption/initiation
  - exhaustion
  - high_volume_pulse

`WatcherFacts`
- `price_in_stage1_activation_corridor`
- `price_breached_stage1_failure`
- `zone_acceptance_above/below`
- `reaccept_inside_value`
- `failed_auction_confirmed`
- `extreme_location_hit`
- `reverse_confirmation_hit`
- `driver_change_hit`
- `current_15m_window_id`
- `latest_realtime_price`

### 6.1.4 当前 RabbitMQ / bundle 语义必须显式处理

实现时必须额外注意以下几点：

1. 新 code layer 不能把顶层 `window_code` 当成多窗口指标的真实时框来源
- `open_interest`
- `long_short_ratios`
- `options_surface`
- `avwap`
- `rvwap_sigma_bands`
- 这些都必须从 `payload.by_window` 或 `payload.series_by_window` 取值

2. `bundle.1m.*` 仍可作为 Stage1 的基础快照输入
- 但不能再承担 watcher 的全部职责

3. watcher 必须新增自己的输入源抽象
- `indicator_event_feed`
- `realtime_price_feed`
- `bundle_snapshot_feed`
- 这样才能把“分钟级战略刷新”和“事件驱动战术执行”彻底分开

### 6.1.5 当前文件改造

必须改：
- `/data/systems/llm/src/workflow/code_layer.rs`
- `/data/systems/llm/src/llm/filter/code_layer_entry.rs`
- `/data/systems/llm/src/llm/filter/code_layer_management.rs`
- `/data/systems/llm/src/llm/filter/core_shared.rs`

实施要求：
- `workflow/code_layer.rs` 拆分，不允许继续单文件承载战略、战术、watcher 三类构建
- `filter` 层补齐 `3D`、`7D AVWAP`、`15m RVWAP`、`3D EMA`
- `options_surface` 继续只放 `aux_context` 原始包，但必须额外产出 `Stage1` 可消费的战略辅助摘要
- 必须新增 `i27/options_surface` 的过滤与归一逻辑，而不是继续直接透传原始 payload
- 必须新增 `options_guardrail_snapshot` 构建逻辑，供 `Stage2` 在 path corridor 与关键期权障碍重叠时使用

---

## 6.2 Stage1 改造

### 6.2.1 新职责

Stage1 必须完全重写成：
- `3D / 1D / 4H` 地图
- 唯一主剧本
- 战略 path
- `risk_grade`
- `4H / 1D` 驱动归因

### 6.2.2 新输入合同

Stage1 输入对象应至少包含：
- `task`
- `strategic_indicator_summary`
- `previous_stage1_output`
- `refresh_reason`

不再使用当前旧的泛型 `indicator_summary` 直塞模式。

### 6.2.3 新输出合同

Stage1 输出必须至少包含：
- `monitoring_status`
- `no_trade_reason`
- `map_summary`
- `current_script`
- `driver_attribution`
- `current_path`

`current_path` 必须新增：
- `risk_grade`
- 结构化 `reevaluation_trigger`
- 明确的 `path envelope`
- 必要的 `options_context_summary` 或等价战略辅助字段，用来表达期权面如何约束当前 path

`no_trade_reason` 只允许：
- `conflict_no_edge`
- `script_not_unique`
- `path_not_actionable`

### 6.2.4 代码侧校验

Stage1 parser 必须新增强校验：

1. `monitoring_status=no_edge` 时：
- `current_script == null`
- `current_path == null`

2. `monitoring_status=active` 时：
- 必须恰好一个 `current_path`
- `risk_grade` 必须存在
- `failure_switch` 必须是机器可读 identifier

3. 多时框冲突裁决必须做 parser 级别一致性校验：
- `4H` 逆 `1D` 且非极限位置，不允许输出可交易 path
- `4H` 同时逆 `1D` 和 `3D`，不允许输出趋势 continuation 型 target corridor

4. “价格还远离 activation_level”不得成为 `no_edge`

### 6.2.5 当前文件改造

必须改：
- `/data/systems/llm/src/workflow/stage1.rs`
- `/data/systems/llm/src/llm/prompt/workflow_stage1/base.txt`
- `/data/systems/llm/src/llm/prompt/workflow_stage1.rs`
- `/data/systems/llm/src/llm/workflow_provider.rs`
- `/data/systems/llm/src/workflow/parser.rs` 或其拆分后的 `parser/stage1.rs`

### 6.2.6 Stage1 schema 重设计要求

Stage1 schema 不允许在旧 `Stage1Output` 上做“字段追加式修补”。

必须明确做一次 schema 换代，至少做到：

1. 删除旧时代的宽泛字段语义
- `map_summary.market_tradeable`
- `map_summary.location_bias`
- `driver_attribution.driver_bias`
- 这些旧字段名如果继续保留，只能在语义完全重定义后存在；否则应删除

2. 新 schema 必须围绕新的战略职责组织
- `map_summary`
- `current_script`
- `driver_attribution`
- `current_path`
- `risk_grade`
- `no_trade_reason`

3. 新 schema 必须直接表达多时框裁决和 path 风险约束
- `3D / 1D / 4H` 背景结论
- `risk_grade`
- `failure_switch`
- `reevaluation_trigger`

4. 新 schema 必须允许 `options_surface` 的战略辅助摘要进入 Stage1 输出或其解释对象
- 但不能把期权面写成主 gate
- schema 命名必须继续使用逻辑名 `options_surface` 或 `options_context_summary`，不得把上游 `i27` 编号直接暴露给 LLM

---

## 6.3 watcher / candidate engine 改造

### 6.3.1 这是本次重构的关键新增层

当前代码完全没有这一层，必须新增。

watcher 不是一个 helper，而是新的运行主组件。

### 6.3.2 watcher 的职责

watcher 必须负责：
- 维护当前战略 path runtime state
- 维护当前战术 plan runtime state
- 生成 `path_review_candidate`
- 生成 `entry_candidate`
- 直接触发 `hard_invalidation`
- 控制 `same_15m_window` 内最多 `2` 次“实际成交后被打掉”的尝试
- 处理 `primary_entry_plan` 与 `secondary_entry_plan` 的生命周期

### 6.3.3 watcher 必须持有的状态

新增 `WorkflowState` 不得再只剩两个字段。

至少必须新增：
- `active_stage1_path_id`
- `active_stage1_ts`
- `active_risk_grade`
- `path_status`
- `pending_stage1_refresh_reason`
- `approved_tactical_plan`
- `tactical_plan_generated_at`
- `current_15m_window_id`
- `filled_stopout_attempt_count`
- `last_path_review_at`
- `last_entry_review_at`
- `last_management_eval_at`

必要时拆成两个状态对象：
- `WorkflowStrategicState`
- `WorkflowTacticalState`

### 6.3.4 候选事件模型

推荐最小事件集：
- `path_review_candidate`
- `entry_candidate`
- `hard_invalidation`
- `management_event`
- `no_edge_reentered`

约束：
- `hard_invalidation` 由 watcher 直接判定，不先问 Stage2
- `path_review_candidate` 用于 path 仍活着但需要重新审计或重排 tactical plan
- `entry_candidate` 用于准备执行 `primary` 或 `secondary` entry 之前的最后一次 Stage2 战术复核

### 6.3.5 触发源

watcher 输入不应只靠当前 `1m minute_bundle`。

实施上必须显式接入：
- `1m minute_bundle`
- `15m` 触发确认事实
- 秒级价格更新或等价实时事件流

推荐实现：
- 继续使用 RabbitMQ
- 新增独立 routing key 或独立 consumer 供 watcher 消费
- `Stage1` 基础快照继续走 `q.llm.ind.minute <- x.ind <- bundle.1m.*`
- watcher 事件流新增独立队列，至少绑定 `x.ind <- evt.*.<symbol>`
- 若要实现真正的秒级执行边际监测，再新增 watcher 的实时价格队列或直接接入更低延迟价格流
- watcher 不再依赖“每来一个 1m bundle 就顺手跑 Stage2”

这里必须明确：
- 仅复用当前 `q.llm.ind.minute` 不足以落地 `v2.0.0`
- 如果没有 watcher 自己的事件流和价格流，新架构会再次退化成旧 `Stage2` 轮询器

### 6.3.6 当前文件改造

新增：
- `/data/systems/llm/src/workflow/watcher.rs`
- `/data/systems/llm/src/workflow/candidate.rs`

必须改：
- `/data/systems/llm/src/workflow/state.rs`
- `/data/systems/llm/src/workflow/persistence.rs`
- `/data/systems/llm/src/app/runtime.rs`

---

## 6.4 Stage2 改造

### 6.4.1 新职责

Stage2 必须从旧的：

`WAIT / EXECUTE / REQUEST_STAGE1_REEVALUATION`

重写成新的：

`PATH_CONFIRMED / REQUEST_STAGE1_REEVALUATION`

Stage2 必须严格按两步运行：

`第一步：path 审计`
- 当前 path 是否还活着

`第二步：entry 设计`
- 只有 path 还活着，才输出新的 `tactical_entry_plan`

### 6.4.2 新输入合同

Stage2 输入至少包含：
- `candidate_event`
- `path_runtime_state`
- `previous_tactical_plan`
- `tactical_position_slice`
- `latest_15m_trigger_facts`
- `state_guardrail_snapshot`
- `driver_guardrail_snapshot`
- `stage1_output`

当前旧的 `runtime_contract + indicator_summary + active_positions + account` 模式必须删除。

`Stage2` 的新输入对象不建议继续沿用旧名 `Stage2PromptInput`。
更合理的命名应当是：
- `Stage2ReviewInput`
- `PathReviewInput`
- 或等价的新合同名

这样可以从命名上彻底切断 `v1.3.1` 时代的“定时决策器”心智模型。

### 6.4.3 path 审计规则

Stage2 path 审计顺序必须硬编码成：

1. 先看 watcher 是否已给出 `hard_invalidation`
2. 再看是否满足软否决三联条件：
   - `extreme_location`
   - `reverse_confirmation`
   - `driver_change`
3. 只有 path 还活着，才进入战术设计

以下情况不得单独触发 `REQUEST_STAGE1_REEVALUATION`：
- 当前微结构不够好
- 当前还没走到执行位置
- 主入场未触发
- 当前 `15m` 只是出现短暂反向压力

### 6.4.4 战术设计规则

当 path 还活着时，Stage2 必须输出：
- `primary_entry_plan`
- `secondary_entry_plan`
- `attempt_policy`

Stage2 允许：
- 大幅后移 `entry_activation_level`
- 大幅加深 `entry_activation_level`
- 重设更紧的 `entry_invalidation_level`
- 重设执行级 `stop_loss`
- 同时重写 `primary` 与 `secondary`

Stage2 不允许：
- 放宽 `Stage1.failure_level`
- 越出 `Stage1` path envelope
- 重写 `Stage1.activation_level` 的战略含义

### 6.4.5 当前 15m 与 path 反向时的处理

如果 path 仍活着，但当前 `15m` 的卖压、`OI`、`OBI/OFI/microprice/spot_confirm` 等表现与 path 相反：

Stage2 必须优先做的不是请求重评，而是：
- 重排 `primary_entry_plan`
- 重排 `secondary_entry_plan`
- 大幅调整战术入场点与执行级止损

只有当 path 审计已经失败，才允许请求 `Stage1` 重评。

### 6.4.6 当前文件改造

必须改：
- `/data/systems/llm/src/workflow/stage2.rs`
- `/data/systems/llm/src/llm/prompt/workflow_stage2/base.txt`
- `/data/systems/llm/src/llm/prompt/workflow_stage2.rs`
- `/data/systems/llm/src/llm/workflow_provider.rs`
- `/data/systems/llm/src/workflow/parser.rs` 或其拆分后的 `parser/stage2.rs`

必须删除的旧 Stage2 语义：
- `WAIT`
- `EXECUTE`
- `execution_intent`
- `management_actions`
- `WorkflowRuntimeContract.allow_execute`
- `hard_gate / soft_gate` 的旧回显校验逻辑

### 6.4.7 Stage2 schema 重设计要求

Stage2 schema 必须整体重写，不能在旧 `Stage2Decision` 上删几个字段后继续使用。

必须明确做到：

1. 删除旧决定模型
- `decision=WAIT`
- `decision=EXECUTE`
- `execution_intent`
- `management_actions`
- `hard_gate`
- `soft_gate`
- `request_stage1_reevaluation` 这种旧嵌套结构也不建议保留原名

2. 新决定模型只保留：
- `stage2_decision = PATH_CONFIRMED | REQUEST_STAGE1_REEVALUATION`
- `tactical_entry_plan`
- `reevaluation_reason`

3. `tactical_entry_plan` 必须是完整新对象，而不是旧 `execution_intent` 的变体
- `primary_entry_plan`
- `secondary_entry_plan`
- `attempt_policy`

4. 新 schema 必须天然表达新的职责边界
- 先 path audit
- 再 tactical entry design
- 绝不直接下 broker-ready order intent

5. parser 校验必须围绕 path envelope，而不是围绕旧 runtime gate 回显
- 如果存在 `options_guardrail_snapshot`，parser 只校验它是否作为战术约束存在，不得允许它单独改写 path 生死

---

## 6.5 执行引擎改造

### 6.5.1 新链路

执行引擎不再吃 `Stage2.execution_intent`。

新链路必须是：

`Stage2.tactical_entry_plan -> watcher 选中具体 entry_plan -> execution adapter -> Binance execution`

### 6.5.2 新输入对象

执行引擎应吃一个 watcher 选出的 `ConcreteEntryPlan`：
- `path_id`
- `entry_plan_id`
- `entry_profile`
- `intent_mode`
- `entry_zone`
- `entry_activation_level`
- `entry_invalidation_level`
- `stop_loss`
- `take_profit_1`
- `take_profit_2`
- `ttl_minutes`
- `max_drift_pct`
- `attempt_index`
- `context_key`

### 6.5.3 当前文件改造

必须改：
- `/data/systems/llm/src/execution/intent_adapter.rs`
- `/data/systems/llm/src/execution/binance.rs`

实施要求：
- 删除旧 `AdaptedExecutionIntent` 单一意图模型
- 新增 `AdaptedEntryPlan`
- `binance.rs` 入口从“从 LLM 直接执行”改为“执行 watcher 选定的 entry_plan”
- 如果命名上仍保留 `intent` 一词，必须仅表示执行层适配对象；不得再承载旧 Stage2 决策语义

---

## 6.6 管理引擎改造

### 6.6.1 新原则

管理不再由 Stage2 输出。

管理必须回到代码侧，根据：
- `Stage1.management_plan`
- `EntrySnapshot / PositionSnapshot`
- 结构化 `driver deterioration`
- `tp1 / tp2 / stop migration`

来生成确定性管理动作。

### 6.6.2 新职责

管理引擎必须负责：
- 命中 `tp1 / tp2`
- `stop_migration_rules`
- `reduce_on_driver_deterioration`
- `exit_full_on_driver_deterioration`
- `failure_level` 硬失效后的强制退出

### 6.6.3 当前文件改造

必须重写：
- `/data/systems/llm/src/workflow/management.rs`

必须从主链移除：
- `Stage2.management_actions`
- `adapt_management_action(...)` 主路径依赖

如果保留 `adapt_management_action`，也只能作为代码侧管理引擎到 broker 执行的适配器，不再由 LLM 直接产出。

---

## 6.7 persistence 与状态改造

### 6.7.1 新持久化对象

至少新增以下持久化对象：

1. `workflow_state_v2`
- 当前战略状态
- 当前战术状态
- 窗口与尝试次数

2. `stage1_output_v2`
- 最近一次战略输出

3. `tactical_plan_v2`
- 最近一次 `PATH_CONFIRMED` 产出的完整战术 plan

4. `entry_snapshot_v2`
- 实际入场后的持仓快照

5. `management_state_v2`
- 已完成的管理动作与当前允许的下一步管理边界

### 6.7.2 当前文件改造

必须改：
- `/data/systems/llm/src/workflow/state.rs`
- `/data/systems/llm/src/workflow/persistence.rs`

实施要求：
- 不再以单一 `context_key -> snapshot` 视角表达全部运行状态
- tactical plan 与 filled-stopout attempt 必须可恢复
- 同一 symbol 下的多执行 context 仍然要支持

---

## 6.8 runtime 编排改造

### 6.8.1 runtime.rs 必须瘦身

`app/runtime.rs` 在新架构中只允许承担：
- MQ 消费与事件分发
- Stage1 调度
- watcher 调度
- Stage2 调用调度
- execution / management / signal 调度
- journal 记录

不得继续把：
- Stage2 业务规则
- persistence 细节
- execution 业务判断
- management 规则
堆在同一个函数里。

### 6.8.2 运行主循环要拆成三个通道

建议拆成：

1. `StrategicRefreshLoop`
- 处理 `scheduled_4h`
- 处理 `path_invalidated`
- 处理 `no_edge_reentered`

2. `WatcherLoop`
- 消费实时价格与事件
- 维护 path state
- 生成 candidate
- 执行 primary/secondary entry
- 驱动 management

3. `Stage2ReviewLoop`
- 只处理 `path_review_candidate / entry_candidate`

### 6.8.3 当前文件改造

必须重写：
- `/data/systems/llm/src/app/runtime.rs`

实施要求：
- 不能再保留 `invoke_workflow_bundle_models(...)` 这种“一口气做完整个旧链路”的大函数
- `runtime.rs` 中旧的 `workflow_code_allows_execution(...)` 必须删除
- 旧 `stage2_runtime_eval -> runtime_contract -> execution_intent` 主链必须删除

---

## 6.9 provider / prompt / parser 改造

### 6.9.1 provider schema 必须整体换代

`workflow_provider.rs` 需要重新定义：

`Stage1 schema`
- 支持 `risk_grade`
- 支持新的 `map_summary`
- 支持新的 `no_trade_reason`
- 支持更严格的 `reevaluation_trigger`

`Stage2 schema`
- 只支持：
  - `PATH_CONFIRMED`
  - `REQUEST_STAGE1_REEVALUATION`
- 支持 `tactical_entry_plan`
- 支持 `primary_entry_plan`
- 支持 `secondary_entry_plan`
- 支持 `attempt_policy`

这里的关键不是“字段补齐”，而是“旧 schema 退役”。

因此 provider 层必须遵守：
- 不保留旧 `Stage2Decision` JSON schema 作为 fallback
- 不保留旧 `execution_intent_schema`
- 不保留旧 `management_action_schema` 主链
- 不保留旧 `WorkflowRuntimeContract` 回显合同
- 不保留旧 `WAIT / EXECUTE` enum
- 不允许把新增 `i27` 指标以编号名直接塞进 provider schema；provider 侧只能认逻辑名 `options_surface`

### 6.9.2 prompt 必须跟职责同步

`workflow_stage1/base.txt`
- 改成 `3D / 1D / 4H` map builder
- 强化多时框冲突裁决
- 强化 `risk_grade` 与 target 约束

`workflow_stage2/base.txt`
- 改成 path auditor first
- 明确只有 path alive 才谈 entry plan
- 明确软否决三联条件
- 明确“15m 反向压力时允许大幅改战术 entry/SL，但不得改战略 failure”

### 6.9.3 parser 必须拆开

Stage1 parser 要校验：
- `no_edge` 语义
- `risk_grade`
- 多时框冲突裁决一致性
- `options_surface` 只作为战略辅助输入，不得被 parser 或 prompt 偷偷升格成主 gate

Stage2 parser 要校验：
- 只允许两个决定
- `PATH_CONFIRMED` 时必须有完整 tactical plan
- `REQUEST_STAGE1_REEVALUATION` 时不得带 tactical plan
- tactical plan 不得越出 Stage1 path envelope
- `take_profit_1 / take_profit_2` 必须继承 Stage1 path
- `options_guardrail_snapshot` 即使存在，也只能约束 tactical entry，不能单独触发重评

### 6.9.4 当前文件改造

必须改：
- `/data/systems/llm/src/llm/workflow_provider.rs`
- `/data/systems/llm/src/llm/prompt/workflow_stage1/base.txt`
- `/data/systems/llm/src/llm/prompt/workflow_stage2/base.txt`
- `/data/systems/llm/src/workflow/parser.rs`

### 6.9.5 清理 v1.3.1 历史设计残留

本次升级必须显式清理以下 `v1.3.1` 时代残留，不允许“逻辑绕过但代码还在”：

- `WorkflowRuntimeContract`
- `Stage2RuntimeEvaluation`
- `WAIT / EXECUTE / REQUEST_STAGE1_REEVALUATION` 旧 decision contract
- `execution_intent`
- `management_actions`
- `hard_gate / soft_gate` 旧主链
- `stage2_refresh_minutes`
- 旧的 `Stage2PromptInput` 命名与其旧字段组织方式
- “期权面只在 aux_context 生存、但没有正式战略/战术摘要合同”的半接入状态

这是一次大版本升级，不是旧设计的兼容演化。

---

## 6.10 config 改造

### 6.10.1 保留

保留：
- `workflow.stage1_refresh_hours`
- `workflow.state_dir`
- `persist_prompt_inputs`
- `telegram / x` 配置

### 6.10.2 删除或降级

删除旧的：
- `workflow.stage2_refresh_minutes`
- 旧 `soft_gate_min_pass` 若仍只服务旧 Stage2 execute 语义，则改为 watcher/stage2 内部 guardrail 配置或直接固化到新 predicate 中

约束：
- 不允许为了兼容旧配置，把新的事件驱动 Stage2 又退化回定时轮询 Stage2

### 6.10.3 新增

建议新增：
- `workflow_v2.state_dir`
- `watcher.review_debounce_secs`
- `watcher.entry_candidate_cooldown_secs`
- `watcher.realtime_price_source`
- `watcher.max_filled_stopout_attempts`
- `watcher.entry_attempt_window_policy`
- `watcher.indicator_events_queue_key`
- `watcher.realtime_price_queue_key`
- `watcher.use_indicator_snapshot_fanout`

---

## 6.11 telegram / x / journal 改造

### 6.11.1 外部通知保留，但改绑定点

保留：
- `/data/systems/llm/src/app/telegram.rs`
- `/data/systems/llm/src/app/x.rs`

但通知绑定点必须改成新链路：

外部交易信号建议绑定：
- watcher 选中具体 `entry_plan` 并提交执行时
- 实际成交后
- 关键管理动作后

不建议再把外部信号直接绑在：
- `Stage2 PATH_CONFIRMED`
- 任何尚未进入执行的 path review

原因：
- `PATH_CONFIRMED` 是“当前 path 可继续沿用 + tactical plan 已生成”
- 不是“当前已经成交或必须立刻通知外部世界”

### 6.11.2 journal 事件需要重做

至少新增：
- `workflow_stage1_output_v2`
- `workflow_path_review_candidate`
- `workflow_entry_candidate`
- `workflow_stage2_path_audit`
- `workflow_tactical_plan_approved`
- `workflow_entry_plan_selected`
- `workflow_entry_execution_report`
- `workflow_management_report`

---

## 7. 文件级实施清单

### 7.1 必须新增

- `/data/systems/llm/src/workflow/watcher.rs`
- `/data/systems/llm/src/workflow/candidate.rs`
- `/data/systems/llm/src/workflow/contracts/strategic.rs`
- `/data/systems/llm/src/workflow/contracts/tactical.rs`
- `/data/systems/llm/src/workflow/contracts/runtime.rs`
- `/data/systems/llm/src/workflow/parser/stage1.rs`
- `/data/systems/llm/src/workflow/parser/stage2.rs`
- `/data/systems/llm/src/workflow/code_layer/mod.rs`
- `/data/systems/llm/src/workflow/code_layer/strategic.rs`
- `/data/systems/llm/src/workflow/code_layer/tactical.rs`
- `/data/systems/llm/src/workflow/code_layer/guardrail.rs`
- `/data/systems/llm/src/workflow/code_layer/candidate.rs`

### 7.2 必须重写

- `/data/systems/llm/src/app/runtime.rs`
- `/data/systems/llm/src/workflow/state.rs`
- `/data/systems/llm/src/workflow/persistence.rs`
- `/data/systems/llm/src/workflow/management.rs`
- `/data/systems/llm/src/workflow/stage1.rs`
- `/data/systems/llm/src/workflow/stage2.rs`
- `/data/systems/llm/src/llm/workflow_provider.rs`
- `/data/systems/llm/src/execution/intent_adapter.rs`
- `/data/systems/llm/src/execution/binance.rs`

### 7.3 完成迁移后必须删除

- `/data/systems/llm/src/workflow/schema.rs`
- `/data/systems/llm/src/workflow/parser.rs`
- 旧 Stage2 `WAIT / EXECUTE` 相关分支
- 旧 `execution_intent` 直达执行主链
- 旧 `management_actions` 由 Stage2 输出的主链
- 旧 `runtime_contract.allow_execute` 主链
- 旧 `stage2_refresh_minutes` 调度语义
- 旧 `Stage2RuntimeEvaluation`
- 旧 `WorkflowRuntimeContract`
- 旧 `execution_intent_schema / management_action_schema`

如果阶段性需要过渡编译，可以保留 facade 文件，但过渡完成后必须删除，不能永久双轨。

---

## 8. 实施顺序

### Phase 0. 先锁数据源与消息拓扑

1. 复核 `indicator_engine` 当前 bundle 是否满足 `Stage1 strategic summary` 的原始字段要求
2. 复核 `evt.*` fanout 是否已在生产链路中稳定发布
3. 为 watcher 设计并落地新的 MQ queue / bindings
4. 明确是否需要 raw `3d` kline；若需要，则先改上游 `i19 kline_history`
5. 明确 watcher 的实时价格源来自哪里；若当前 MQ 没有合适队列，先补拓扑

完成标志：
- 我们已经明确区分“上游已有但 `llm` 未接入”和“上游必须新增”的工作项
- 新 Stage1 / Stage2 / watcher 不再建立在错误的数据源假设上

### Phase 1. 先定合同

1. 新建 strategic / tactical / runtime contracts
2. 重写 provider schema
3. 重写 Stage1 / Stage2 prompt
4. 拆分 parser

完成标志：
- 旧 `WAIT / EXECUTE` 合同从编译主链移除

### Phase 2. 再改 code layer

1. 拆 strategic/tactical/guardrail/candidate builder
2. 补齐 `3D / 7D AVWAP / 15m RVWAP / 3D EMA`
3. 补齐 `options_surface` 的 Stage1 战略摘要与 Stage2 guardrail snapshot
4. 产出 watcher facts
5. 修正多窗口 bundle 解析逻辑，不再误用顶层 `window_code`

完成标志：
- Stage1 和 Stage2 不再共享一个旧式 `IndicatorSummary`

### Phase 3. 引入 watcher

1. 扩展 `WorkflowState`
2. 新增 `watcher.rs`
3. 新增 `candidate.rs`
4. 新增 tactical plan persistence

完成标志：
- watcher 能独立生成 `path_review_candidate / entry_candidate / hard_invalidation`

### Phase 4. 改 Stage1

1. 接入新的 strategic summary
2. 输出 `risk_grade`
3. 输出新的 `no_trade_reason`
4. 校验多时框冲突裁决

完成标志：
- Stage1 输出已与 `v2.0.0` 对齐

### Phase 5. 改 Stage2

1. 接入新的 tactical review input
2. 改成 path audit first
3. 输出 dual-entry tactical plan
4. 删除 execution/management 旧合同

完成标志：
- Stage2 只剩 `PATH_CONFIRMED / REQUEST_STAGE1_REEVALUATION`

### Phase 6. 改执行与管理

1. watcher 选出 concrete entry plan
2. execution adapter / binance 吃 new entry plan
3. management engine 代码侧化

完成标志：
- 旧 `execution_intent` 与 `management_actions` 从主链删除

### Phase 7. 瘦身 runtime 与清理旧代码

1. 重写 runtime orchestrator
2. 删除旧 schema/parser monolith
3. 删除旧 Stage2 runtime eval 旧逻辑
4. 删除旧 persistence/state 兼容层

完成标志：
- 仓库中不再存在旧 Stage2 主链

---

## 9. 验收标准

本次改造通过的标准不是“能编译”。

必须同时满足以下 10 条：

1. `Stage1` 只能输出一个战略主剧本和一个战略 path
2. `Stage1` 能正确输出：
   - `risk_grade`
   - `conflict_no_edge / script_not_unique / path_not_actionable`
3. `Stage2` 只允许两个输出：
   - `PATH_CONFIRMED`
   - `REQUEST_STAGE1_REEVALUATION`
4. `Stage2` 的软否决只有在三联条件同时成立时才触发
5. path 活着但 15m 反向压力增强时，Stage2 会大幅修改 tactical entry 与执行级 SL，而不是直接重评
6. watcher 能在同一 `15m` 窗口内完成：
   - `primary_entry_plan`
   - `secondary_entry_plan`
   - 最多 `2` 次“实际成交后被打掉”的尝试控制
7. execution engine 不再直接吃 LLM 的旧 `execution_intent`
8. management engine 不再依赖 `Stage2.management_actions`
9. 外部通知仍然保留，但绑定到新链路关键节点
10. 仓库中旧 `WAIT / EXECUTE / execution_intent / management_actions` 主路径已被删除
11. watcher 已不再只依赖 `q.llm.ind.minute <- bundle.1m.*`
12. 多窗口指标解析已改为读取 `payload.by_window / payload.series_by_window`，不再误用顶层 `window_code`

### 9.1 必测场景

至少覆盖以下测试：

1. `4H` 与 `1D` 同向，`3D` 反向
- 允许做
- `risk_grade=aligned_trend`
- target corridor 收窄

1.5 `i27/options_surface` 出现显著 pin / gamma wall / expiry magnet`
- `Stage1` 能把它收敛到 `options_context_summary`
- 它可以影响 `risk_grade / target corridor / failure envelope`
- 但不能单独主导主剧本

2. `4H` 逆 `1D`，但在极限位置，且为 `value_return`
- 允许 path
- `risk_grade=countertrend_repair`

3. `4H` 同时逆 `1D` 与 `3D`
- 只允许短程修复 target
- 不得输出 continuation 型远端目标

4. path 活着，但 15m 反向压力变大
- `PATH_CONFIRMED`
- tactical entry plan 后移或加深
- stop_loss 收紧

5. path 被软否决
- 必须同时满足 `extreme_location + reverse_confirmation + driver_change`

5.5 当前 path corridor 与关键期权障碍重叠
- `Stage2` 可收到 `options_guardrail_snapshot`
- 只能用于收紧 tactical entry / stop_loss
- 不得单独触发 `REQUEST_STAGE1_REEVALUATION`

6. 第一次成交后被打掉，且仍在同一 `15m` 窗口
- watcher 可以按 `secondary_entry_plan` 再入一次

7. 第二次成交后再被打掉
- watcher 不允许第三次尝试

8. `open_interest / long_short_ratios / options_surface / avwap` 的 minute bundle 顶层 `window_code` 仍是低窗口
- 新 code layer 仍能正确抽取 `4h / 1d / 3d` 战略信息

9. watcher 已接入 `evt.*.<symbol>`，但 `Stage1` 仍继续走 `bundle.1m.*`
- `Stage2` 不再因每个 minute bundle 被动轮询

10. 若系统要求 Stage1 消费 raw `3d` bars
- `indicator_engine` 的 `kline_history` 已扩出 `3d`
- 否则 Stage1 必须仍能只依赖 `3d` 结构化指标完成地图

---

## 10. 明确禁止事项

以下实现方式明确禁止：

1. 继续保留旧 `Stage2 WAIT` 语义，只是换个名字
2. watcher 继续缺位，仍由 `runtime.rs` 每个 minute bundle 顺手跑 Stage2
3. 让 Stage2 继续输出 broker-ready 的旧 `execution_intent`
4. 让 Stage2 继续输出 `management_actions`
5. 把“当前价格没到位”错误实现成 `Stage1=no_edge`
6. 让单一 15m 微结构弱化直接触发重评
7. 为了兼容旧 state 文件而保留错误字段与错误主链
8. 让 `telegram / x` 继续绑定旧 `WAIT / EXECUTE` 语义

---

## 11. 最终结论

本次改造的真实工作量集中在五个地方：
- `Stage1` 战略合同重做
- `watcher / candidate engine` 新增
- `Stage2` 从定时决策器改成 path auditor
- `execution / management` 与旧 LLM intent 脱钩
- `runtime / state / persistence` 重写

这五块里，`watcher / candidate engine` 是新的核心。

如果不把 watcher 独立出来，而只是继续在当前 `runtime.rs` 和 `stage2.rs` 上堆判断，那么无论 prompt 写得多漂亮，最后都不会真正落成 `v2.0.0`。

所以这次实施的标准很明确：

**不是把旧 workflow 改得“看起来像 v2.0.0”，而是把 `systems/llm` 真正重构成 `Stage1 -> watcher -> Stage2 -> watcher execution -> management engine` 这条新链路。**
