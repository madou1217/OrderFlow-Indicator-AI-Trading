# LLM层工作流的修改落地代码方案 v1

#此次修改是对llm层的大幅重构，重构要求：默认删除与本次重构无关的代码，只保留必要的helper。一切以本文档要求为准。重构时各个功能最好实现文件级别的职责明确的分离。不要把多个功能，逻辑混在一个文件内。

基于以下两类输入生成：
- [llm层工作流的修改方案v1.3.1.md](/data/docs/llm层工作流的修改方案v1.3.1.md)
- 当前 `systems/llm` 的真实代码结构

本文档的目标不是继续讨论交易逻辑，而是给出一份可以直接开工的代码实施方案。

本文档必须严格服从 [llm层工作流的修改方案v1.3.1.md](/data/docs/llm层工作流的修改方案v1.3.1.md)。
如果当前代码的实现习惯与 v1.3.1 冲突，以 v1.3.1 为准。

---

## 1. 目标与硬约束

本次改造的唯一目标是把当前 `systems/llm` 从旧的：

`Stage1 scan + Stage2 core(entry/pending/management) + Binance execution`

改造成 v1.3.1 规定的：

`代码层 + Stage1 + Stage2 + 执行引擎`

必须严格遵守以下硬约束：

1. 工作流内核只能是：
   `位置 → 状态 → 驱动 → 触发 → 执行`
2. Stage1 只能输出一个当前主剧本和一个当前 `path object`
3. Stage2 不能自己切换到另一个 path 并直接交易
4. `failure_switch` 只能表示“旧剧本失效后的下一优先重评方向”
5. 15m 只能做 setup 确认，不重写剧本
6. 1m / 100ms 只负责执行优化，不负责方向判断
7. 管理逻辑必须围绕“驱动是否恶化”展开
8. 不得把以下内容重新写回内核主链：
   - 多 path 并行激活
   - `position_policy / position_transition`
   - 固定 RR 门槛
   - `options_surface` 权重化 gate
   - `market_tradeable=false` 的固定阈值块
   - Stage2 的 freshness veto 二次交易判断

---

## 1.1 开工前需求确认（第一轮答复已收到，仍有阻塞项）

以下问题不是实现细节，而是会直接决定“什么叫做高质量交易”和“哪些规则属于内核、哪些只能放在执行/风险层”的前提问题。

这些问题在正式开工前必须由需求方明确回答。

如果回答与 [订单流交易员交易流程V1.md](/data/docs/订单流交易员交易流程V1.md) 冲突，以原始工作流内核为准；如果回答属于账户约束、执行约束、风控约束，则必须放在执行层或风险层，不能反写进工作流内核。

### 1.1.1 关于“高质量交易”的定义

1. 如果系统严格遵守原始工作流，但因此长时间 `NO_TRADE`，你认为这是成功还是失败？
   这个问题决定系统是“流程正确优先”还是“出手频率优先”。

2. 当“错过一笔本来能赚钱的单”和“做了一笔不符合工作流的烂单”发生冲突时，你认为哪一种错误更严重？
   这里请你给出明确优先级。

3. 你要的“像人类顶级订单流交易员”里，排序第一的是：
   - 严格按流程判断
   - 稳定过滤低质量单
   - 最终收益结果
   请你明确三者优先顺序。

### 1.1.2 关于交易范围与运行边界

4. v1 的实际交易范围是什么？
   请明确：
   - 交易所/账户
   - 合约类型
   - 首批交易 symbol
   - 是先单 symbol 试点，还是从一开始就多 symbol 并行

5. 同一 `symbol` 下，v1 是否需要真实支持多个并发上下文？
   这里不是问你是否把“单 symbol 单仓”写进策略内核，而是问：
   - 账户/执行层是否允许这种约束作为外部运行限制
   - 还是必须从 v1 开始就支持同 symbol 多上下文并发

6. 这个系统的 v1 目标是：
   - 全自动实盘决策与执行
   - 先做高质量决策引擎，允许人工复核
   - 先做影子决策但不执行
   这个答案会影响运行时保护、日志、失败处理和回退策略。

### 1.1.3 关于数据缺失与异常处理

7. 如果 `spot_confirm / OI / funding / ratio / VPIN` 中有一部分缺失、延迟或晚到，Stage2 默认应该怎么处理？
   请明确：
   - 一律降级为 `NO_TRADE`
   - 允许在部分证据缺失时继续判断
   - 仅某些字段缺失时阻断

8. 如果 Stage1 地图仍有效，但 Stage2 当前 15m 证据和 `driver_attribution` 出现冲突，你希望系统：
   - 直接等待，不开仓
   - 立刻请求 Stage1 重评
   - 允许在 soft gate 里继续通过
   这个问题决定“驱动冲突”在系统里是等待条件、重评条件还是可容忍噪音。

### 1.1.4 关于剧本失效与持仓处理

9. 当 `failure_level` 被触发且当前已有持仓时，你希望默认行为是什么？
   目前文档允许：
   - `REQUEST_STAGE1_REEVALUATION`
   - 必要时并发 `FLATTEN_POSITION`
   但这里还需要你明确：`failure_level` 命中时，平仓应该是默认动作，还是可选动作。

10. 当 `take_profit_1` 已兑现，但驱动继续强化时，v1 是否允许“超出 `next_path_target` 的延展持仓”？
   如果允许，就意味着需要新的 re-anchor/extend 合同；
   如果不允许，就意味着 v1 必须严格停留在 Stage1 已定义的路径目标内，超出部分只能通过下一轮 Stage1 重评获得。

11. 当驱动恶化但 `failure_level` 尚未命中时，你希望管理默认偏向哪一边？
   - 更激进地减仓/退出
   - 更保守地等待 failure_level
   - 按 setup_type 区分
   这个问题决定 `management_plan` 的默认管理哲学。

### 1.1.5 关于 gate 与执行层自由度

12. `soft gate = 3/4` 在你的理解里，是不是 v1 必须严格固定执行的规则？
   如果不是，请说明哪些 setup_type 允许例外，为什么。

13. 执行层的 `immediate / pullback / breakout` 三类 `intent_mode`，是否已经覆盖你对“1m / 100ms 只负责怎么进”的全部预期？
   如果不够，请说明缺的不是策略，而是哪种执行行为模式。

14. 执行层是否允许加入纯账户级/风险级约束，例如：
   - 最大日内亏损
   - 单 symbol 冷却时间
   - 最大同时持仓数
   - 账户级熔断
   如果允许，这些必须被视为外部风险层，而不是工作流内核的一部分。

### 1.1.6 关于验收标准

15. 对理论回放的“通过标准”你希望怎么定义？
   当前文档只保留了 5 类行情回放，但还没有定义“回放通过”到底是指：
   - 剧本选择正确
   - path object 完整且方向正确
   - setup 等待与放弃点正确
   - 管理动作正确
   - 以上全部

16. 在正式开工前，你是否要求先冻结一份“需求已确认版”文档？
   如果要，这一版应当把：
   - 你的问题答案
   - 不属于内核的账户/风险约束
   - v1 明确不做的范围
   一次性写死，后续代码实现只允许在该版本内执行，不再口头追加。

### 1.1.7 第一轮已确认答案（2026-03-27）

以下内容视为已确认需求，除非后续明确推翻，否则直接进入冻结版实施文档：

1. `NO_TRADE` 本身可以是成功结果。
   只要系统严格遵守 [订单流交易员交易流程V1.md](/data/docs/订单流交易员交易流程V1.md) 的工作流，不交易不是失败。
   但需求方同时明确要求：系统不能因为状态滞后、刷新不及时或合同缺失，而错过“最新证据已经清楚成立”的显著机会。

2. 错误优先级明确为：
   - 第一严重：做出一笔不符合工作流的烂单
   - 第二严重：错过一笔本来能赚钱的单

3. “像人类顶级订单流交易员”的优先级明确为：
   - 第一：最终收益结果
   - 第二：低质量过滤能力
   - 第三：流程忠实度

4. v1 交易范围冻结为：
   - 交易所：Binance
   - 账户：需求方自有账户
   - 合约：`ETHUSDT` 永续合约
   - symbol 范围：单 symbol，且 v1 永远只做 `ETHUSDT`

5. v1 运行形态冻结为：
   - 全自动实盘决策与执行
   - 不做人工复核链路
   - 不做影子模式

6. 数据缺失处理默认策略冻结为：
   - 优先尝试数据库补数
   - 如果补不到，不得仅因为 `spot_confirm / OI / funding / ratio / VPIN` 缺失就一律阻断交易
   - 允许在部分证据缺失时继续判断，但不得伪造、补写或臆测缺失数据

7. `failure_level` 命中且已有持仓时，默认行为冻结为：
   - 先请求 `Stage1` 重评
   - `FLATTEN_POSITION` 不是强制默认动作，而是可选并发动作

8. 当驱动恶化但 `failure_level` 尚未命中时，管理默认哲学冻结为：
   - 按 `setup_type` 区分，不写成全局单一风格

9. 执行层 `intent_mode` 当前只先实现：
   - `immediate`
   - `pullback`
   - `breakout`
   需求方认为长期不够，但允许在 v1 之后再扩展，不要求本轮先发明新执行模式。

10. v1 暂不引入外部账户级风险约束：
   - 不加最大日亏
   - 不加 symbol 冷却时间
   - 不加最大同时持仓数
   - 不加账户级熔断

11. v1 暂不纳入理论回放作为验收范围。
   理论回放移到下一版本，不在本次冻结版实施方案内作为交付要求。

12. 正式开工前必须先冻结一份“需求已确认版”文档。
   后续代码实现只允许在冻结版内执行，不再口头追加内核规则。

### 1.1.8 需求冻结补充（2026-03-27）

以下内容是第一轮问答后的补充冻结结论。自本节确认后，v1 文档不再存在阻塞冻结开工的未决内核问题。

1. 关于“同一 symbol 多上下文”的具体含义，现已确认。

   冻结结论：
   - v1 必须支持同一 `ETHUSDT` 下多个上下文并发恢复与管理
   - 允许例如：
     - 旧多单上下文仍在管理
     - 新反手上下文已经开始记录自己的 `entry_snapshot / path_id / management_action`
   - 这是一条运行期恢复与执行合同，不是新的策略规则

2. 关于 Stage1 与 Stage2 的权责，现已确认。

   冻结结论：
   - 继续忠于原始内核
   - Stage2 不能在 15m 直接改剧本或改方向
   - Stage2 只负责用最新证据更快触发 `REQUEST_STAGE1_REEVALUATION`
   - Stage1 仍然是主剧本与 `path object` 的唯一授权来源

3. 关于 `next_path_target` 之外的延展持仓，现已确认。

   冻结结论：
   - 允许延展到 `next_path_target` 之外
   - 但延展必须先经过一次新的 Stage1 重评
   - Stage2 不允许自行扩展目标位
   - 在新的 Stage1 未给出新 path / 新目标位之前，当前 path 的管理上限仍然止于 `next_path_target`

4. 关于 `soft gate = 3/4`，需求方已明确“不想写死”，并已确认采用按 `setup_type` 分开的配置方式。

   最终冻结默认值：
   - `A_continuation = 3/4`
   - `B_reversal = 2/4`
   - `C_value_return = 2/4`

   推荐理由：
   - `A_continuation` 最容易在 mid-auction 或晚一步追价时误开，应该维持更严格的软过滤
   - `B_reversal` 在 hard gate 已满足“极限位置 + 反转确认”的前提下，若继续要求 `3/4`，容易错过第一段反转
   - `C_value_return` 本质也是失败拍卖后的回归价值单，进入窗口通常比延续更短，适合比 `A_continuation` 更宽一些

   以上三组值自 2026-03-27 起作为 v1 冻结默认值，不允许实现层自行再拆更多档位。

---

## 2. 当前代码基线

当前 `systems/llm` 的主要结构如下：

| 路径 | 当前职责 | 与 v1.3.1 的关系 |
|---|---|---|
| [runtime.rs](/data/systems/llm/src/app/runtime.rs) | 调度 MQ 消费、先跑 stage1 scan，再跑 stage2 core，再决定是否执行 | 需要重构为新工作流总编排器 |
| [provider.rs](/data/systems/llm/src/llm/provider.rs) | 负责两段 prompt 调用、拼接 `STAGE_1_MARKET_SCAN_JSON`、解析模型响应 | 需要改成新的 Stage1 / Stage2 双 prompt 管线 |
| [decision.rs](/data/systems/llm/src/llm/decision.rs) | 解析旧式 `LONG/SHORT/NO_TRADE`、management、pending-order 输出 | 需要改成解析 `Stage1Output`、`Stage2Decision`、`ExecutionIntent`、`ManagementAction` |
| [scan.rs](/data/systems/llm/src/llm/filter/scan.rs) | 旧 Stage1 scan 输入压缩 | 不再作为最终结构；可复用其结构压缩逻辑生成 `indicator_summary` |
| [core.rs](/data/systems/llm/src/llm/filter/core.rs) | 旧 Stage2 core 输入拼装 | 需要拆解并重组为新的 Stage2 输入合同 |
| [core_entry.rs](/data/systems/llm/src/llm/filter/core_entry.rs) | entry 模式过滤 | 旧模式，需下线 |
| [core_management.rs](/data/systems/llm/src/llm/filter/core_management.rs) | management 模式过滤 | 旧模式，需下线 |
| [core_pending.rs](/data/systems/llm/src/llm/filter/core_pending.rs) | pending-order 模式过滤 | 不属于 v1.3.1 主工作流，需移出主链 |
| [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs) | 公共裁剪和 realtime_flow_context 逻辑 | 可复用部分基础函数 |
| [prompt.rs](/data/systems/llm/src/llm/prompt.rs) | 旧的 `Scan / Finalize / Management / Pending` prompt 路由 | 需要改成 `Stage1 / Stage2` 路由 |
| [execution/binance.rs](/data/systems/llm/src/execution/binance.rs) | 实盘下单、管理、挂单修改 | 需要保留交易所接口，但上层输入类型要改 |
| [config.rs](/data/systems/llm/src/app/config.rs) | LLM 调度与执行配置 | 需要加入 Stage1 调度、workflow state、兼容开关，并下放旧 gate 参数 |

当前运行方式的关键事实：

1. Stage1 每个 15m bundle 都会跑一次 scan
2. Stage2 会按账户状态切成 `entry / pending / management`
3. Stage2 会消费 Stage1 的旧 scan JSON，再基于实时 bundle 做 finalize
4. 交易执行入口仍然围绕旧 `TradeIntent`
5. 管理与 pending-order 都是单独的 LLM 模式
6. 当前代码里还保留了 `entry freshness recheck`、`RR gate`、`V gate`、`entry/sl remap` 等旧交易/执行规则

这些都与 v1.3.1 的双层分离版不一致。

---

## 3. 目标代码架构

目标架构必须变成：

```text
indicator bundle
    ↓
代码层（15m，无LLM）
    ↓
Stage1（4H 或显式刷新，LLM）
    ↓
Stage2（15m，LLM）
    ↓
执行引擎（1m / 100ms，无LLM）
```

新的职责分配：

- `代码层`
  - 输入原始 indicator bundle
  - 输出 `indicator_summary`
  - 维护 `auction_context.tracked_zones / zone_states / recent_15m_bars`

- `Stage1`
  - 输入 `indicator_summary`
  - 输出一个 `current_script + current_path`
  - 输出 `driver_attribution`
  - 输出 `monitoring_status`

- `Stage2`
  - 输入 `indicator_summary + stage1_output + active_positions`
  - 先判断当前剧本是否失效
  - 失效则请求 Stage1 重评
  - 未失效则检查 activation + setup + gate
  - 需要时输出 `execution_intent`
  - 如有持仓，输出 `management_actions[]`

- `执行引擎`
  - 输入 `execution_intent` / `management_actions[]`
  - 做 1m / 100ms 成交优化和订单变更

---

## 4. 代码落地原则

### 4.1 保留哪些现有能力

以下能力保留并复用：

- Binance REST / WS 的交易执行与账户状态拉取
- 现有 temp input / temp output / journal 持久化框架
- 现有 indicator bundle 读取方式
- 现有 prompt provider 适配器
- 现有 `core_shared.rs` 中可复用的结构裁剪函数
- 现有 `PositionContextState` / journal 恢复机制中的“持仓上下文持久化”思想

### 4.2 必须移出主工作流的旧逻辑

以下逻辑不得继续挂在主工作流判断链中：

- `management_mode / pending_order_mode` 作为 LLM 路由主开关
- pending-order 单独 LLM 模式
- `entry freshness recheck veto`
- `min_rr` 和 `min_distance_v` 作为核心策略 gate
- `entry_sl_remap`
- 旧 `scan_v6_x` 结构直接作为 Stage1/Stage2 合同
- 基于当前账户状态切换 prompt 职责

这些逻辑如果未来仍然保留，只能下放为：
- 执行层兼容行为
- 风险配置
- 研究附录
- 灰度兼容开关

不能再以“工作流必需规则”的形式存在。

---

## 5. 新模块设计

### 5.1 新建 `workflow` 领域模块

新增目录：

```text
systems/llm/src/workflow/
  mod.rs
  schema.rs
  state.rs
  predicate.rs
  code_layer.rs
  stage1.rs
  stage2.rs
  parser.rs
  management.rs
  persistence.rs
```

各文件职责：

| 文件 | 职责 |
|---|---|
| `schema.rs` | 定义所有新工作流结构体 |
| `state.rs` | 定义运行期 `WorkflowState` |
| `predicate.rs` | 负责结构化谓词的确定性判断 |
| `code_layer.rs` | 从现有 raw bundle 生成 `indicator_summary` |
| `stage1.rs` | Stage1 输入输出拼装与辅助校验 |
| `stage2.rs` | Stage2 输入输出拼装与辅助校验 |
| `parser.rs` | 解析 Stage1 / Stage2 模型输出 |
| `management.rs` | 管理规则执行辅助 |
| `persistence.rs` | `stage1_output`、tracked zones 的 symbol 级持久化，以及 `entry_snapshot` 的 context 级持久化 |

### 5.2 核心结构体

`schema.rs` 必须至少定义以下类型：

```rust
pub struct IndicatorSummary { ... }
pub struct AuctionContext { ... }
pub struct TrackedZone { ... }
pub struct ZoneState { ... }
pub struct Stage1Output { ... }
pub struct MapSummary { ... }
pub struct DriverAttribution { ... }
pub struct CurrentPath { ... }
pub struct ReevaluationTrigger { ... }
pub struct ManagementPlan { ... }
pub struct HardGateEvaluation { ... }
pub struct SoftGateEvaluation { ... }
pub struct Stage2Decision { ... }
pub struct ExecutionIntent { ... }
pub struct ManagementAction { ... }
pub struct EntrySnapshot { ... }
pub struct WorkflowState { ... }
```

其中必须满足：

- `Stage1Output` 只能有一个 `current_script`
- `Stage1Output` 只能有一个 `current_path`
- `current_path.id` 必须存在，作为 Stage2 / 执行层 / 持久化链路的唯一当前 path 标识
- `Stage2Decision` 不得包含“切换到另一个并行 path”的动作
- `EntrySnapshot` 必须绑定 `context_key`，不得只用 `symbol` 作为唯一标识
- `ManagementPlan` 必须把结构位目标和驱动恶化响应写成明确字段，不能只留自然语言说明
- `ExecutionIntent` 必须是可直接传给执行引擎的线协议，不能只留空对象
- `ManagementAction` 必须按 action type 携带足够参数，不允许执行层自行猜测
- `Stage2Decision` 必须支持 `management_actions[]`，因为同一 `symbol` 下允许多个上下文并发恢复与管理
- `ManagementAction` 只能包含：
  - `HOLD`
  - `REDUCE_POSITION`
  - `FLATTEN_POSITION`
  - `MOVE_STOP`
  - `UPDATE_TAKE_PROFIT`

### 5.3 结构化谓词

`predicate.rs` 只实现 v1.3.1 允许的谓词：

- `zone_acceptance_above`
- `zone_acceptance_below`
- `reaccept_inside_value`
- `failed_auction_confirmed`
- `price_above_on_close`
- `price_below_on_close`
- `max_age_minutes`
- `near_level`
- `event_after_precondition`

不得新增：
- RR 谓词
- 仓位排他谓词
- path 并行切换谓词

---

## 6. 代码层实施方案

### 6.1 新代码层不改 indicator_engine

本次不改 `systems/indicator_engine`。

原因：
- v1.3.1 的代码层属于 LLM 系统内部的数据压缩层
- 当前 indicator bundle 已经包含所需原始信息
- 直接在 `systems/llm` 内从 raw bundle 生成 `indicator_summary`，改造范围最小

### 6.2 新增 `workflow::code_layer`

新增：

`systems/llm/src/workflow/code_layer.rs`

功能：
- 输入 `ModelInvocationInput.indicators`
- 输出 `IndicatorSummary`

生成规则：

1. 位置层、状态层、驱动层、触发层由当前 `scan.rs` 和 `core_shared.rs` 现有裁剪逻辑抽取
2. 所有事件统一补齐：
   - `confirmed_at`
   - `confirmed_price`
3. 新增 `auction_context`
   - `tracked_zones`
   - `zone_states`
   - `recent_15m_bars`
4. `options_surface` 如果保留，只能进入 `aux_context`

### 6.3 代码复用策略

直接复用或迁移的现有代码来源：

- 从 [scan.rs](/data/systems/llm/src/llm/filter/scan.rs) 迁移位置层压缩逻辑
- 从 [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs) 迁移事件、orderbook、footprint、divergence 的基础裁剪逻辑
- 从 [core_entry.rs](/data/systems/llm/src/llm/filter/core_entry.rs) 提取 entry 关注的事件裁剪逻辑
- 从 [core_management.rs](/data/systems/llm/src/llm/filter/core_management.rs) 提取管理期所需的持仓证据构造逻辑

### 6.4 tracked zones 的实现

必须实现持久化的 `tracked_zones`。

实现方式：

1. Stage1 每次输出 `current_path.tracked_zones`
2. `workflow::persistence` 将其按 symbol 落盘
3. 下一轮代码层在生成 `IndicatorSummary` 时读取上一轮 `tracked_zones`
4. 代码层为每个 tracked zone 计算 `zone_states`

### 6.5 `entry_snapshot` 的持久化粒度

`stage1_output` 和 `tracked_zones` 仍然是 symbol 级状态，但 `entry_snapshot` 不能做成 symbol 单实例。

必须改成：

1. `entry_snapshot` 按 `context_key` 落盘
2. `context_key` 复用当前 runtime / journal 已有的执行上下文键语义，不新发明仓位政策
3. 同一 `symbol` 下允许存在多个 `entry_snapshot`
4. 不得因为持久化文件只有一个而隐含引入 “one-symbol-one-position” 或其他仓位排他规则

这里的 `context_key` 是运行期上下文标识，不是新的交易策略字段。它只负责区分：
- 同一 symbol 下不同持仓方向或执行上下文
- 同一 symbol 下需要分别恢复的 entry / management 连续性

建议新增状态文件目录：

```text
systems/llm/state/workflow/
  ETHUSDT.stage1_output.json
  ETHUSDT.tracked_zones.json
  ETHUSDT.entry_snapshot.BOTH.json
  ETHUSDT.entry_snapshot.LONG.json
  ETHUSDT.entry_snapshot.SHORT.json
```

如果当前 symbol 只有一个上下文，就只存在其中一个文件；如果有多个上下文并存，则分别持久化，不互相覆盖。

---

## 7. Stage1 实施方案

### 7.1 Stage1 输入

Stage1 输入必须改成：

```json
{
  "task": "执行地图、剧本选择、path object 构建、驱动归因",
  "indicator_summary": {},
  "previous_stage1_output": {},
  "refresh_reason": "scheduled_4h | thesis_invalidated | no_edge_reentered"
}
```

不得再输入旧的：
- `scan_v6_x`
- `stage_1_market_scan_json`
- 多条 prior paths

### 7.2 Stage1 输出

Stage1 输出必须严格遵守 v1.3.1：

- `meta.stage1_ts`
- `monitoring_status`
- `no_trade_reason`
- `refresh_hints`
- `map_summary`
- `current_script`
- `driver_attribution`
- `current_path`

其中 `current_path` 还必须包含：
- `id`
- 六个核心字段：`thesis / activation_level / first_path_target / next_path_target / failure_level / failure_switch`
- `setup_type`
- `reevaluation_trigger`
- `management_plan`
- `tracked_zones`

其中语义必须区分清楚：

- `activation_level / first_path_target / next_path_target / failure_level` 是价格或价格带字段，必须绑定 1D / 4H 结构位
- `thesis` 是当前 path 的剧本说明，不是价格字段
- `failure_switch` 是失效后的下一优先重评方向，不是价格字段，也不是并行活跃 path

不得输出：
- `paths[]`
- `map_premises`
- `path_premises`
- `switch_predicate`
- `position_policy`

### 7.2.1 `management_plan` 必填合同

`management_plan` 不能只写成自然语言摘要，必须至少展开为：

```json
{
  "take_profit_1_basis": "first_path_target",
  "take_profit_2_basis": "next_path_target",
  "take_profit_1_level": 0,
  "take_profit_2_level": 0,
  "stop_migration_rules": [
    {
      "after_target": "take_profit_1 | take_profit_2",
      "new_stop_basis": "activation_level | first_path_target | next_path_target",
      "new_stop_level": 0
    }
  ],
  "reduce_on_driver_deterioration": [
    {
      "driver_signal": "spot_confirmation_lost | oi_support_lost | fake_order_risk_rising | driver_flip_confirmed",
      "reduce_ratio": 0.0
    }
  ],
  "exit_full_on_driver_deterioration": [
    {
      "driver_signal": "driver_flip_confirmed"
    }
  ]
}
```

约束：

- `take_profit_1_level` 必须绑定 `first_path_target`
- `take_profit_2_level` 必须绑定 `next_path_target`
- 目标位必须来自结构位，不得引入固定 RR 目标
- `stop_migration_rules` 只能在目标位兑现后生效，不得先于目标位主动移损
- 超出 `next_path_target` 的目标延展不得由 `management_plan` 自行发明
- 如果需求方希望继续延展，必须先触发新的 `Stage1` 重评，由新 path 提供新的目标位
- `reduce_on_driver_deterioration` 和 `exit_full_on_driver_deterioration` 只能引用原始工作流已定义的驱动恶化语义：
  - 现货确认丢失
  - OI 支持丢失 / unwind
  - fake order risk 上升
  - spot / futures 驱动关系翻转
- 上述 `driver_signal` 名称可以按代码风格调整，但语义不得超出原文
- `failure_level_breached` 不属于 `management_plan`
- `failure_level_breached` 属于 Stage2 的剧本失效处理：应触发 `REQUEST_STAGE1_REEVALUATION`，必要时可并发 `FLATTEN_POSITION`

### 7.3 Prompt 改造

调整 [prompt.rs](/data/systems/llm/src/llm/prompt.rs)：

当前：
- `Scan`
- `Finalize`

改为：
- `WorkflowStage1`
- `WorkflowStage2`

新增文件：

```text
systems/llm/src/llm/prompt/workflow_stage1.rs
systems/llm/src/llm/prompt/workflow_stage2.rs
systems/llm/src/llm/prompt/workflow_stage1/base.txt
systems/llm/src/llm/prompt/workflow_stage2/base.txt
```

Stage1 prompt 只允许模型做：
- 地图
- 当前主剧本选择
- 当前 path object 构造
- 驱动归因

明确禁止：
- 并行剧本
- 多 path 排序
- RR 规则扩展
- 仓位政策发明

### 7.4 Stage1 解析器

新增 `workflow::parser::parse_stage1_output`。

解析器必须验证：

1. `meta.stage1_ts` 必须存在
2. `monitoring_status = active` 时必须有 `current_script` 和 `current_path`
3. `monitoring_status = active` 时，`current_path` 必须同时包含：
   - `id`
   - 六个核心字段
   - `setup_type`
   - `reevaluation_trigger`
   - `management_plan`
   - `tracked_zones`
4. `monitoring_status = no_edge` 时 `current_script = null` 且 `current_path = null`
5. `failure_switch` 只能是剧本名，不是 path id
6. `setup_type` 只能是：
   - `A_continuation`
   - `B_reversal`
   - `C_value_return`
7. `reevaluation_trigger` 只能表达三类证据：
   - `extreme_location`
   - `reverse_confirmation`
   - `driver_change`
8. `management_plan` 必须包含：
   - `take_profit_1_basis`
   - `take_profit_2_basis`
   - `take_profit_1_level`
   - `take_profit_2_level`
   - `stop_migration_rules`
   - `reduce_on_driver_deterioration`
   - `exit_full_on_driver_deterioration`
9. `take_profit_1_basis` 只能是 `first_path_target`
10. `take_profit_2_basis` 只能是 `next_path_target`
11. `take_profit_1_level` 必须与 `first_path_target` 对齐
12. `take_profit_2_level` 必须与 `next_path_target` 对齐
13. `management_plan` 中不得出现：
   - `rr`
   - `min_rr`
   - 任意固定盈亏比阈值
   - 任意仓位排他或对冲政策
14. `current_path.id` 必须存在且非空
15. `failure_switch` 只能是剧本重评方向，不能被解析成价格位

`tracked_zones` 虽然是工程字段，但在本方案里属于 v1.3.1 已经明确允许的必要实施合同，不得省略。

---

## 8. Stage2 实施方案

### 8.1 Stage2 输入

Stage2 输入必须统一，不再分 `entry / management / pending` 三种 prompt 模式。

输入合同：

```json
{
  "task": "执行当前path检查、setup确认、输出execution_intent、执行持仓管理",
  "indicator_summary": {},
  "stage1_output": {},
  "active_positions": [],
  "account": {}
}
```

现有的：
- `management_mode`
- `pending_order_mode`
- `stage_1_setup_scan_json`

全部退出主工作流。

### 8.2 Stage2 唯一权限

Stage2 的主决策只有三种：

1. 等待当前 path
2. 基于当前 path 输出 `execution_intent`
3. 请求 Stage1 重评

此外，`management_actions[]` 不是第四种并列主决策，而是独立附带通道：
- 只要存在持仓，Stage2 都可以同时输出 `management_actions[]`
- `management_actions[]` 可以与 `WAIT / EXECUTE / REQUEST_STAGE1_REEVALUATION` 任一主决策并存
- `management_actions[]` 允许同一 `symbol` 下按不同 `context_key` 同时管理多个恢复链路

绝对禁止：

1. 自己实例化 `failure_switch`
2. 自己并行持有多个 path
3. 自己把 `alternate path` 当场激活并直接下单

### 8.3 Stage2 输出

新增统一结构：

```json
{
  "decision": "WAIT | EXECUTE | REQUEST_STAGE1_REEVALUATION",
  "reason": "",
  "request_stage1_reevaluation": {
    "refresh_reason": "thesis_invalidated | no_edge_reentered",
    "trigger_source": "failure_level | reevaluation_trigger | refresh_hint"
  },
  "execution_intent": {},
  "management_actions": []
}
```

当前 `TradeIntent`、`PositionManagementIntent`、`PendingOrderManagementIntent` 三套输出模型不再作为主链协议。

输出约束：
- `management_actions[]` 是正交字段，不是互斥分支
- 有持仓时，即使 `decision = EXECUTE` 或 `REQUEST_STAGE1_REEVALUATION`，仍然允许同时携带 `management_actions[]`
- 没有持仓时，`management_actions = []`
- 如果 `failure_level` 已触发且当前 `context_key` 有持仓，允许输出：
  - `decision = REQUEST_STAGE1_REEVALUATION`
  - `management_actions[]` 中可包含一个或多个 `FLATTEN_POSITION`

### 8.3.1 `execution_intent` 线协议

`EXECUTE` 时，`execution_intent` 不能再是空对象，必须至少展开为：

```json
{
  "side": "LONG | SHORT",
  "intent_mode": "immediate | pullback | breakout",
  "entry_zone": {
    "low": 0,
    "high": 0
  },
  "trigger_price": null,
  "stop_loss": 0,
  "take_profit_1": 0,
  "take_profit_2": 0,
  "ttl_minutes": 15,
  "max_drift_pct": 0.3,
  "path_id": "path_current",
  "entry_snapshot": {
    "context_key": "",
    "path_id": "path_current"
  }
}
```

约束：

- `side` 必须与 `current_script` / `current_path` 方向一致
- `entry_zone` 或 `trigger_price` 必须来自 `activation_level` 与 setup 确认后的执行区
- `stop_loss` 必须绑定 `failure_level`
- `take_profit_1` 必须绑定 `first_path_target`
- `take_profit_2` 必须绑定 `next_path_target`
- `path_id` 必须等于 `current_path.id`
- `entry_snapshot.path_id` 必须等于 `execution_intent.path_id`
- `ttl_minutes` 和 `max_drift_pct` 属于执行参数，不得承载方向判断
- `entry_snapshot.context_key` 必须存在，供 execution / management 连续性恢复使用

### 8.3.2 `management_actions[]` 线协议

`management_actions[]` 中的每个元素都必须是无歧义协议，不允许执行层猜测动作参数。

最小合同：

```json
{
  "type": "HOLD | REDUCE_POSITION | FLATTEN_POSITION | MOVE_STOP | UPDATE_TAKE_PROFIT",
  "context_key": "",
  "path_id": "path_current",
  "reduce_ratio": null,
  "new_stop_loss": null,
  "take_profit_1": null,
  "take_profit_2": null
}
```

按动作类型约束：

- `HOLD`：所有可选数值字段必须为 `null`
- `REDUCE_POSITION`：必须带 `reduce_ratio`
- `FLATTEN_POSITION`：不得携带新的价格目标
- `MOVE_STOP`：必须带 `new_stop_loss`
- `UPDATE_TAKE_PROFIT`：至少带一个非空的 `take_profit_1` 或 `take_profit_2`

统一约束：

- `context_key` 必须存在
- `path_id` 必须等于该 `context_key` 对应持仓上下文所绑定的 `entry_snapshot.path_id`
- 对于“当前 path 下新开的仓位”，其 `path_id` 可以与 `current_path.id` 相同；对于“旧上下文的持续管理”，其 `path_id` 可以不同于当前 `current_path.id`
- 所有价格字段必须来自该上下文所属 path 的 `management_plan` 或该 path 的结构位，不得来自 RR 推导
- `management_actions[]` 不得编码仓位排他、对冲、加仓优先级之类额外政策

### 8.4 Stage2 解析器

新增 `workflow::parser::parse_stage2_decision`。

解析器必须保证：

- `REQUEST_STAGE1_REEVALUATION` 必须带 `request_stage1_reevaluation.refresh_reason`
- `request_stage1_reevaluation.refresh_reason` 只能取：
  - `thesis_invalidated`
  - `no_edge_reentered`
- `REQUEST_STAGE1_REEVALUATION` 不能同时带 `execution_intent`
- `EXECUTE` 必须带完整 `execution_intent`
- `WAIT` 不得偷偷带交易指令
- `management_actions[]` 可以与任一主决策并存
- 没有持仓时不得输出非空 `management_actions[]`
- `execution_intent.side` 必须与当前 path 方向一致
- `execution_intent.stop_loss` 必须与 `failure_level` 对齐
- `execution_intent.take_profit_1` 必须与 `first_path_target` 对齐
- `execution_intent.take_profit_2` 必须与 `next_path_target` 对齐
- `execution_intent.path_id` 必须等于 `current_path.id`
- `execution_intent.entry_snapshot.context_key` 必须存在
- `execution_intent.entry_snapshot.path_id` 必须等于 `execution_intent.path_id`
- `management_actions[]` 中每个元素的 `type` 都必须满足对应字段约束
- `management_actions[]` 中每个元素都必须携带 `context_key`
- `management_actions[]` 中每个元素的 `path_id` 都必须与该 `context_key` 的持仓上下文绑定 path 一致
- `management_actions[]` 的价格字段必须来自对应上下文所属 path 的 `management_plan` 或 path 结构位
- `management_actions[]` 不得携带 RR 字段或仓位政策字段

### 8.5 Stage2 谓词执行

Stage2 的结构化判断由 `workflow::predicate` 负责，不由 prompt 文本隐式完成。

Stage2 运行顺序必须严格固定：

1. 处理 `no_edge`
2. 检查 `failure_level`
3. 检查 `reevaluation_trigger`
4. 检查 `activation_level`
5. 检查 `setup_type`
6. 过 `hard gate / soft gate`
7. 输出 `execution_intent`
8. 独立执行持仓管理

不能调整顺序。

### 8.5.1 `hard gate / soft gate` 字段级合同

Stage2 在代码内必须落成明确的 gate 结构，不允许只保留模糊描述。

建议最小内部结构：

```json
{
  "hard_gate": {
    "location_valid": false,
    "trigger_confirmed": false
  },
  "soft_gate": {
    "state_clear": false,
    "driver_clear": false,
    "orderflow_real": false,
    "invalidation_clear": false,
    "passed_count": 0
  }
}
```

字段语义：

- `hard_gate.location_valid`
  - 价格必须位于当前 path 对应的 value edge / anchor / sigma / IB / single print / liquidation zone / reaccept 区
- `hard_gate.trigger_confirmed`
  - 必须已经出现与 `setup_type` 对应的确认事件，不能裸猜
- `soft_gate.state_clear`
  - OI / ratio / funding / VPIN 与当前剧本不冲突
- `soft_gate.driver_clear`
  - `driver_attribution` 与当前 15m 证据一致，没有出现明显 driver flip
- `soft_gate.orderflow_real`
  - OBI / OFI / microprice / spot_confirm / fake_order_risk 对当前 setup 不构成反证
- `soft_gate.invalidation_clear`
  - `failure_level` 与 `execution_intent.stop_loss` 都明确存在

判定规则：

- `hard_gate` 必须全部为 `true`
- `soft_gate` 采用按 `setup_type` 分开的最低通过值
- 当前推荐默认值为：
  - `A_continuation: passed_count >= 3`
  - `B_reversal: passed_count >= 2`
  - `C_value_return: passed_count >= 2`
- `trigger_confirmed` 的成立必须绑定到第 4 步 setup checklist，不得跳过 setup 直接过 gate

### 8.5.2 setup checklist 与 gate 的绑定

为避免 `hard gate / soft gate` 变成新的自由裁量层，Stage2 必须固定采用以下绑定关系：

- `A_continuation`
  - setup checklist 只检查：`initiation / stacked imbalance / OBI-OFI-microprice 同向 / spot_confirm / fake_order_risk / OI 支持`
- `B_reversal`
  - setup checklist 只检查：`absorption 或 exhaustion / divergence / 现货不再同向推动 / footprint 失败信号`
- `C_value_return`
  - setup checklist 只检查：`failed auction / 回收 value / 缺失 OI 与 spot 支持`

这些 checklist 只是在代码里把原文触发条件结构化，不得扩展成新的策略评分器。

### 8.5.3 `failure_level` 与持仓管理的边界

为了避免把“剧本失效”误写成“管理规则”，必须明确：

- `failure_level` 命中属于 path invalidation，不属于 `management_plan`
- `management_plan` 只负责：
  - 结构位目标兑现后的止盈 / 移损
  - 驱动恶化导致的减仓 / 退出
- `failure_level` 命中时，Stage2 必须优先走：
  - `REQUEST_STAGE1_REEVALUATION`
- 如果当前 `context_key` 存在持仓，Stage2 可以并发输出：
  - `management_actions[]` 中对应上下文的 `type = FLATTEN_POSITION`

实现上不得把 `failure_level_breached` 塞回 `driver_signal` 列表。

---

## 9. 执行引擎实施方案

### 9.1 保留 `execution/binance.rs`

[binance.rs](/data/systems/llm/src/execution/binance.rs) 保留为交易所适配层。

本次不重写 Binance 交互细节，只改上层输入协议。

### 9.2 新增执行适配层

新增：

```text
systems/llm/src/execution/intent_adapter.rs
```

功能：
- 把新的 `ExecutionIntent` 转为当前 Binance 下单函数可执行的参数
- 把新的 `ManagementAction` 转为当前管理执行函数可执行的参数

适配要求：

- 适配层不得补全缺失的 `stop_loss / take_profit_1 / take_profit_2`
- 适配层不得自行推断方向
- 适配层不得根据 RR 或账户状态改写 `management_actions[]`
- 如果上游协议缺字段，必须报错而不是猜测执行

### 9.3 新的执行职责

执行引擎必须只做：

- `immediate / pullback / breakout` 三类入场执行
- 挂止损和止盈
- 根据 `management_actions[]` 逐条减仓 / 平仓 / 移损 / 更新止盈

### 9.4 旧逻辑处置

以下逻辑从主链移除：

- pending-order LLM mode
- `entry freshness recheck`
- `entry_sl_remap`
- `min_rr`
- `min_distance_v`

处理方式：

- 默认关闭
- 如果短期为了兼容保留，必须移到 `compatibility` 配置块
- 新工作流路径默认不读取这些参数

---

## 10. runtime 重构方案

### 10.1 重构目标

当前 [runtime.rs](/data/systems/llm/src/app/runtime.rs) 的职责太集中。

需要拆成：

1. bundle 接收
2. `indicator_summary` 生成
3. Stage1 调度与刷新
4. Stage2 调度
5. 执行分发
6. state 持久化

### 10.2 新运行时流程

新的主流程必须是：

```text
收到 15m bundle
→ build_indicator_summary
→ load_workflow_state
→ if 到 4h 边界或存在 refresh_request: run_stage1
→ persist_stage1_output
→ run_stage2
→ dispatch_execution_intent
→ dispatch_management_actions
→ persist_entry_snapshot_for_context / workflow_state
```

### 10.3 Stage1 调度

新增调度规则：

- 默认只在 `00:00 / 04:00 / 08:00 / 12:00 / 16:00 / 20:00 UTC` 跑 Stage1
- 如果 Stage2 请求重评，则在下一个 15m cycle 立即跑 Stage1

这要求 `config.rs` 新增：

```yaml
llm:
  workflow:
    stage1_refresh_hours: [0, 4, 8, 12, 16, 20]
    stage2_refresh_minutes: [0, 15, 30, 45]
    state_dir: "systems/llm/state/workflow"
    legacy_modes_enabled: false
```

### 10.4 runtime 内部函数重组

建议在 [runtime.rs](/data/systems/llm/src/app/runtime.rs) 中新增或抽离：

- `build_indicator_summary_from_bundle`
- `load_workflow_state`
- `load_entry_snapshot_for_context`
- `should_run_stage1`
- `invoke_stage1_workflow`
- `invoke_stage2_workflow`
- `handle_stage1_reevaluation_request`
- `persist_workflow_state`
- `persist_entry_snapshot_for_context`

并逐步删除当前：

- `invoke_models_scan_stage` 驱动的旧 stage1 scan 流程
- `management_mode / pending_order_mode` 路由判断
- `load_entry_freshness_recheck_snapshot`
- `evaluate_entry_freshness_recheck`

---

## 11. provider 与 prompt 管线改造

### 11.1 Provider 分层

当前 [provider.rs](/data/systems/llm/src/llm/provider.rs) 直接把“scan / finalize / management / pending”写死在调用层。

改造后必须变成两层：

1. 通用 provider 适配层
2. workflow stage 调用层

建议新增：

```text
systems/llm/src/llm/workflow_provider.rs
```

职责：
- `invoke_stage1_models`
- `invoke_stage2_models`

现有 [provider.rs](/data/systems/llm/src/llm/provider.rs) 继续保留底层 provider 适配细节，不再直接承载工作流语义。

### 11.2 Prompt 输入持久化

保留当前 prompt input artifact 机制，但 stage 名称改成：

- `workflow_stage1`
- `workflow_stage2`

不再使用：

- `scan`
- `entry_core`
- `management_core`
- `pending_core`

---

## 12. 配置改造

### 12.1 保留现有配置

保留：

- `llm.request_enabled`
- `llm.default_model`
- `llm.prompt_template`
- `models[].stage1_reasoning`
- `models[].stage2_reasoning`
- `bundle_stale_secs`
- `bundle_execution_stale_secs`

### 12.2 新增 workflow 配置

新增：

```yaml
llm:
  workflow:
    enabled: true
    stage1_refresh_hours: [0, 4, 8, 12, 16, 20]
    stage2_refresh_minutes: [0, 15, 30, 45]
    soft_gate_min_pass:
      A_continuation: 3
      B_reversal: 2
      C_value_return: 2
    state_dir: "systems/llm/state/workflow"
    persist_prompt_inputs: true
    legacy_modes_enabled: false
```

### 12.3 降级为兼容配置

以下配置不再进入主工作流：

- `llm.execution.min_distance_v`
- `llm.execution.min_rr`
- `llm.execution.entry_sl_remap.*`

处理方式：

- 改名移动到 `llm.compatibility.execution_policy`
- 默认关闭
- 主工作流代码路径不得依赖这些字段

---

## 13. 文件级改动清单

### 13.1 新建文件

```text
systems/llm/src/workflow/mod.rs
systems/llm/src/workflow/schema.rs
systems/llm/src/workflow/state.rs
systems/llm/src/workflow/predicate.rs
systems/llm/src/workflow/code_layer.rs
systems/llm/src/workflow/stage1.rs
systems/llm/src/workflow/stage2.rs
systems/llm/src/workflow/parser.rs
systems/llm/src/workflow/management.rs
systems/llm/src/workflow/persistence.rs
systems/llm/src/execution/intent_adapter.rs
systems/llm/src/llm/workflow_provider.rs
systems/llm/src/llm/prompt/workflow_stage1.rs
systems/llm/src/llm/prompt/workflow_stage2.rs
systems/llm/src/llm/prompt/workflow_stage1/base.txt
systems/llm/src/llm/prompt/workflow_stage2/base.txt
```

### 13.2 修改文件

| 文件 | 修改内容 |
|---|---|
| [main.rs](/data/systems/llm/src/main.rs) | 挂载 `workflow` 模块 |
| [runtime.rs](/data/systems/llm/src/app/runtime.rs) | 改造成新 workflow 编排主线 |
| [config.rs](/data/systems/llm/src/app/config.rs) | 增加 workflow 配置，降级旧 gate 参数 |
| [provider.rs](/data/systems/llm/src/llm/provider.rs) | 收敛为底层 provider 适配层 |
| [prompt.rs](/data/systems/llm/src/llm/prompt.rs) | 切换到 `workflow_stage1 / workflow_stage2` |
| [decision.rs](/data/systems/llm/src/llm/decision.rs) | 迁移为 workflow parser，或拆分并逐步废弃 |
| [scan.rs](/data/systems/llm/src/llm/filter/scan.rs) | 仅保留可复用裁剪函数，主入口迁出 |
| [core.rs](/data/systems/llm/src/llm/filter/core.rs) | 仅保留可复用函数，主入口迁出 |
| [core_entry.rs](/data/systems/llm/src/llm/filter/core_entry.rs) | 复用部分裁剪逻辑后下线 |
| [core_management.rs](/data/systems/llm/src/llm/filter/core_management.rs) | 复用部分裁剪逻辑后下线 |
| [core_pending.rs](/data/systems/llm/src/llm/filter/core_pending.rs) | 从主工作流移除 |
| [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs) | 作为基础函数库保留 |
| [binance.rs](/data/systems/llm/src/execution/binance.rs) | 新增对 `ExecutionIntent / ManagementAction` 的适配入口 |

### 13.3 计划废弃文件

以下文件在 workflow v1 稳定后可以进入废弃流程：

- [prompt/entry.rs](/data/systems/llm/src/llm/prompt/entry.rs)
- [prompt/management.rs](/data/systems/llm/src/llm/prompt/management.rs)
- [prompt/pending_order.rs](/data/systems/llm/src/llm/prompt/pending_order.rs)
- [prompt/scan.rs](/data/systems/llm/src/llm/prompt/scan.rs)
- `prompt/entry/*`
- `prompt/management/*`
- `prompt/pending_order/*`
- `prompt/scan/*`

---

## 14. 实施顺序

### 阶段 1：引入新 schema 和 state

完成项：
- 新建 `workflow/schema.rs`
- 新建 `workflow/state.rs`
- 新建 `workflow/persistence.rs`
- 新增 state 目录落盘

验收标准：
- 项目可编译
- 可以序列化/反序列化 `Stage1Output`、`Stage2Decision`
- 可以按 symbol 读写 `WorkflowState`
- 可以在同一 symbol 下按 `context_key` 分别读写多个 `EntrySnapshot`

### 阶段 2：落地代码层

完成项：
- 新建 `workflow/code_layer.rs`
- 从 raw bundle 生成 `indicator_summary`
- 接入 `tracked_zones / zone_states / recent_15m_bars`

验收标准：
- 输入当前真实 bundle，可产出完整 `IndicatorSummary`
- `options_surface` 仅出现在 `aux_context`
- `auction_context` 三项齐全

### 阶段 3：落地 Stage1

完成项：
- 新建 Stage1 prompt
- 新建 Stage1 parser
- runtime 增加 Stage1 4h/刷新调度

验收标准：
- Stage1 只输出一个 `current_script`
- Stage1 只输出一个 `current_path`
- `failure_switch` 只能是剧本名

### 阶段 4：落地 Stage2

完成项：
- 新建 Stage2 prompt
- 新建 Stage2 parser
- runtime 接入统一 Stage2 输入

验收标准：
- 不再区分 entry / management / pending 三种 LLM prompt
- Stage2 主决策只能 `WAIT / EXECUTE / REQUEST_STAGE1_REEVALUATION`
- `management_actions[]` 是独立附带通道，不是第四种主决策
- Stage2 不得直接切 path

### 阶段 5：接入执行引擎

完成项：
- 新建 `execution/intent_adapter.rs`
- 将 `ExecutionIntent / ManagementAction` 对接到 Binance 执行器
- 下线 pending-order LLM 主路径

验收标准：
- `EXECUTE` 能转成真实下单调用
- 携带 `management_actions[]` 的 Stage2 输出能转成真实管理调用
- 没有 `pending_order_mode` 参与主工作流判断

### 阶段 6：清理旧逻辑

完成项：
- 关闭 `entry freshness recheck`
- 关闭 `entry_sl_remap`
- 关闭 `min_rr` / `min_distance_v` 主链 gate
- 清理旧 prompt 路由

验收标准：
- 主工作流只走 v1.3.1 规定的 Stage1 / Stage2 / ExecutionEngine
- 旧逻辑仅能在 compatibility 开关下存在

---

## 15. 验收与测试

### 15.1 单元测试

必须新增：

- `workflow/schema.rs` 的 roundtrip 测试
- `workflow/predicate.rs` 的 acceptance / failed auction / freshness / ordering 测试
- `workflow/parser.rs` 的 Stage1 / Stage2 输出解析测试
- `workflow/persistence.rs` 的 state 读写测试
- `workflow/persistence.rs` 的多 `context_key` `EntrySnapshot` 不互相覆盖测试
- `workflow/parser.rs` 的 `management_plan` 合同校验测试
- `workflow/stage2.rs` 的 `hard_gate / soft_gate` 字段级判定测试
- `execution/intent_adapter.rs` 的 `ExecutionIntent / ManagementAction` 线协议校验测试
- `workflow/parser.rs` 的 `current_path.id ↔ execution_intent.path_id` 一致性测试
- `workflow/parser.rs` 的 `management_actions[].context_key ↔ path_id ↔ persisted entry_snapshot` 一致性测试
- `workflow/stage2.rs` 的 `failure_level` 命中后 `REQUEST_STAGE1_REEVALUATION + 可选 FLATTEN_POSITION` 测试

### 15.2 运行时测试

必须新增：

- Stage1 调度只在 4h 边界或 refresh 时触发
- Stage2 每 15m 都会跑
- Stage2 请求重评后，下一个 15m cycle 会触发 Stage1
- 持仓存在时，Stage2 即使不开新仓也会输出 `management_actions[]`

### 15.3 v1 暂不纳入理论回放

根据 2026-03-27 的需求确认，v1 冻结版不把理论回放作为当前交付的验收项目。

原因不是放弃验证，而是先把内核、合同、代码主链收敛正确，再在下一个版本单独补：

1. 趋势延续
2. V 型反转
3. 区间震荡
4. 假突破
5. 无边缘震荡

当前版本因此也不引入：
- shadow mode 对比
- 新旧系统决策差异统计
- 额外 KPI 体系
- 理论回放通过率指标

### 15.4 建议执行命令

```bash
cd /data
cargo test -p llm
cargo build -p llm
```

如果需要单独验证工作流模块：

```bash
cd /data
cargo test -p llm workflow::
```

---

## 16. 最终落地定义

只有同时满足以下条件，才算本次改造完成：

1. 代码主链已经从旧 `scan + finalize + management/pending` 切换到新 `代码层 + Stage1 + Stage2 + 执行引擎`
2. Stage1 只输出一个当前主剧本和一个当前 path
3. Stage2 只能等待、执行当前 path、或请求 Stage1 重评
4. 执行层只负责 1m / 100ms 执行优化与订单管理
5. 主链中已经没有多 path 并行、Stage2 自主切 path、固定 RR gate、pending-order LLM 模式这些旧逻辑

一句话定义本方案：

`不是在当前代码上打补丁，而是把当前 llm 层的主状态机整体收敛到 v1.3.1 的双层分离版。`
