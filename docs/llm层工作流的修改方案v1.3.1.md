# LLM层工作流修改方案 v1.3.1（双层分离版）

基于 [订单流交易员交易流程V1.md](/data/docs/订单流交易员交易流程V1.md) 收敛得到的主文档。

本文将原先混在一起的两类内容强制拆开：
- `内核版`：100%忠于原始交易工作流，不额外发明策略规则
- `实施版`：只补落地代码所需的工程合同、接口和状态表达，不改写内核

如果实施版与内核版冲突，以内核版为准。

---

## 文档定位

这份文档只回答两件事：

1. 顶级订单流交易员的标准工作流到底是什么
2. 这个工作流怎样被 LLM 层和代码层准确表达，而不在实现过程中偷偷变成另一套策略

这意味着：
- 本文保留执行所必需的结构化字段、接口和状态机
- 本文移除或降级所有会改变原始交易方法的“新增规则”
- 本文不把工程便利性包装成交易内核

---

## 第一部分：工作流内核版（100%忠于原文）

### 1. 内核总原则

唯一允许的决策顺序是：

`位置 → 状态 → 驱动 → 触发 → 执行`

高质量交易的内核不是“多看几个指标”，而是按顺序回答五个问题：
- 价格现在在哪儿
- 市场现在是什么状态
- 是谁在推动
- 关键位上有没有确认
- 这笔单的失效点和目标点是否清楚

任何实现都不得改变这个顺序。

### 2. 第一步：先做 1D / 4H 地图，不找进场

先用位置层画出结构地图，只做位置判断，不做进场判断。

必须画出的核心结构：
- 1D / 4H 的 `POC / VAH / VAL / HVN / LVN`
- 1D session 的 `TPO POC / IB / Single Prints`
- 关键锚点 `AVWAP`
- 4H / 1D `RVWAP ±1σ / ±2σ`
- 主要爆仓密度峰值区
- 4H / 1D `EMA100 / EMA200 regime`

地图阶段只做一件事：把价格分成三类。
- 在价值区内
- 在价值区边缘
- 已离开价值区且过度延伸

如果价格在区间中间，通常直接不做。

### 3. 第二步：再看 4H 状态，只选一种当前剧本

4H 只允许选三种剧本中的一种作为当前主剧本：
- `延续`
- `拥挤反转`
- `回归价值`

三个剧本的定义保持原文含义：

`延续`
- 价格与 OI 同向扩张
- ratio 没到极端拥挤
- funding / VPIN 没明显反噬
- 趋势结构顺着 EMA100 / EMA200

`拥挤反转`
- 价格到 1D / 4H 极限位置
- ratio 拥挤
- funding 偏一边
- OI 继续堆积但价格推进效率下降

`回归价值`
- 价格离开 value
- 但没有形成真正接受
- 准备回 `POC / HVN / AVWAP / RVWAP`

这里最关键的不是“看多还是看空”，而是当前 move 的仓位属性。

原始内核要求：
- 一个时刻只有一个当前主剧本
- 不允许把多个剧本同时当成并行活跃决策对象

### 4. 剧本切换规则

剧本不是选了就锁死，但也不是 15m 可以随意改写。

只有当以下三类证据同时成立时，旧剧本才被推翻，必须重评：
- 价格到达 1D / 4H 极限位置
- 触发层已确认反向信号
- 驱动层出现变化

这一步的本质是：
- 旧剧本失效
- 必须重选剧本

不是：
- 15m 自己发明新剧本
- 15m 在未重评的前提下直接切到另一套交易逻辑

### 5. 第二点五步：每个当前剧本都必须落成一个 scenario/path object

当前主剧本必须被写成一个可执行路径对象，而不是抽象标签。

一个合格的 scenario/path object 至少回答六件事：
- `thesis`
- `activation_level`
- `first_path_target`
- `next_path_target`
- `failure_level`
- `failure_switch`

内核要求：
- 这些字段必须价格化
- 必须绑定到 1D / 4H 结构位
- `failure_switch` 表达的是“如果当前剧本失效，下一优先重评哪个替代剧本”
- `failure_switch` 不是“另一个同时处于激活监控中的并行主剧本”

### 6. 第三步：做 4H 驱动归因

驱动层只回答三件事：
- `CVD / divergence` 是否有效
- 现货是否同方向确认
- 当前更像 `spot_led / futures_led / mixed`

如果价格在涨，但只有合约侧在推，现货不确认，这波更像脆弱延续。

如果现货主导、合约没跟上，更容易出现后续追价和加速。

### 7. 第四步：15m 只等 setup，不再改剧本

15m 只做 setup 检查，不负责重写交易逻辑。

只允许等待三类 setup：

`A. 延续单`
- 价格先到 1D / 4H 边缘位或再接受位
- initiation
- footprint stacked imbalance
- OBI / OFI / microprice 同向
- spot_confirm = true
- fake_order_risk 低
- OI 状态仍支持

`B. 反转单`
- 只在 1D / 4H 极限结构位寻找
- absorption 或 exhaustion
- 有效 divergence
- 现货不再继续推原方向
- footprint 出现失衡失败 / 拍卖未完成 / 主动单推进不了价格

`C. 回归价值单`
- 先突破 value 外沿
- 但没有得到 OI 扩张、spot 确认、持续 OFI 支持
- 收回 value 内
- 目标通常回中枢，而不是追趋势末端

内核边界：
- 15m 不负责重选当前主剧本
- 15m 只负责确认“当前路径是否开始兑现”

### 8. 第五步：1m / 100ms 只负责“怎么进”

1m / 100ms 只拿来做执行优化：
- 看 footprint 最后一脚
- 看 OFI / microprice / OBI 临门一脚
- 用 intrabar POC 找更好成交与更紧失效点

1m 是 `execution frame`，不是 `decision frame`。

### 9. 第六步：出场和管理

出场和管理的内核不是固定 RR。

第一目标通常放在最近的结构位：
- `POC / HVN / LVN`
- `AVWAP`
- `RVWAP ±1σ / ±2σ`
- `TPO single print`
- 爆仓密度峰值区

持仓中最重要的是看驱动有没有变。

如果这笔单原本是：
- `spot_led + initiation`

后来变成：
- `OI 缩`
- `spot 不跟`
- `fake_order_risk` 上升

即使没到硬止损，也可以减仓或离场。

### 10. 硬过滤器与软过滤器

`Hard gate`
- 位置够好
- 触发已确认

`Soft gate`
- 状态清楚
- 驱动清楚
- 盘口真实
- 失效点明确

实现层可以把这些条件结构化，但不得篡改其含义。

### 11. 内核禁止事项

以下内容不属于工作流内核，不得伪装成内核规则：
- 同时运行多个并行主剧本
- Stage2 在 15m 自主切换到另一条 path 并直接交易
- 将 `failure_switch` 实现为“当前可并行交易的备份路径”
- 把固定 RR 阈值写成策略内核
- 把固定仓位排他政策写成策略内核
- 把 `options_surface` 之类辅助信息写成主过滤器
- 把“通常不做”硬写成唯一正确的阈值块

---

## 第二部分：实施版（只补必要工程合同）

### 1. 实施版总原则

实施版只做四件事：
- 把内核里的判断对象变成可传输的数据结构
- 把内核里的时序关系变成可执行的接口合同
- 把 15m、1m、100ms 的职责边界落到代码
- 保证 Stage2 不越权重写 Stage1 的剧本选择

实施版不得做三件事：
- 新增原文没有定义的交易规则
- 用工程方便替代交易逻辑
- 让低时框组件夺走高时框的决策权限

### 2. 总体架构

```text
indicator_engine
    ↓
代码层（15m，无LLM）
    ↓
Stage1（4H 或显式刷新，LLM）
    ↓
Stage2（15m，LLM）
    ↓
执行引擎（1m / 100ms，无LLM）
```

职责边界：
- `代码层`：只压缩和标准化数据，不做主观分析
- `Stage1`：做地图、选当前主剧本、输出当前 path object、给出驱动归因
- `Stage2`：检查当前 path 是否开始兑现；若旧剧本被推翻，则请求 Stage1 重评
- `执行引擎`：只负责下单优化和订单管理

### 3. 代码层合同

代码层输出 `indicator_summary`，只提供两类信息：
- 原始数值
- 可审计的确定性标签

必须按四层输出：
- `位置层`
- `状态层`
- `驱动层`
- `触发层`

必须额外输出三类执行必需数据：
- `confirmed_at`
- `confirmed_price`
- `auction_context`

推荐合同如下：

```json
{
  "meta": {
    "symbol": "ETHUSDT",
    "ts": "2026-03-26T18:30:00Z",
    "current_price": 2040.88
  },
  "位置层": {},
  "状态层": {},
  "驱动层": {},
  "触发层": {},
  "auction_context": {
    "tracked_zones": [],
    "zone_states": [],
    "recent_15m_bars": []
  },
  "aux_context": {
    "options_surface": {}
  }
}
```

代码层约束：
- 不输出 LLM 主观结论
- 所有事件必须带 `confirmed_at` 与 `confirmed_price`
- acceptance / reacceptance / failed auction 必须绑定具体 zone
- `options_surface` 如保留，只能放入 `aux_context`，不得直接写成内核 gate

### 4. Stage1 合同

Stage1 只执行内核的第 1、2、2.5、3 步。

#### 4.1 Stage1 输入

```json
{
  "task": "执行地图、剧本选择、path object 构建、驱动归因",
  "indicator_summary": {},
  "previous_stage1_output": {},
  "refresh_reason": "scheduled_4h | thesis_invalidated | no_edge_reentered"
}
```

#### 4.2 Stage1 输出

Stage1 只能输出一个当前主剧本和一个当前 path object。

```json
{
  "meta": {
    "stage1_ts": "2026-03-26T16:00:00Z"
  },
  "monitoring_status": "active | no_edge",
  "no_trade_reason": null,
  "refresh_hints": [],
  "map_summary": {
    "price_location_class": "inside_value_middle | value_edge | outside_value_extended",
    "key_levels": {}
  },
  "current_script": "continuation | crowded_reversal | value_return | null",
  "driver_attribution": {
    "flow_driver": "spot_led | futures_led | mixed",
    "spot_confirming": true,
    "driver_note": ""
  },
  "current_path": {
    "id": "path_current",
    "thesis": "",
    "activation_level": {},
    "first_path_target": {},
    "next_path_target": {},
    "failure_level": {},
    "failure_switch": "continuation | crowded_reversal | value_return | null",
    "setup_type": "A_continuation | B_reversal | C_value_return",
    "reevaluation_trigger": {
      "extreme_location": {},
      "reverse_confirmation": {},
      "driver_change": {}
    },
    "management_plan": {},
    "tracked_zones": []
  }
}
```

#### 4.3 Stage1 约束

- `monitoring_status = active` 时，必须有且仅有一个 `current_path`
- `monitoring_status = no_edge` 时，`current_script = null` 且 `current_path = null`
- `failure_switch` 是下次重评的优先候选，不是并行活跃 path
- `reevaluation_trigger` 只是把原文“极限位置 + 反向确认 + 驱动变化”结构化，供 Stage2 请求刷新使用
- Stage1 不得输出多条并行 path 供 Stage2 选择

### 5. Stage2 合同

Stage2 只执行内核的第 4 步和第 6 步，并负责把第 5 步交给执行引擎。

#### 5.1 Stage2 输入

```json
{
  "task": "执行当前path检查、setup确认、输出execution_intent、执行持仓管理",
  "indicator_summary": {},
  "stage1_output": {},
  "active_positions": [],
  "account": {}
}
```

#### 5.2 Stage2 的唯一权限

Stage2 只有三种合法动作：
- `继续等待当前 path`
- `基于当前 path 输出 execution_intent`
- `请求 Stage1 重评`

Stage2 明确没有的权限：
- 不能自己选择新主剧本
- 不能自己实例化 `failure_switch`
- 不能把 alternate path 当场激活并直接交易

#### 5.3 Stage2 执行顺序

`第一步：处理 no-edge 状态`
- 如果 `monitoring_status = no_edge`，本轮不开新仓
- 仅检查 `refresh_hints` 是否触发重新进入条件
- 如触发，则请求 Stage1 刷新

`第二步：检查当前剧本是否被推翻`
- 先检查 `failure_level`
- 再检查 `reevaluation_trigger`
- 任一成立：本轮禁止基于旧 path 新开仓，并请求 Stage1 重评

`第三步：只有在当前剧本仍有效时，才检查 activation_level`
- activation 未满足：继续等待
- activation 满足：进入 setup 检查

`第四步：按 setup_type 检查 15m setup`
- `A_continuation`：检查 initiation / imbalance / OBI / OFI / spot_confirm / fake_order_risk / OI 支持
- `B_reversal`：检查 absorption 或 exhaustion / divergence / 现货不再同向推动 / footprint 失败信号
- `C_value_return`：检查 failed auction / 回收 value / 缺失 OI 与 spot 支持

`第五步：应用 hard gate 和 soft gate`
- hard gate：位置够好、触发已确认
- soft gate：状态清楚、驱动清楚、盘口真实、失效点明确

`第六步：输出 execution_intent`
- 只有当前 path 通过上面所有检查时，才能输出

`第七步：独立执行持仓管理`
- 不论本轮是否有新开仓，已有持仓都要按 `management_plan` 管理
- 管理核心仍然是：驱动有没有变

### 6. 执行引擎合同

执行引擎只承接内核第 5 步。

#### 6.1 输入

```json
{
  "execution_intent": {
    "side": "LONG | SHORT",
    "intent_mode": "immediate | pullback | breakout",
    "entry_zone": {},
    "trigger_price": null,
    "stop_loss": 0,
    "take_profit_1": 0,
    "take_profit_2": 0,
    "ttl_minutes": 15,
    "max_drift_pct": 0.3,
    "path_id": "path_current",
    "entry_snapshot": {}
  },
  "management_action": {
    "type": "HOLD | REDUCE_POSITION | FLATTEN_POSITION | MOVE_STOP | UPDATE_TAKE_PROFIT"
  },
  "broker_state": {},
  "realtime_data": {}
}
```

#### 6.2 职责

执行引擎只能做这些事：
- 用 1m / 100ms 数据优化成交
- 在 `ttl_minutes` 内执行意图
- 下单后挂止损和止盈
- 按 `management_action` 修改订单或仓位

执行引擎不得做这些事：
- 判断方向
- 判断是否值得做
- 新增管理规则
- 改写 Stage1 / Stage2 的状态机

### 7. 必要的结构化谓词

为了让实施版能准确表达内核，允许保留以下结构化谓词：
- `zone_acceptance_above`
- `zone_acceptance_below`
- `reaccept_inside_value`
- `failed_auction_confirmed`
- `price_above_on_close`
- `price_below_on_close`

允许保留以下事件约束：
- `max_age_minutes`
- `near_level`
- `event_after_precondition`

这些谓词的作用只有一个：
- 把“确认发生在对的位置、对的时间、对的顺序”写成代码可判定的条件

它们不是新增交易逻辑，只是原文触发要求的结构化表达。

### 8. 管理计划合同

`management_plan` 必须直接服务于原文第 6 步，不得另起炉灶。

推荐最小合同：

```json
{
  "take_profit_1_basis": "first_path_target",
  "take_profit_2_basis": "next_path_target",
  "stop_migration_rules": [],
  "reduce_on_driver_deterioration": [],
  "exit_full_on": []
}
```

约束：
- 目标位必须来自结构位，不来自随意 RR
- 减仓 / 退出的核心条件必须围绕驱动恶化
- 如果需要移损，应当在目标位兑现后进行

### 9. 从旧版正文中移出的内容

以下内容从本版主文档正文中移出，不再视为标准工作流的一部分：
- `多 path 并行评估`
- `Stage2 switch_predicate 后直接切 path`
- `position_policy / position_transition`
- `RR >= 1.5` 之类固定数值门槛
- `obstacles_to_target` 的固定数量阈值
- `market_tradeable=false` 的固定全满足阈值块
- `options_surface` 对剧本和 gate 的权重调节规则

如果未来确实需要这些东西：
- 应单独放到 `风险配置`、`账户约束`、`执行参数` 或 `研究附录`
- 不得写回工作流内核正文

### 10. 内核与实施的映射表

| 原始工作流步骤 | 内核要求 | 实施层表达 |
|---|---|---|
| 第1步：1D/4H 地图 | 只画位置，不找进场 | `代码层位置层 + Stage1.map_summary` |
| 第2步：选当前剧本 | 只选一个当前主剧本 | `Stage1.current_script` |
| 第2.5步：scenario/path object | 六个价格化字段 | `Stage1.current_path` |
| 第3步：驱动归因 | spot / futures / mixed | `Stage1.driver_attribution` |
| 第4步：15m 等 setup | 不改剧本，只确认当前 path | `Stage2.setup_check` |
| 剧本切换 | 旧剧本失效后必须重评 | `Stage2.request_stage1_refresh` |
| 第5步：1m / 100ms 执行 | 只负责怎么进 | `execution_intent + 执行引擎` |
| 第6步：管理与出场 | 结构位目标 + 驱动变化管理 | `management_plan + management_action` |

---

## 结论

本版 v1.3.1 的定义是：
- `内核版` 保证对 [订单流交易员交易流程V1.md](/data/docs/订单流交易员交易流程V1.md) 的忠实性
- `实施版` 只做必要结构化表达，不再把工程规则升级成交易规则

因此，后续如果要基于本文件继续写：
- 代码实施方案
- LLM prompt 方案
- schema 设计
- 调度与状态机设计

都必须遵守一个顺序：

先证明“没有改工作流内核”，再谈“怎样工程化落地”。
