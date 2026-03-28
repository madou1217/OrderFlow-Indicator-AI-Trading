# LLM层工作流修改方案 v2.0.0（战略-战术-执行分层版）

基于 [订单流交易员交易流程V1.md](/data/docs/订单流交易员交易流程V1.md) 重新按第一性原理收敛得到的主文档。

本版相对 `v1.3.1` 的核心变化只有三件事：

- `Stage1` 明确升级为 `3D / 1D / 4H` 地图与主剧本层
- `代码层` 明确接管持续监测、候选事件生成与状态推进
- `Stage2` 不再做“定时等待”，而改为“事件驱动的 path 复核 + entry plan 设计”

如果实施版与内核版冲突，以内核版为准。

---

## 文档定位

这份文档只回答三件事：

1. 顶级订单流交易员的标准工作流到底是什么
2. LLM 在这套工作流里最该承担哪一层判断
3. 哪些判断必须留给代码层，不能继续混在 Stage2 里

这意味着：
- 本文保留执行所必需的结构化字段、接口和状态机
- 本文不把代码侧的持续盯盘伪装成 LLM 判断
- 本文不让低时框组件夺走高时框的剧本选择权

---

## 第一部分：工作流内核版（100%忠于交易员流程）

### 1. 内核总原则

唯一允许的决策顺序是：

`位置 → 状态 → 驱动 → 触发 → 执行`

高质量交易的核心不是“多看几个指标”，而是按顺序回答六个问题：
- 价格现在在哪儿
- 市场现在是什么状态
- 是谁在推动
- 当前主剧本是什么
- 关键位上有没有确认
- 这笔单的失效点和目标点是否清楚

任何实现都不得改变这个顺序。

### 2. 第一步：先做 3D / 1D / 4H 地图，不找进场

地图层先看更高一级背景，再看交易主时框。

必须画出的核心结构：
- `3D / 1D / 4H` 的 `POC / VAH / VAL / HVN / LVN`
- `1D session` 的 `TPO POC / IB / Single Prints`
- 关键锚点 `AVWAP`，最少保留 `7D / 3D / 1D / 4H`
- `RVWAP ±1σ / ±2σ`，战略层最少看 `1D / 4H`，战术层补 `15m`
- 主要爆仓密度峰值区
- `3D / 1D / 4H` 的 `EMA100 / EMA200 regime`

地图阶段只做一件事：把价格分成三类。
- 在价值区内
- 在价值区边缘
- 已离开价值区且过度延伸

这里的职责边界是：
- `3D` 用来定义更外层 regime、外层 value 和大背景约束
- `1D / 4H` 用来定义当前可交易结构与可执行路径
- 地图层不负责进场，不负责具体执行

### 3. 第二步：再看 4H / 1D 状态，只选一种当前主剧本

当前主剧本仍然只允许三选一：
- `延续`
- `拥挤反转`
- `回归价值`

剧本选择的主轴仍然是 `4H`，但必须参考 `1D` 的状态一致性，并受 `3D` 背景约束。

三个剧本的定义保持原文含义：

`延续`
- 价格与 OI 同向扩张
- ratio 没到极端拥挤
- funding / VPIN 没明显反噬
- 趋势结构顺着 EMA100 / EMA200

`拥挤反转`
- 价格到 `1D / 4H` 极限位置
- ratio 拥挤
- funding 偏一边
- OI 继续堆积但价格推进效率下降

`回归价值`
- 价格离开 value
- 但没有形成真正接受
- 准备回 `POC / HVN / AVWAP / RVWAP`

这里最关键的不是“多空”，而是当前 move 的仓位属性。

原始内核要求：
- 一个时刻只有一个当前主剧本
- 不允许把多个剧本同时当成并行活跃决策对象
- `3D` 只能约束和定性背景，不能额外创建第四种剧本

#### 3.1 多时框冲突裁决规则

当 `3D / 1D / 4H` 的方向或状态解释出现冲突时，不允许模型自由发挥，必须按以下顺序裁决：

- 如果 `4H` 与 `1D` 同向，正常选择当前主剧本
- 如果 `4H` 逆 `1D`，只有当价格已到 `1D / 4H` 极限位置，并且当前解释属于 `crowded_reversal` 或 `value_return` 时，才允许保留该 path
- 如果 `4H` 同时逆 `1D` 和 `3D`，该 path 只能被写成短程修复 path，不得写成趋势 `continuation`
- 如果没有极限位置、没有足够触发、只是中间区域的方向打架，直接输出 `no_edge`

这里的含义是：
- `4H` 仍然是主剧本选择轴心
- `1D` 负责判断这个 `4H` 剧本是否属于“顺主状态”还是“逆状态但可交易”
- `3D` 负责给出更外层 regime 约束和风险边界

因此，`3D` 不直接投票选剧本，但能限制：
- 当前 path 是否只能作为短程修复
- 当前 target corridor 是否必须收敛
- 当前 path 是否应直接降级为 `no_edge`

#### 3.2 风险等级与目标约束

每个 `Stage1` 战略 path 都必须带一个 `risk_grade`：
- `aligned_trend`
- `countertrend_repair`
- `high_conflict_repair`

判级规则如下：
- `aligned_trend`：`4H` 与 `1D` 同向；如果 `3D` 反向，仍可保留为 `aligned_trend`，但必须降低风险上限并收窄 `target corridor`
- `countertrend_repair`：`4H` 逆 `1D`，但已到 `1D / 4H` 极限位置，且剧本只能是 `crowded_reversal` 或 `value_return`
- `high_conflict_repair`：`4H` 同时逆 `1D` 和 `3D`，或虽然可做但只能作为短程修复 path

风险等级必须和目标约束绑定：
- `aligned_trend`：可以给正常 `target corridor`，但若 `3D` 反向则必须收窄 target，并降低风险上限
- `countertrend_repair`：目标优先看回归价值区、`POC / HVN / AVWAP / RVWAP` 中枢，不应直接写成远端趋势目标
- `high_conflict_repair`：只能给短程修复目标，不得写成趋势 `continuation`，也不得写远端趋势目标

### 4. 第三步：做 4H / 1D 驱动归因

驱动层只回答三件事：
- `CVD / divergence` 是否有效
- 现货是否同方向确认
- 当前更像 `spot_led / futures_led / mixed`

驱动归因的主轴是 `4H`，但必须参考 `1D` 同方向背景。

如果价格在涨，但只有合约侧在推，现货不确认，这波更像脆弱延续。

如果现货主导、合约没跟上，更容易出现后续追价和加速。

### 5. 第三点五步：每个当前主剧本都必须落成一个战略 path object

当前主剧本必须被写成一个可执行路径对象，而不是抽象标签。

一个合格的战略 path object 至少回答七件事：
- `thesis`
- `risk_grade`
- `activation_level`
- `first_path_target`
- `next_path_target`
- `failure_level`
- `failure_switch`

内核要求：
- 这些字段必须价格化
- 必须绑定到 `3D / 1D / 4H` 结构位，其中交易锚点以 `1D / 4H` 为主
- `failure_switch` 表达的是“如果当前剧本失效，下一优先重评哪个替代剧本”
- `failure_switch` 不是“另一个同时处于激活监控中的并行主剧本”
- `risk_grade` 必须和 `target corridor`、风险上限、以及是否允许远端目标直接绑定

补充边界：
- `activation_level` 与 `failure_level` 属于战略层，不是执行层可随意改写的字段
- 后续低时框只能在当前 path 之内细化 entry plan，不能改写当前主剧本

### 6. 第四步：15m 只做触发确认与战术入场设计，不再重选剧本

`15m` 的任务有且只有两件事：
- 确认当前 path 是否在低时框上开始兑现
- 在当前 path 仍然成立时，设计更合适的战术入场方案

`15m` 不允许做这些事：
- 重选当前主剧本
- 激活 `failure_switch`
- 在未重评的前提下切到另一条 path

15m 仍然只允许围绕三类 setup 工作：

`A. 延续单`
- 价格先到 `1D / 4H` 边缘位或再接受位
- initiation
- footprint stacked imbalance
- OBI / OFI / microprice 同向
- spot_confirm = true
- fake_order_risk 低
- OI 状态仍支持

`B. 反转单`
- 只在 `1D / 4H` 极限结构位寻找
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
- `15m` 不负责持续盯盘
- `15m` 不负责“等待”
- `15m` 只在代码层已经捕获到候选机会时，复核当前 path 是否值得给出入场方案

### 7. 第五步：1m / 100ms 只负责“怎么进”

`1m / 100ms` 只拿来做执行优化：
- 看 footprint 最后一脚
- 看 OFI / microprice / OBI 临门一脚
- 用 intrabar POC 找更好成交与更紧执行止损

`1m` 是 `execution frame`，不是 `decision frame`。

### 8. 第六步：出场和管理

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

### 9. 硬过滤器与软过滤器

`Hard gate`
- 位置够好
- 触发已确认

`Soft gate`
- 状态清楚
- 驱动清楚
- 盘口真实
- 失效点明确

实现层可以把这些条件结构化，但不得篡改其含义。

### 10. 内核禁止事项

以下内容不属于工作流内核，不得伪装成内核规则：
- 同时运行多个并行主剧本
- `Stage2` 固定每 `15m` 心跳式等待并输出 `WAIT`
- `Stage2` 持续监测价格是否站回 / 跌破
- `Stage2` 直接修改当前 `current_script`
- `Stage2` 放宽 `Stage1.failure_level`
- 将 `failure_switch` 实现为并行活跃备份 path
- 把固定 RR 阈值写成策略内核
- 把 `options_surface` 之类辅助信息写成主过滤器

---

## 第二部分：实施版（只补必要工程合同）

### 1. 实施版总原则

实施版只做五件事：
- 把内核里的判断对象变成可传输的数据结构
- 把 `3D / 1D / 4H` 与 `15m / 1m / 100ms` 的职责边界落到代码
- 把持续监测与候选事件生成从 LLM 中剥离出来
- 让 `Stage2` 只在事件触发时运行，而不是定时轮询
- 保证 `Stage2` 只能细化 entry plan，不能改写战略剧本

实施版不得做三件事：
- 新增原文没有定义的交易规则
- 用工程方便替代交易逻辑
- 让低时框组件夺走高时框的决策权限

### 2. 总体架构

```text
indicator_engine
    ↓
代码层（压缩与标准化，无LLM）
    ↓
Stage1（4H 或显式刷新，LLM）
    ↓
path watcher / candidate engine（事件驱动，无LLM）
    ├─ entry_candidate / path_review_candidate → Stage2（事件驱动，LLM）
    ├─ hard_invalidation → Stage1 refresh
    └─ tp / stop / management_event → 执行与仓位管理
    ↓
执行引擎（1m / 100ms，无LLM）
```

职责边界：
- `代码层`：只压缩和标准化数据，不做主观分析
- `Stage1`：做地图、选当前主剧本、输出战略 path、给出驱动归因与管理锚点
- `path watcher / candidate engine`：持续监测 path 的确定性条件并生成候选事件
- `Stage2`：只在候选事件出现时运行，复核 path，并输出战术 entry plan 或请求重评
- `执行引擎`：只负责成交优化和订单管理

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
  "位置层": {
    "map_3d": {},
    "map_1d": {},
    "map_4h": {}
  },
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
- `options_surface` 如保留，只能放入 `aux_context`
- `options_surface` 必须至少压成 `4H / 1D` 可消费的战略辅助摘要，供 `Stage1` 做 path 设计时参考
- `AVWAP` 数据源最少产出 `7D / 3D / 1D / 4H` 四组锚点
- `RVWAP sigma bands` 最少产出 `15m / 4H / 1D` 三组偏离带

#### 3.1 指标层与 Stage1 / Stage2 输入分配

指标分配必须服务于一个边界：
- `Stage1` 负责战略 path 设计
- `Stage2` 负责战术 entry plan 设计与 path 复核

因此，指标不能按“都给两边一份”处理，而必须按主职责分配。

第一性原理下的分配规则只有三条：
- 凡是会改变 `主剧本 / strategic path / target corridor / failure boundary` 的信息，优先属于 `Stage1`
- 凡是只会改变“当前 path 该怎么进、进得多激进、进在哪一脚”的信息，优先属于 `Stage2`
- 凡是纯确定性的阈值触发、价格穿越、接受/跌破、目标命中，优先属于代码侧 watcher，而不是任何一个 LLM stage

| 指标层 | 指标 | Stage1 用法 | Stage2 用法 |
|---|---|---|---|
| 位置层 | `1 price_volume_structure`、`4 liquidation_density`、`18 AVWAP`、`20 TPO market profile`、`21 RVWAP sigma bands`、`23 EMA trend regime`、`24 FVG/缺口` | `Stage1` 的主输入。用于 `3D / 1D / 4H` 地图、位置分类、主剧本背景、`activation_level / target / failure_level` 的战略锚定。`AVWAP` 最少保留 `7D / 3D / 1D / 4H`。`RVWAP` 在 `Stage1` 里以 `4H / 1D` 为战略锚。 | `Stage2` 只接收与当前 path 直接相关的战术位置切片，不重画全图。最少包含：当前 path 周边的 `1D / 4H` 结构位、当日 `TPO POC / IB / Single Print`、`15m RVWAP`、以及与当前 path corridor 真正相交的 `AVWAP` 锚点。`7D / 3D AVWAP` 只有在它们已进入当前 path 的有效障碍区或目标区时，才进入 `Stage2`。 |
| 状态层 | `16 funding`、`17 VPIN`、`25 open_interest`、`26 long_short_ratios` | `Stage1` 的主输入。用于 `4H / 1D` 状态判断和主剧本选择，回答这波更像 build、unwind、crowding 还是反身风险。只使用确认后的规范桶，不做秒级触发。 | `Stage2` 不接原始状态层全量包，只接 `state_guardrail_snapshot`。它的作用只是复核“当前 tactical entry 是否仍服从战略状态前提”，而不是重新做状态分类。 |
| 驱动层 | `14 CVD pack`、`3 divergence`、`15 whale trades` | `Stage1` 的主输入。用于 `4H / 1D` 驱动归因，判断 `spot_led / futures_led / mixed`，并决定 path 的驱动前提。 | `Stage2` 不接原始驱动层全量包，只接 `driver_guardrail_snapshot` 与 path 相关的最新驱动变化摘要。它的作用是判断“这次 entry 候选是否仍受当前驱动支持”，而不是重做 driver attribution。 |
| 触发层 | `2 footprint`、`5 orderbook_depth`、`6 absorption`、`7 initiation`、`12 buying exhaustion`、`13 selling exhaustion`、`22 high_volume_pulse` | `Stage1` 只接收“战略相关的已确认触发摘要”，例如最近一次导致剧本切换或重画 path 的高质量 confirmed event。`Stage1` 不以这层做战术入场设计。 | `Stage2` 的主输入。`Stage2` 主要依赖这层完成 path 复核和战术 entry plan 设计，包括 `footprint / OBI / OFI / microprice / spot_confirm / fake_order_risk / absorption / initiation / exhaustion` 等。 |
| 辅助层 | `options_surface（期权辅助层）` | `Stage1` 的辅助战略输入。用于 `4H / 1D` path 设计时补充判断：当前 path 上方/下方是否存在显著的期权 pin、gamma wall、dealer positioning、expiry magnet 或 vol surface 异常，从而帮助收敛 `target corridor / failure envelope / risk_grade`。它可以影响 path 质量评估与目标收敛，但不能单独决定主剧本，也不能单独作为内核 gate。 | `Stage2` 默认不接原始期权全量包。只有当当前 path corridor、entry corridor 或 target corridor 明确与关键期权障碍/磁吸区重叠时，才允许接收一个裁剪后的 `options_guardrail_snapshot` 作为战术约束补充；它不得单独触发 path 重评。 |

补充约束：
- `19 kline_history` 只是数据载体，不作为独立打分因子
- `8–11` 只是 `6 / 7` 的方向衍生标签，不单独加权
- `Stage2` 不接收完整 `3D / 1D / 4H` 地图原始包；它继承 `Stage1.map_summary` 和 `Stage1.current_path`，只额外接收与当前 path 直接相关的局部切片与 guardrail snapshot
- `options_surface` 必须以“名称优先”而不是“指标编号优先”写入合同；实现层不应把是否属于 `i27` 这类可漂移编号写死进工作流内核

### 4. Stage1 合同

Stage1 只执行内核的第 1、2、3、3.5 步。

#### 4.1 Stage1 输入

```json
{
  "task": "执行3D/1D/4H地图、主剧本选择、战略path构建、4H/1D驱动归因",
  "strategic_indicator_summary": {},
  "previous_stage1_output": {},
  "refresh_reason": "scheduled_4h | path_invalidated | no_edge_reentered | regime_shift"
}
```

#### 4.1.1 Stage1 输入分配原则

`Stage1` 的输入必须是“战略输入”，不能退化成 15m 触发器。

`Stage1` 必须重点消费：
- 位置层全量战略地图：`PVS / liquidation_density / AVWAP / TPO / RVWAP / EMA / FVG`
- 状态层全量战略快照：`funding / VPIN / OI / long_short_ratios`
- 驱动层全量战略快照：`CVD pack / divergence / whale trades`
- `options_surface` 的 `4H / 1D` 战略辅助摘要：用于补充当前 path 的目标收敛、障碍区识别、失败后磁吸风险和 `risk_grade` 收敛，但不得单独主导主剧本选择

`Stage1` 在做主剧本选择时，必须同时遵守“多时框冲突裁决规则”：
- `4H` 与 `1D` 同向时，正常选剧本
- `4H` 逆 `1D` 时，只有在 `1D / 4H` 极限位置且属于 `crowded_reversal` 或 `value_return` 时才允许
- `4H` 同时逆 `1D` 和 `3D` 时，只能输出短程修复 path，不得写成趋势 `continuation`
- 如果只是中间区域打架且缺少足够触发，直接输出 `no_edge`

其中窗口边界必须明确：
- `AVWAP`：`7D / 3D / 1D / 4H`
- `RVWAP`：战略层只使用 `4H / 1D`
- `EMA regime`：战略层只使用 `4H / 1D`
- `TPO`：战略层以 `1D session` 为主

`Stage1` 可以消费但只能作为辅助上下文的内容：
- 触发层中最近一段“已确认、已绑定到 1D / 4H 关键位”的高质量事件摘要
- 这些摘要只能用于解释为什么当前 path 成立、为什么旧 path 失效、或为什么需要重画战略地图
- `options_surface` 的辅助期权上下文，只能作为 path 设计的加权补充，不能直接替代位置层、状态层或驱动层

`Stage1` 明确不应把以下内容当成主输入：
- 连续滚动的 `15m` orderbook 微结构噪音
- `15m RVWAP` 这类战术偏离带
- 秒级 `OBI / OFI / microprice`
- 只对执行优化有意义的 `1m / 100ms` 末端信息

#### 4.2 Stage1 输出

Stage1 只能输出一个当前主剧本和一个战略 path object。

```json
{
  "meta": {
    "stage1_ts": "2026-03-26T16:00:00Z"
  },
  "monitoring_status": "active | no_edge",
  "no_trade_reason": "conflict_no_edge | script_not_unique | path_not_actionable | null",
  "refresh_hints": [],
  "map_summary": {
    "regime_3d": {},
    "location_1d": {},
    "location_4h": {},
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
    "risk_grade": "aligned_trend | countertrend_repair | high_conflict_repair",
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
- `monitoring_status = active` 不要求当前价格已经接近 `activation_level`；只要战略 path 足够清晰，价格距离较远也应保持 `active + current_path`
- `monitoring_status = no_edge` 的允许原因只包括：
  - `conflict_no_edge`：多时框冲突，且不满足极限位置/足够触发
  - `script_not_unique`：无法收敛成唯一主剧本
  - `path_not_actionable`：即使有方向，也无法落成清晰战略 path
- “当前价格还没走到好位置”不得作为 `Stage1=no_edge` 的理由
- `activation_level` 与 `failure_level` 是战略层字段，不得由 Stage2 直接改写
- `failure_switch` 是下次重评的优先候选，不是并行活跃 path
- `reevaluation_trigger` 是战略剧本被推翻的结构化表达
- Stage1 不得输出多条并行 path 供 Stage2 选择

### 5. Path watcher / candidate engine 合同

这是 `v2.0.0` 的关键新增层。

它的职责不是做主观判断，而是：
- 监测当前 path 的确定性状态
- 维护 path runtime state
- 生成高质量候选事件
- 作为初筛器，只把值得送审的候选机会送给 `Stage2`
- 在 `Stage2` 已给出战术计划后，负责按计划执行主 entry 与备选 re-entry
- 将“等待”从 LLM 中移除

#### 5.1 输入

```json
{
  "stage1_output": {},
  "approved_tactical_plan": {},
  "indicator_summary": {},
  "realtime_ohlc": {},
  "realtime_events": {},
  "active_positions": []
}
```

#### 5.2 必须负责的监测任务

- 是否接近或进入 `activation_level`
- 是否跌破或接受于 `failure_level` 之外
- 是否发生 `reclaim / acceptance / failed_auction / reacceptance`
- 是否命中 `tp1 / tp2`
- 是否触发 `management_plan` 里的确定性规则
- 是否达到 `ttl_minutes / max_drift_pct` 之类执行约束
- 是否命中 `primary_entry_plan / secondary_entry_plan` 的激活条件
- 是否已经用尽同一 `15m` 窗口内允许的实际止损入场次数

#### 5.3 候选事件

推荐最小事件集：
- `entry_candidate`
- `path_review_candidate`
- `hard_invalidation`
- `no_edge_reentered`
- `management_event`

约束：
- `hard_invalidation` 是代码侧可直接判定的失效，不必先经过 Stage2
- `entry_candidate` 必须代表“代码侧已确认出现了值得送审的机会”，而不是纯噪音触碰
- `path_review_candidate` 用于“path 仍未被硬失效，但出现了值得 LLM 复核的 path 审计信号或战术层重排需求”
- 只有在 `entry_candidate` 或 `path_review_candidate` 出现时才调用 `Stage2`

#### 5.4 二次入场与尝试次数边界

watcher 必须支持在同一 `Stage1` 战略 path 下执行二次入场，但边界必须明确：
- `Stage2` 可以一次性给出 `primary_entry_plan` 与 `secondary_entry_plan`
- watcher 只允许在同一个 `15m` 窗口内执行这两层计划
- 最多允许 `2` 次“实际成交后被打掉”的入场次数
- 未成交、漂移超限、超时撤单、setup 失效但未成交，不计入这 `2` 次

这意味着：
- watcher 负责次数统计和时间窗控制
- `Stage2` 负责定义允许的主入场和备选 re-entry
- 超出次数或时间窗后，当前 tactical plan 自动失效，除非 `Stage2` 再次更新
- 一旦 `Stage2` 已确认“path 还活着”，后续战术等待、触发监测、主入场与 re-entry 的执行边际应主要交给 watcher

### 6. Stage2 合同

Stage2 在 `v2.0.0` 中不再是“15m 等待器”，而是“事件驱动的 path 复核器与战术 entry plan 设计器”。

#### 6.1 Stage2 输入

```json
{
  "task": "复核当前path是否仍成立，并在当前path内部生成战术entry_plan",
  "candidate_event": {},
  "path_runtime_state": {},
  "previous_tactical_plan": {},
  "tactical_position_slice": {},
  "latest_15m_trigger_facts": {},
  "state_guardrail_snapshot": {},
  "driver_guardrail_snapshot": {},
  "stage1_output": {},
  "active_positions": [],
  "account": {}
}
```

#### 6.1.1 Stage2 输入分配原则

`Stage2` 的输入必须是“战术复核输入”，不能重新变成战略地图生成器。

`Stage2` 必须重点消费：
- `stage1_output.current_path` 与 `stage1_output.map_summary`
- `candidate_event` 与 `path_runtime_state`
- `latest_15m_trigger_facts`

其中 `latest_15m_trigger_facts` 必须能表达当前战术层是否出现与 path 相反的即时压力，例如：
- 当前 `15m` 卖压 / 买压是否显著与 path 方向相反
- 当前 `15m` 的 `OBI / OFI / microprice / spot_confirm / fake_order_risk`
- 当前战术窗口下的短时 `OI / unwind / cover / pressure shift` 摘要

`Stage2` 需要的战术位置切片应当只包含：
- 当前 path 周边的 `1D / 4H` 关键位
- 当前交易日相关的 `TPO POC / IB / Single Print`
- `15m RVWAP ±σ`
- 与当前 path 最近、且对当前入场真正构成约束的 `4H / 1D AVWAP` 参考锚点
- 如果 `7D / 3D AVWAP` 已进入当前 path corridor 或目标 corridor，也应作为障碍锚点纳入

`Stage2` 只应把以下内容作为 path 复核辅助，而不是重选剧本输入：
- `state_guardrail_snapshot`
- `driver_guardrail_snapshot`
- `options_guardrail_snapshot`：仅当当前 path corridor 与关键期权障碍或磁吸区明确重叠时才提供，用于约束战术 entry，不用于重选剧本

`Stage2` 明确不应接收或依赖：
- 完整 `3D / 1D / 4H` 全图原始包
- 全量位置层重新打分结果
- 原始 `CVD pack / divergence / whale trades` 全量时间序列
- 原始 `funding / VPIN / OI / long_short_ratios` 全量时间序列
- `kline_history` 原始载体
- 与当前 path 无关的广义指标噪音

#### 6.2 Stage2 的唯一权限

Stage2 只有两种合法输出：
- `PATH_CONFIRMED`
- `REQUEST_STAGE1_REEVALUATION`

Stage2 明确没有的权限：
- 不能持续盯盘
- 不能输出 `WAIT`
- 不能自己选择新主剧本
- 不能自己实例化 `failure_switch`
- 不能把 alternate path 当场激活并直接交易
- 不能放宽或改写 `Stage1.failure_level`

#### 6.3 Stage2 真正负责的事情

`Stage2` 的职责排序必须明确：
- 第一优先：作为 `path auditor`，判断当前战略 path 是否仍可信
- 第二优先：只有在 path 仍可信时，才设计更好的 tactical entry

这意味着：
- `Stage2` 可以否决当前 path，并直接输出 `REQUEST_STAGE1_REEVALUATION`
- 这种否决不必等待 `Stage1.failure_level` 被硬击穿
- 但否决理由必须来自 path 审计，而不是临场另起炉灶

#### 6.3.1 Stage2 的 path 审计顺序

`Stage2` 在讨论 entry 之前，必须先完成 path 生死审计，顺序不可颠倒：

`第一步：检查硬失效`
- 如果当前 path 已被 `failure_level` 硬失效，或 watcher 已明确给出 `hard_invalidation`，直接输出 `REQUEST_STAGE1_REEVALUATION`

`第二步：检查软失效`
- 只有当以下三类证据同时成立时，才允许 `Stage2` 在硬失效之前软否决当前 path：
  - `extreme_location`
  - `reverse_confirmation`
  - `driver_change`
- 这三者必须共同构成“旧剧本已被新证据推翻”的 path 审计结论，不能只凭单一 15m 微结构弱化就请求重评

`第三步：只有 path 仍活着，才进入战术设计`
- 只要 `Stage2` 审计结论是“path 还活着”，就应返回 `PATH_CONFIRMED`
- 后续的战术等待、入口深浅、主入场与备选 re-entry 的执行边际，应主要交给 watcher
- `Stage2` 此时负责更新 `tactical_entry_plan`，而不是持续代替 watcher 做等待判断

如果 `path` 仍活着，但当前 `15m` 战术层出现与 path 方向相反的显著压力，例如：
- 当前卖压 / 买压与 path 方向相反
- 当前 `OBI / OFI / microprice / spot_confirm` 不支持原先的主 entry
- 当前短时 `OI` 表现出与 path 相反的拥挤、反向推进或压力堆积

那么 `Stage2` 明确允许：
- 大幅后移或加深 `primary_entry_plan.entry_activation_level`
- 大幅后移或加深 `secondary_entry_plan.entry_activation_level`
- 相应重设更紧的 `entry_invalidation_level`
- 重算执行级 `stop_loss`

这里的原则是：
- 可以大幅修改战术入场点和执行级止损
- 不能改主方向
- 不能改主剧本
- 不能放宽 `Stage1.failure_level`
- 不能越出 `Stage1` 定义的 path envelope

在 `PATH_CONFIRMED` 的前提下，Stage2 负责的是：
- 复核当前战略 path 在最新 `15m` 触发层下是否仍然合理
- 在当前 path envelope 内设计更合适的战术入场方式
- 可以同时重写 `primary_entry_plan` 与 `secondary_entry_plan`
- 给出更细的 `entry_activation_level`
- 给出更紧的 `entry_invalidation_level`
- 选择 `entry_profile / intent_mode / entry_zone`
- 给出执行所需的 `stop_loss / ttl / max_drift`

这里的边界是：
- `entry_activation_level` 是战术入场确认位，不等于改写 `Stage1.activation_level`
- `entry_invalidation_level` 是执行级失效位，不等于改写 `Stage1.failure_level`
- 即使 `Stage2` 因当前 `15m` 反向压力而大幅后移 entry 或重设更深的战术入场点，它也只能调整执行级 `entry_invalidation_level / stop_loss`，不得放宽战略 `failure_level`
- 如果 `Stage2` 是被 `path_review_candidate` 触发，且判断 path 仍然可信，但当前这脚微结构不够好，仍应返回 `PATH_CONFIRMED`，同时更新 `tactical_entry_plan`，让 watcher 继续等待下一脚
- “当前微结构不够好”“当前还没走到执行位置”“当前主入场未触发”都不得单独作为 `REQUEST_STAGE1_REEVALUATION` 的理由

#### 6.4 Stage2 输出

```json
{
  "stage2_decision": "PATH_CONFIRMED | REQUEST_STAGE1_REEVALUATION",
  "tactical_entry_plan": {
    "path_id": "path_current",
    "primary_entry_plan": {
      "entry_profile": "reclaim_then_hold | pullback_acceptance | failed_auction_reentry",
      "intent_mode": "immediate | pullback | breakout",
      "entry_activation_level": {},
      "entry_zone": {},
      "entry_invalidation_level": {},
      "stop_loss": 0,
      "take_profit_1": 0,
      "take_profit_2": 0,
      "ttl_minutes": 15,
      "max_drift_pct": 0.3,
      "entry_snapshot": {},
      "entry_note": ""
    },
    "secondary_entry_plan": {
      "entry_profile": "reclaim_then_hold | pullback_acceptance | failed_auction_reentry",
      "intent_mode": "immediate | pullback | breakout",
      "entry_activation_level": {},
      "entry_zone": {},
      "entry_invalidation_level": {},
      "stop_loss": 0,
      "take_profit_1": 0,
      "take_profit_2": 0,
      "ttl_minutes": 15,
      "max_drift_pct": 0.3,
      "entry_snapshot": {},
      "entry_note": ""
    },
    "attempt_policy": {
      "max_filled_stopout_attempts": 2,
      "count_unfilled_attempts": false,
      "time_window": "same_15m_window"
    }
  },
  "reevaluation_reason": null
}
```

约束：
- `take_profit_1` 与 `take_profit_2` 必须继承自 `Stage1.current_path`
- `PATH_CONFIRMED` 时必须给出完整 `tactical_entry_plan`
- `REQUEST_STAGE1_REEVALUATION` 时 `tactical_entry_plan = null`
- `secondary_entry_plan` 只用于同一战略 path 内的备选 re-entry，不得变相生成第二条战略 path

#### 6.5 为什么 Stage2 只有两个输出

因为“等待”已经被代码侧 watcher 接管。

也就是说：
- 值不值得送审，由代码侧先过滤
- 送到 Stage2 以后，只剩两个问题：
  - 当前 path 还能不能用
  - 如果还能用，应该怎样设计更好的 tactical entry plan

因此 `v2.0.0` 中不再保留 `WAIT / NO_ACTION` 一类输出。

### 7. 执行引擎合同

执行引擎只承接内核第 5 步。

watcher 在这一层之前必须先完成一件事：
- 从 `Stage2.tactical_entry_plan.primary_entry_plan / secondary_entry_plan` 中选出当前要执行的那一条具体 `entry_plan`

#### 7.1 输入

```json
{
  "entry_plan": {
    "side": "LONG | SHORT",
    "intent_mode": "immediate | pullback | breakout",
    "entry_zone": {},
    "entry_activation_level": {},
    "entry_invalidation_level": {},
    "stop_loss": 0,
    "take_profit_1": 0,
    "take_profit_2": 0,
    "ttl_minutes": 15,
    "max_drift_pct": 0.3,
    "path_id": "path_current",
    "entry_snapshot": {}
  },
  "broker_state": {},
  "realtime_data": {}
}
```

#### 7.2 职责

执行引擎只能做这些事：
- 用 `1m / 100ms` 数据优化成交
- 在 `ttl_minutes` 内执行 `entry_plan`
- 下单后挂止损和止盈
- 处理 drift、超时、未成交撤单等执行细节

执行引擎不得做这些事：
- 判断方向
- 判断是否值得做
- 重写 `Stage1 / Stage2` 的状态机

### 8. 仓位管理合同

仓位管理在 `v2.0.0` 中默认由代码侧执行，不再与 `Stage2` 混在一起。

#### 8.1 输入

```json
{
  "management_plan": {},
  "active_positions": [],
  "indicator_summary": {},
  "realtime_data": {}
}
```

#### 8.2 职责

仓位管理层负责：
- 命中 `tp1 / tp2` 后的分批止盈
- 按 `stop_migration_rules` 移损
- 在 `driver_deterioration` 被结构化确认后减仓或离场
- 在战略 `failure_level` 被硬失效时直接退出

约束：
- 目标位必须来自结构位，不来自随意 RR
- 驱动恶化判断必须围绕已结构化的确定性事件
- 如果出现 plan 外的 regime 断裂，可直接请求 `Stage1 refresh`

### 9. 必要的结构化谓词

为了让实施版能准确表达内核，允许保留以下结构化谓词：
- `zone_acceptance_above`
- `zone_acceptance_below`
- `reaccept_inside_value`
- `failed_auction_confirmed`
- `price_above_on_close`
- `price_below_on_close`
- `entry_reclaim_confirmed`
- `entry_hold_confirmed`

允许保留以下事件约束：
- `max_age_minutes`
- `near_level`
- `event_after_precondition`
- `inside_path_envelope`

这些谓词的作用只有一个：
- 把“确认发生在对的位置、对的时间、对的顺序”写成代码可判定的条件

它们不是新增交易逻辑，只是原文触发要求的结构化表达。

### 10. 内核与实施的映射表

| 原始工作流步骤 | 内核要求 | 实施层表达 |
|---|---|---|
| 第1步：3D/1D/4H 地图 | 先看背景，再看交易结构 | `代码层位置层 + Stage1.map_summary` |
| 第2步：选当前剧本 | 只选一个当前主剧本 | `Stage1.current_script` |
| 第3步：驱动归因 | `4H / 1D` 驱动归因 | `Stage1.driver_attribution` |
| 第3.5步：战略 path object | 七类核心字段，含 `risk_grade` | `Stage1.current_path` |
| 第4步：15m 触发确认 | 不改剧本，只确认 path | `candidate engine + Stage2` |
| 等待与监测 | 从 LLM 中剥离 | `path watcher / candidate engine` |
| 第5步：1m / 100ms 执行 | 只负责怎么进 | `Stage2.tactical_entry_plan -> watcher选出具体entry_plan -> 执行引擎` |
| 第6步：管理与出场 | 结构位目标 + 驱动变化管理 | `management_plan + 仓位管理层` |

---

## 结论

本版 `v2.0.0` 的定义是：
- `Stage1` 是战略层，负责 `3D / 1D / 4H` 地图、主剧本、战略 path、驱动归因
- `代码层` 是监测层，负责持续盯盘、候选事件生成、状态推进与二次入场次数控制
- `Stage2` 是战术层，但其第一身份是 `path auditor`，第二身份才是 tactical entry 设计者，不再负责“等待”
- `执行与仓位管理层` 是执行层，负责成交、移损、止盈与离场

因此，后续如果要基于本文件继续写：
- 代码实施方案
- LLM prompt 方案
- schema 设计
- 调度与状态机设计

都必须遵守一个顺序：

先证明“没有改工作流内核”，再谈“怎样工程化落地”。
