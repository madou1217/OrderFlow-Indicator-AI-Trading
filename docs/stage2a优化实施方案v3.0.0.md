# Stage2A优化实施方案 v3.0.0

## 1. 唯一目标

本版实施只服务于这一个目标：

把 `Stage2A` 从“默认基于 `15m` 微结构审核 path 并顺手给出近止损”的模块，改造成“以 `Stage1.current_path` 为主语的审核 + 执行设计器”。

这里的关键不是多加几个字段，而是把 `Stage2A` 的主语改对：

- `Stage1` 负责给 path
- `Stage2A` 负责审核这条 path 现在还活不活
- 如果 path 还活着，`Stage2A` 负责设计这条 path 的执行方式(entry和stop loss)
- `15m` 只负责优化 entry price，不再默认负责 path 的生死和 `stop_loss` 的主锚点


## 2. 第一性原理

这次实施必须同时满足四条原则：

1. 高时间框 thesis，必须由高时间框输入审核。
2. 高时间框执行，必须由高时间框结构决定风险锚点。
3. 低时间框只负责局部定价优化，不负责默认宣判 path 死亡。
4. 输出 schema 必须把“path 审核”和“执行设计”分开表达，不能再用一个混合字段把两件事揉在一起。

如果不按这四条原则改，那么即使 prompt 写得更漂亮，模型也还是会被输入结构和 schema 结构重新拉回旧逻辑。

## 3. 本版实施范围

本版只改四个位置：

- `systems/llm/src/workflow/stage2_input.rs`
- `systems/llm/src/workflow/schema.rs`
- `systems/llm/src/llm/prompt/workflow_stage2a/base.txt`
- `systems/llm/src/workflow/parser.rs`

本版不改：

- `Stage1`
- `Stage2B`
- `Stage2C`
- watcher 生命周期
- execution 下单流程


## 4. 输入源实施方案

### 4.1 实施目标

`Stage2A` 的输入必须从“高时间框背景 + 低时间框触发 + 5m连续性确认”的旧结构，改成“单一真源的 path 主语层 + 强化后的高时间框背景层 + 15m定价层 + 守护约束层”的新结构。

### 4.2 目标输入结构

实施后的 `Stage2APromptInput` 建议收敛为：

```json
{
  "task": "...",
  "stage1_output": {},
  "strategic_context_frozen": {},
  "entry_location_context_15m": {},
  "state_guardrail_snapshot": {},
  "driver_guardrail_snapshot": {},
  "options_guardrail_snapshot": {}
}
```

### 4.3 保留的输入

- `stage1_output`
- `strategic_context_frozen`
- `entry_location_context_15m`
- `state_guardrail_snapshot`
- `driver_guardrail_snapshot`
- `options_guardrail_snapshot`

这些字段保留的原因不是“它们现在就完美”，而是它们已经覆盖了大部分所需数据源，本版重点是重组，不是推倒重来。

### 4.4 高时间框数据源增强

这里必须明确一件事：

本版需要增强 `4h / 1d` 数据源在现有输入里的占比和密度。

也就是说，`strategic_context_frozen` 不能只是“保留原样”，而必须强化成真正服务 `Stage2A` 做 path 审核和执行设计的高时间框输入层。

当前代码里，`build_strategic_context_frozen(...)` 已经提供了一部分高时间框信息，主要包括：

- `price_volume_structure_4h`
- `liquidation_density_4h`
- `selected_avwap_anchors`
- `tpo_4h_1d`
- `rvwap_sigma_bands_4h`
- `ema_trend_regime_4h_1d`
- `open_interest_4h`
- `long_short_ratios_4h`
- `options_regime_1d`
- `avwap_reference_7d`

这批数据的问题不是“完全没用”，而是它们对 `Stage2A` 来说还不够厚，尤其是：

- 有些字段只有 `4h`，没有把 `1d` 一起给全
- 有些字段是高时间框背景，但没有足够支撑 `entry / stop_loss` 的执行设计
- 高时间框 state / driver / options 目前更多像 guardrail，而不是 `Stage2A` 的主输入

本版在 `strategic_context_frozen` 里至少要把下面几类数据增强到位：

1. 高时间框位置信息

- `price_volume_structure` 不再只给 `4h`，应同时给 `4h` 和 `1d`
- `rvwap_sigma_bands` 不再只给 `4h`，应同时给 `4h` 和 `1d`
- `tpo_market_profile` 继续保留 `4h / 1d`
- `selected_avwap_anchors` 和 `avwap_reference_7d` 继续保留

这类数据解决的是：

- 当前价格相对高时间框 value 在哪里
- 当前价格相对高时间框锚点在什么位置
- 现在的入场到底是在高时间框承接位附近，还是已经偏离过远

2. 高时间框结构信息

- `liquidation_density` 不再只给 `4h`，应同时给 `4h` 和 `1d`
- `ema_trend_regime` 继续保留 `4h / 1d`
- 与 `Stage1.current_path` 直接相关的 `FVG / value edge / single print / acceptance` 结构结论，要优先体现在 `strategic_context_frozen`

这类数据解决的是：

- `entry_invalidation_level` 应该参考哪类高时间框结构
- `stop_loss` 到底该放在什么结构之外，才算真正破坏这次执行

3. 高时间框状态信息

- `open_interest` 不再只给 `4h`，应同时给 `4h` 和 `1d`
- `long_short_ratios` 不再只给 `4h`，应同时给 `4h` 和 `1d`
- `funding` 和 `vpin` 虽然现在更多出现在 guardrail，但本版要让 `Stage2A` 在高时间框输入里直接看到它们的 `4h / 1d` 状态结论

这类数据解决的是：

- 当前这条 path 是顺着健康状态在运行，还是在高 crowding / squeeze risk / unwind risk 下硬做
- `stop_loss` 需要留多大正常波动容忍，才能不被 crowding 噪声轻易打掉

4. 高时间框驱动信息

- `cvd_pack` 的 `4h / 1d` 结论要从单纯 guardrail 升级成 `Stage2A` 的显式主输入
- `divergence` 在高时间框上的摘要要直接服务 path 审核
- `whale_trades` 的 `4h / 1d` 摘要要保留

这类数据解决的是：

- 这条 path 现在还是由支持它的 driver 在推动，还是 driver 已经反了
- 当前价格回撤是健康回踩，还是主导 driver 已经切换

5. 高时间框约束信息

- `options_regime_1d` 继续保留
- 如果 `options_guardrail_snapshot` 已经给出了与当前 path 直接相关的约束，也要在 prompt 中明确它属于高时间框执行约束，而不是旁支噪声

这类数据解决的是：

- 高时间框执行能不能承受更深一点的正常波动
- 某些结构上看似合理的 entry / stop，是否会被波动偏斜环境直接破坏

具体实施到 builder，建议按下面的口径改：

- 把 `price_volume_structure_4h / liquidation_density_4h / rvwap_sigma_bands_4h / open_interest_4h / long_short_ratios_4h` 这类单窗口字段，尽可能升级成同时提供 `4h` 与 `1d` 的切片
- 不新增新的 model-facing 字段名，但在现有 `strategic_context_frozen` 里提高 `4h / 1d` 切片密度
- 明确把与 `Stage1.current_path` 执行直接相关的高时间框 state / driver / options 信息，从“辅助 guardrail”升级为“显式主输入”
- 仍然不重复表达 `current_path` 本体语义；`path` 本身仍由 `stage1_output` 提供

这样做的目的只有两个：

- 让 `Stage2A` 有足够的高时间框依据去审核 path 是否仍然成立
- 让 `Stage2A` 有足够的高时间框依据去设计服务于该 path 的 `entry / stop_loss`

换句话说，本版的输入改造是：

- 删掉 `5m`
- 瘦身 `15m`
- 明确增强 `4h / 1d` 数据源

### 4.5 删除的输入

从 `Stage2A` 中删除：

- `continuity_confirmation_context_5m`

删除原因：

- `5m` 连续性确认，本质上是在问“现在这几分钟是不是马上就要延续”
- 这会把 `Stage2A` 拉回短周期自证逻辑
- 这和“基于 `Stage1.current_path` 做高时间框执行设计”的目标相冲突

### 4.6 15m输入的瘦身

`entry_location_context_15m` 保留，但只保留“局部定价优化”所需的数据。

当前代码里，`build_entry_location_context_15m(...)` 主要给了这些内容：

- `price_volume_structure_15m`
- `liquidation_density_15m`
- `avwap_anchor_distances`
- `rvwap_sigma_bands_15m`
- `cvd_pack_15m`
- `divergence_15m_summary`
- `vpin_15m`
- `footprint_15m_summary`

这批数据的问题不是“都是错的”，而是粒度和用途不干净。

对于 `Stage2A` 来说，`15m` 只能解决下面四个问题：

- 现在进是不是太追
- 现在离更合理的局部承接位还有多远
- 当前 `15m` 节奏是适合等回踩，还是可以直接执行
- 如果必须在这条高时间框 path 上执行，entry price 应该放在哪个局部位置更优

所以 `15m` 输入要从“局部结构全家桶”改成“局部定价摘要层”。

这里必须写清楚一件事：

这部分不是让 execution 层去“理解一句自然语言结论”，而是让 `stage2_input.rs` 在进入模型之前，先把原始 `15m` 数据做成确定性的规则摘要。

也就是说，实现位置不是 execution，而是：

- `build_entry_location_context_15m(...)`
- 以及它调用的若干本地 summary helper

建议保留的内容，不是原样保留，而是改造成下面这种可计算结构：

```json
{
  "recent_15m_bars_summary": {},
  "avwap_anchor_distances": {},
  "local_price_location_summary": {},
  "local_flow_summary": {},
  "chasing_risk_flags": {}
}
```

每一项都必须来自确定性规则，不允许由 builder 生成模糊话术。

1. 最近几根 `15m` K 线节奏

- 直接使用现有 `summary.auction_context.recent_15m_bars`
- 只取最后 `5` 根已闭合 `15m` K 线
- builder 负责计算：
  - `bar_count`
  - `net_move_pct`
  - `max_pullback_pct`
  - `max_rebound_pct`
  - `last_close_vs_last_5_mid`
  - `last_close_vs_last_bar_range`
- 目标不是让模型自己读 5 根 K，而是直接给它局部节奏事实

2. `avwap_anchor_distances`

- 这项建议保留
- 因为它直接回答“当前价格离 `Stage1` 选出的关键锚点还有多远”
- 这是典型的局部定价信息，不是 path 生死信息

3. 压缩后的 `15m` 位置结论

- 可以继续基于 `price_volume_structure_15m` 和 `rvwap_sigma_bands_15m`
- 但不要把完整 payload 原样喂给模型
- builder 应改成输出下面这些有明确算法的字段：
  - `current_price`
  - `distance_to_15m_poc = current_price - poc_price`
  - `distance_to_15m_vah = current_price - vah`
  - `distance_to_15m_val = current_price - val`
  - `inside_15m_value_area = (val <= current_price && current_price <= vah)`
  - `z_price_minus_rvwap_15m = rvwap_sigma_bands_15m.z_price_minus_rvwap`
  - `is_rvwap_stretched_15m = abs(z_price_minus_rvwap_15m) >= 1.5`
  - `nearest_selected_anchor_distance = min(abs(distance_to_zone_midpoint))`
  - `nearest_selected_anchor_role = anchor_role of nearest_selected_anchor_distance`

上面这批字段都能直接由现有 payload 计算出来：

- `poc_price / vah / val` 来自 `price_volume_structure_15m`
- `z_price_minus_rvwap` 来自 `rvwap_sigma_bands_15m`
- `distance_to_zone_midpoint` 来自 `avwap_anchor_distances.selected_anchor_distances`

如果某项原始字段缺失，就直接输出 `null`，不做猜测性回填。

4. 压缩后的 `15m` timing 结论

- 可以继续参考 `cvd_pack_15m`、`divergence_15m_summary`、`footprint_15m_summary`
- 但输出给模型的必须是规则推导后的数值/布尔字段，而不是自由文本判断
- builder 应改成输出下面这些有明确算法的字段：
  - `last_5_bars_net_move_pct = (last_close - first_open) / first_open`
  - `last_5_bars_range_pct = (max(high) - min(low)) / first_open`
  - `up_close_count = count(close > open)`
  - `down_close_count = count(close < open)`
  - `delta_fut_15m = last(cvd_pack_15m.by_window.15m.series[].delta_fut)`, 若无 series 则回退到 `cvd_pack_15m.delta_fut`
  - `delta_spot_15m = last(cvd_pack_15m.by_window.15m.series[].delta_spot)`, 若无 series 则回退到 `cvd_pack_15m.delta_spot`
  - `divergence_type_15m = divergence_15m_summary.divergence_type`
  - `spot_lead_score_15m = divergence_15m_summary.spot_lead_score`
  - `stacked_buy_15m = footprint_15m_summary.stacked_buy`
  - `stacked_sell_15m = footprint_15m_summary.stacked_sell`
  - `unfinished_auction_15m = footprint_15m_summary.unfinished_auction`
  - `window_delta_15m = footprint_15m_summary.window_delta`

在这批字段之上，builder 只允许再生成少量明确规则布尔值：

- `one_sided_impulse_with_path`
  - LONG: `up_close_count >= 4 && last_5_bars_net_move_pct > 0`
  - SHORT: `down_close_count >= 4 && last_5_bars_net_move_pct < 0`
- `flow_supports_path`
  - LONG: `delta_fut_15m > 0 || delta_spot_15m > 0 || stacked_buy_15m == true || divergence_type_15m contains "bullish"`
  - SHORT: `delta_fut_15m < 0 || delta_spot_15m < 0 || stacked_sell_15m == true || divergence_type_15m contains "bearish"`
- `local_flow_conflicted`
  - LONG: `stacked_sell_15m == true || divergence_type_15m contains "bearish"`
  - SHORT: `stacked_buy_15m == true || divergence_type_15m contains "bullish"`
- `chasing_risk_with_path`
  - LONG: `is_rvwap_stretched_15m == true && current_price > vah && one_sided_impulse_with_path == true`
  - SHORT: `is_rvwap_stretched_15m == true && current_price < val && one_sided_impulse_with_path == true`

到这里为止就够了。

也就是说，builder 不再生成 `wait_pullback / acceptable_now / do_not_chase` 这种半自然语言枚举，而是只输出上述确定性字段；由 `Stage2A` 基于这些字段判断 entry price 该怎么放。

建议删除或降级的内容，也要写清楚：

1. `price_volume_structure_15m` 原始完整切片

- 不适合原样给模型
- 因为它最容易把模型重新拉回“用 15m 重新定义 path”的旧逻辑

2. `liquidation_density_15m` 原始完整切片

- 这类数据可以用于局部避开追价，但不该让模型直接拿它定义执行失效位
- 更合适的做法是压缩成“局部是否存在明显挤压/扫流动性风险”的一句结论

3. `rvwap_sigma_bands_15m` 原始完整切片

- 保留其结论价值
- 删除其原始细节
- 避免模型把 `15m` 的局部偏离直接当成高时间框 stop 依据

4. `cvd_pack_15m`、`vpin_15m` 的原始细节

- 这类 driver / state 数据对局部 timing 有帮助
- 但如果直接给细节，模型容易把它们升级成 path 审核依据
- 更合适的是只保留上面定义过的：
  - `delta_fut_15m`
  - `delta_spot_15m`
  - `flow_supports_path`
  - `local_flow_conflicted`

5. 细粒度 footprint 明细和原始长事件流

- 这部分最容易制造噪声
- 也是最容易把 `stop_loss` 锚在最近微结构边上的来源
- 本版应明确只保留 `footprint_15m_summary` 这种已经压缩过的结论层

具体实施到 builder，建议按下面的口径改：

- `avwap_anchor_distances` 直接保留
- 基于 `summary.auction_context.recent_15m_bars` 新增 `recent_15m_bars_summary`
- 基于 `price_volume_structure_15m / rvwap_sigma_bands_15m / liquidation_density_15m` 生成 `local_price_location_summary`
- 基于 `cvd_pack_15m / divergence_15m_summary / footprint_15m_summary / vpin_15m` 生成 `local_flow_summary`
- 基于 `recent_15m_bars_summary + local_price_location_summary + local_flow_summary + current_path.side` 生成 `chasing_risk_flags`
- `entry_location_context_15m` 的目标从“描述 15m 发生了什么”改成“告诉模型 entry price 该怎么放更合理”

这里的核心不是“`15m` 没价值”，而是：

`15m` 只能帮助模型把 entry 放得更好，不能帮助模型重新定义这条 path 的生死。


## 6. 提示词实施方案

### 6.1 角色定义重写

`workflow_stage2a/base.txt` 的角色定义，要从：

- tactical trade planner

改成：

- current path auditor
- high-timeframe execution designer

这样做的目的，是让模型先完成 path 审核，再进入执行设计，而不是一上来就进入 tactical planning 心态。

### 6.2 输入说明重写

当前 prompt 里有一条关键输入声明：

- Read `stage1_output`, `strategic_context_frozen`, `entry_location_context_15m`, `continuity_confirmation_context_5m`, and guardrails.

应改成：

- Read `stage1_output`, `strategic_context_frozen`, `entry_location_context_15m`, and guardrails.

### 6.3 任务顺序重写

prompt 的任务顺序必须改成：

1. 先审核 `Stage1.current_path` 是否仍然成立
2. 只有在 path 仍然成立时，才设计执行
3. 执行设计时，先确定高时间框结构失效位和可容忍回撤，再决定 entry
4. `15m` 只用于优化 entry price 的局部落点

这一步非常关键。

因为“先看 `15m` 再看 path”与“先看 path 再用 `15m` 微调价格”，会得到完全不同的止损结果。

### 6.4 核心硬规则重写

新 prompt 至少要明确写出以下规则：

1. 不得默认用 `15m` 微结构判定 `Stage1.current_path` 失效。
2. `entry_invalidation_level` 是服务于当前 path 的结构失效位。
3. `stop_loss` 是 execution 的真实风险终止位，必须由高时间框执行结构决定。

### 6.5 输出要求重写

prompt 的输出要求也要同步改变：

- `PATH_CONFIRMED` 不只是“path 还活着”
- 它还必须包含一份服务于该 path 的执行设计
- 这份执行设计的主逻辑必须来自 `stage1_output` 与强化后的 `strategic_context_frozen`
- `entry_location_context_15m` 只能体现为局部价格优化，而不是主风险锚点


## 7. 输出 JSON Schema 实施方案

### 7.1 调整目标

输出 schema 的目标不是“多加几个解释字段”，而是把三件事明确拆开：

- path 审核结果
- 执行设计结果
- 风险设计理由

当前 schema 的问题是：虽然字段名分开了，但表达层仍然太混合，尤其是 `entry_note` 容易把 path 审核、结构失效、真实止损揉在一个字符串里。

### 7.2 顶层输出结构

建议把 `Stage2AOutput` 调整为：

```json
{
  "stage2_decision": "PATH_CONFIRMED | REQUEST_STAGE1_REEVALUATION",
  "path_audit_note": "string",
  "tactical_entry_plan": {},
  "reevaluation_reason": "string | null"
}
```

调整说明：

- 新增 `path_audit_note`
- `path_audit_note` 用来解释为什么当前 path 仍成立，或者为什么必须重评
- `reevaluation_reason` 继续只在 `REQUEST_STAGE1_REEVALUATION` 时使用

第一性原理上的意义是：

`Stage2A` 的第一职责是审核 path，所以 schema 必须给这个职责一个单独出口。

### 7.3 EntryPlan 结构调整

建议把 `EntryPlan` 从当前的：

- `entry_activation_level`
- `entry_zone`
- `entry_invalidation_level`
- `stop_loss`
- `entry_note`

调整为：

```json
{
  "entry_profile": "string",
  "intent_mode": "immediate | pullback | breakout",
  "entry_activation_level": {},
  "entry_zone": {},
  "entry_invalidation_level": {},
  "stop_loss": 0.0,
  "max_drift_pct": 0.0,
  "entry_reason": "string",
  "invalidation_reason": "string",
  "stop_loss_reason": "string"
}
```

调整说明：

- 不再输出 `side`
- 删除单一的 `entry_note`
- 拆成 `entry_reason`
- 拆成 `invalidation_reason`
- 拆成 `stop_loss_reason`

第一性原理上的原因是：

- 方向已经由 `Stage1.current_path.side` 定义，`Stage2A` 不负责改方向
- `entry_reason` 回答为什么在这里进
- `invalidation_reason` 回答什么结构被破坏了才算这次执行失效
- `stop_loss_reason` 回答为什么 live stop 要放在这里

只有把这三件事分开，才不会再把“局部回踩失败”和“高时间框执行失败”混成同一件事。

### 7.4 entry_activation_level 的可选化

建议把 `entry_activation_level` 从必填改为可选。

原因：

- 不是每一笔高时间框执行，都需要一个单独的激活区
- 有些 plan 的核心只是“在高时间框承接区挂入场”
- 当前 schema 强制模型始终输出 `entry_activation_level`，很容易逼它额外编造一个微结构触发层

所以更合理的做法是：

- `entry_zone` 必填
- `entry_invalidation_level` 必填
- `stop_loss` 必填
- `entry_activation_level` 选填




## 8. parser 侧的配套调整

本版不是要让 parser 继续扮演“战术时间框裁判”，而是要把 parser 收缩成“结构解析 + 机械安全检查”。

因此 parser 侧只需要保留这些检查：

- `stage2_decision` 是否合法
- `stop_loss` 是否仍在风险侧
- 必填字段是否存在

需要移除的检查：

- `ALLOWED_TACTICAL_TIMEFRAMES`
- 对 `entry_activation_level / entry_zone / entry_invalidation_level` 的 tactical timeframe 白名单校验

第一性原理上，parser 不该替 `Stage2A` 判断 path 属于什么时间框；那是 `Stage1.current_path` 已经定义好的事情。

