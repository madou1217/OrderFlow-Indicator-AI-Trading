# LLM层工作流修改方案 v2.1.0

基于 [llm层工作流的修改方案v2.0.0.md](/data/docs/llm层工作流的修改方案v2.0.0.md) 的定向修订版。


## 1. 核心目的

本轮修改只围绕这条核心目的展开：

`Stage1` 只基于 `3D / 4H / 1D` 战略输入，负责给出本次 path 的战略 `failure_level`；`Stage2A` 基于 `15m` 战术输入审核 path 并给出 `entry_invalidation_level`；`Stage2B` 基于 `15m` 与当前持仓管理已有仓位；`Stage2C` 基于 `15m` 与当前挂单管理未成交订单；`reevaluation_trigger` 继续由 `Stage1` 输出，并只负责表达“需要重评”的近端条件。`

## 2. 本轮修改清单

1. 重写 `Stage1` 输入合同：基于 `3D / 4H / 1D` 战略输入。
2. 重写 `Stage1` 输出合同：保留 strategic path，收回战略 `failure_level`，移出 `script_rejections` 与 `management_plan`。
3. 保持 `reevaluation_trigger` 的归属不变，继续放在 `Stage1.current_path` 下；但其 zone-trigger 时间框收紧为战略时间框。
4. 将原本混在一起的 `Stage2` 拆成 `Stage2A / Stage2B / Stage2C` 三个独立分支。
5. 补齐 `Stage2A` 合同：只审核 path、只选入场点。
6. 补齐 `Stage2B` 合同：只做持仓管理，不再混入 `Stage2A`。
7. 同步修改 `Stage1` 提示词，使其和修改后的输入输出合同一致。
8. 补齐 `Stage2C` 合同：只做挂单管理，不再混入 `Stage2A / Stage2B`。
9. 明确实施验收标准，便于对照修改前 / 修改后服务。

## 3. 修改前：当前代码里的 Stage1 输出合同

当前代码中的 `Stage1` 输出合同以以下三处为准：

- [schema.rs](/data/systems/llm/src/workflow/schema.rs)
- [workflow_provider.rs](/data/systems/llm/src/llm/workflow_provider.rs)
- [parser.rs](/data/systems/llm/src/workflow/parser.rs)

当前服务实际使用的 `Stage1Output` 可概括为：


## 4. 修改后：Stage1 输入合同

修改后的 `Stage1` 输入只保留战略层输入：

```text
{
  "task": "执行3D/4H/1D地图、主剧本选择、战略path构建", # Stage1 任务说明
  "strategic_indicator_summary": {
    "position_layer": {
      "3d": {}, # 3D 聚合位置摘要
      "4h": {}, # 4H 聚合位置摘要
      "1d": {}, # 1D 聚合位置摘要
      "price_volume_structure": {
        "3d": {}, # i1 price_volume_structure；3D 位置摘要
        "4h": {}, # i1 price_volume_structure；4H 位置摘要
        "1d": {} # i1 price_volume_structure；1D 位置摘要
      },
      "liquidation_density": {
        "3d": {}, # i4 liquidation_density；3D 位置摘要
        "4h": {}, # i4 liquidation_density；4H 位置摘要
        "1d": {} # i4 liquidation_density；1D 位置摘要
      },
      "avwap": {
        "7d": {}, # 7D AVWAP；保留为战略参考锚点，不是战术时间框
        "3d": {}, # 3D AVWAP
        "4h": {}, # 4H AVWAP
        "1d": {} # 1D AVWAP
      },
      "tpo_market_profile": {
        "3d": {}, # i20 TPO market profile；3D 位置摘要
        "4h": {}, # i20 TPO market profile；4H 位置摘要
        "1d": {} # i20 TPO market profile；1D 位置摘要
      },
      "rvwap_sigma_bands": {
        "4h": {}, # i21 RVWAP sigma bands；4H 战略偏离带
        "1d": {} # i21 RVWAP sigma bands；1D 战略偏离带
      },
      "ema_trend_regime": {
        "4h": {}, # i23 EMA trend regime；4H EMA100/200 regime
        "1d": {} # i23 EMA trend regime；1D EMA100/200 regime
      },
      "fvg": {
        "3d": {}, # i24 FVG/缺口；3D 位置摘要
        "4h": {}, # i24 FVG/缺口；4H 位置摘要
        "1d": {} # i24 FVG/缺口；1D 位置摘要
      }
    },
    "state_layer": {
      "3d": {}, # 3D 聚合状态摘要
      "4h": {}, # 4H 聚合状态摘要
      "1d": {}, # 1D 聚合状态摘要
      "funding": {
        "4h": {}, # i16 funding；4H 状态摘要
        "1d": {} # i16 funding；1D 状态摘要
      },
      "vpin": {
        "4h": {}, # i17 VPIN；4H 状态摘要
        "1d": {} # i17 VPIN；1D 状态摘要
      },
      "open_interest": {
        "3d": {}, # i25 open_interest；3D 状态摘要
        "4h": {}, # i25 open_interest；4H 状态摘要
        "1d": {} # i25 open_interest；1D 状态摘要
      },
      "long_short_ratios": {
        "3d": {}, # i26 long_short_ratios；3D 状态摘要
        "4h": {}, # i26 long_short_ratios；4H 状态摘要
        "1d": {} # i26 long_short_ratios；1D 状态摘要
      }
    },
    "driver_layer": {
      "3d": {}, # 3D 聚合驱动摘要
      "4h": {}, # 4H 聚合驱动摘要
      "1d": {}, # 1D 聚合驱动摘要
      "cvd_pack": {
        "4h": {}, # i14 CVD pack；4H 驱动摘要
        "1d": {} # i14 CVD pack；1D 驱动摘要
      },
      "divergence": {
        "4h": {}, # i3 divergence；4H 去趋势后的有效背离摘要
        "1d": {} # i3 divergence；1D 去趋势后的有效背离摘要
      },
      "whale_trades": {
        "3d": {}, # i15 whale trades；3D 驱动摘要
        "4h": {}, # i15 whale trades；4H 驱动摘要
        "1d": {} # i15 whale trades；1D 驱动摘要
      }
    },
    "trigger_layer": {
      "footprint": {}, # i2 footprint；只保留绑定 3D / 4H / 1D 关键位的确认摘要，不放原始 15m 明细
      "orderbook_depth": {}, # i5 orderbook_depth；只保留绑定关键位的确认摘要
      "absorption": {}, # i6 absorption；只保留确认后的结构摘要
      "initiation": {}, # i7 initiation；只保留确认后的结构摘要
      "buying_exhaustion": {}, # i12 buying exhaustion；只保留确认后的结构摘要
      "selling_exhaustion": {}, # i13 selling exhaustion；只保留确认后的结构摘要
      "high_volume_pulse": {} # i22 high_volume_pulse；只保留确认后的结构摘要
    },
    "aux_context": {
      "3d": {}, # 3D 辅助上下文
      "4h": {}, # 4H 辅助上下文
      "1d": {}, # 1D 辅助上下文
      "options_surface": {
        "4h": {}, # i27 options_surface；4H 战略辅助摘要
        "1d": {} # i27 options_surface；1D 战略辅助摘要
      }
    },
    "structural_refresh_context": {
      "refresh_cause": "scheduled_2h | hard_invalidation | reverse_confirmation | extreme_location | driver_regime_conflict | no_edge_reentered | regime_shift", # 本次重画原因
      "affected_zone_id": "zone_x | null", # 本次重画涉及的结构区 ID
      "affected_timeframe": "3d | 4h | 1d | 4h-1d | 1d-3d | null", # 受影响时间框
      "structural_summary": "" # 结构变化摘要
    }
  },
  "previous_stage1_output": {}, # 上一条 Stage1 输出，仅用于 continuity / comparison
  "refresh_reason": "scheduled_2h | path_invalidated | no_edge_reentered | regime_shift" # 本次刷新原因
}
```

这份合同的实施含义是：生成给 `Stage1` 的 prompt input 时，只组装上述战略字段。
其中 `7D AVWAP` 必须显式保留；它不是把 `Stage1` 变成 `7D` 交易层，而是作为战略背景锚点参与 `3D / 4H / 1D` 路径判断。
其中 `i25 open_interest`、`i26 long_short_ratios`、`i27 options_surface` 也必须显式保留；它们分别属于状态层和辅助层，不应在本版输入合同里消失。
其中 `price_volume_structure / liquidation_density / TPO / RVWAP / EMA / FVG`、`CVD pack / divergence / whale_trades`、以及 `footprint / orderbook_depth / absorption / initiation / exhaustion / high_volume_pulse` 也必须显式保留；对 `Stage1` 而言，它们只能以战略可消费的确认摘要进入，不得退化成原始 15m 事件流。

## 5. 修改后：Stage1 输出合同

修改后的 `Stage1` 输出收敛为“只表达战略 path”：

```text
{
  "meta": {
    "stage1_ts": "2026-03-26T16:00:00Z" # Stage1 本次输出时间
  },
  "monitoring_status": "active | no_edge", # active=存在战略 path; no_edge=当前无战略机会
  "no_trade_reason": "conflict_no_edge | script_not_unique | path_not_actionable | null", # no_edge 原因
  "refresh_hints": ["..."], # 补充提示
  "map_summary": {
    "location_3d": {}, # 3D 位置摘要；用于保留 3D 背景的可观测输出
    "location_1d": {}, # 1D 位置摘要
    "location_4h": {}, # 4H 位置摘要
    "price_location_class": "inside_value_middle | value_edge | outside_value_extended", # 位置粗分类
    "key_levels": {} # 高时框关键位
  },
  "opportunity_assessment": {
    "location_quality": "high | medium | low | null",
    "state_quality": "high | medium | low | null",
    "driver_quality": "high | medium | low | null",
    "geometry_quality": "high | medium | low | null",
    "uniqueness_quality": "high | medium | low | null",
    "overall_quality": "high | medium | low | null", 
    "disqualifiers": []
  },
  "current_script": "continuation | crowded_reversal | value_return | null", # 当前唯一主剧本
  "driver_attribution": {
    "flow_driver": "spot_led | futures_led | mixed", # 当前主导 driver
    "spot_confirming": true, # 现货是否确认
    "driver_note": "" # 驱动说明
  },
  "current_path": {
    "id": "path_current", # path 唯一 ID
    "side": "LONG | SHORT", # 方向
    "thesis": "", # path 主论点
    "risk_grade": "aligned_trend | countertrend_repair | high_conflict_repair", # 风险等级
    "activation_anchor_id": "zone_activation", # 激活锚点 ID
    "activation_level": {
      "low": 0.0,
      "high": 0.0,
      "timeframe": "4h | 1d | 4h-1d", # 战略激活区时间框
      "label": "",
      "reason": ""
    },
    "first_path_target_anchor_id": "zone_target_1", # 第一目标锚点 ID
    "first_path_target": {
      "low": 0.0,
      "high": 0.0,
      "timeframe": "4h | 1d | 4h-1d", # 第一战略目标区时间框
      "label": "",
      "reason": ""
    },
    "next_path_target_anchor_id": "zone_target_2", # 第二目标锚点 ID
    "next_path_target": {
      "low": 0.0,
      "high": 0.0,
      "timeframe": "4h | 1d | 4h-1d", # 第二战略目标区时间框
      "label": "",
      "reason": ""
    },
    "failure_anchor_id": "zone_failure", # 失效锚点 ID
    "failure_level": {
      "low": 0.0,
      "high": 0.0,
      "timeframe": "4h | 1d | 4h-1d", # 本次 path 的战略 hard invalidation
      "label": "",
      "reason": ""
    },
    "failure_switch": "continuation | crowded_reversal | value_return | machine_identifier | null", # 战略失效后的理论切换
    "setup_type": "A_continuation | B_reversal | C_value_return", # setup 机器枚举
    "reevaluation_trigger": { # 保持 Stage1 归属，但 zone-trigger 时间框收紧到战略层
      "extreme_location": {
        "kind": "accepted_into_zone | accepted_beyond_zone | rejected_from_zone | reaccepted_through_zone",
        "zone_id": "",
        "timeframe": "4h | 1d | 4h-1d",
        "min_confirmed_bars": 1,
        "summary": "",
        "evidence": []
      },
      "reverse_confirmation": {
        "kind": "accepted_into_zone | accepted_beyond_zone | rejected_from_zone | reaccepted_through_zone",
        "zone_id": "",
        "timeframe": "4h | 1d | 4h-1d",
        "min_confirmed_bars": 1,
        "summary": "",
        "evidence": []
      },
      "driver_change": {
        "kind": "driver_flip | spot_confirmation_lost | oi_support_lost | state_regime_conflict",
        "expected_flow_driver": "spot_led | futures_led | mixed",
        "invalidate_when_drivers": [],
        "require_spot_confirmation": true,
        "driver_signal": "spot_confirmation_lost | oi_support_lost | fake_order_risk_rising | driver_flip_confirmed",
        "min_confirmed_windows": 1,
        "summary": "",
        "evidence": []
      }
    },
    "tracked_zones": [] # 战略锚点池；activation / target / failure / reevaluation_trigger 都从这里引用
  }
}
```

这份合同的实施含义是：`Stage1` 直接输出的只有 strategic path 本体，不再承载 post-entry 管理合同。

## 6. 修改后：Stage2A / Stage2B / Stage2C 合同

### 6.1 Stage2A 输入合同

`Stage2A` 只负责审核 path 与给入场点。它只在 `flat_no_orders` 状态下被调起。

```text
{
  "task": "审核当前 strategic path，并基于 15m 战术输入设计 tactical entry",
  "candidate_event": {}, # 当前触发 Stage2A 的候选事件
  "path_runtime_state": {}, # 当前 path 的运行时状态
  "previous_tactical_plan": {}, # 上一轮 tactical plan
  "exposure_state": "flat_no_orders", # Stage2A 只在 flat_no_orders 下运行
  "tactical_position_slice": {}, # 当前战术位置切片
  "latest_15m_trigger_facts": {}, # 15m 战术事实
  "state_guardrail_snapshot": {}, # 状态护栏
  "driver_guardrail_snapshot": {}, # 驱动护栏
  "options_guardrail_snapshot": {}, # 期权护栏
  "stage1_output": {}, # Stage1 strategic path；其中 failure_level 与 reevaluation_trigger 只读
  "account": {} # 账户上下文
}
```

### 6.2 Stage2A 输出合同

`Stage2A` 只输出 path 审核结论与唯一主入场计划，不再输出任何管理合同。

```text
{
  "stage2_decision": "PATH_CONFIRMED | REQUEST_STAGE1_REEVALUATION", # Stage2A 决策
  "tactical_entry_plan": { # PATH_CONFIRMED 时必须为对象；REQUEST_STAGE1_REEVALUATION 时必须为 null
    "path_id": "path_current", # 必须匹配 Stage1 current_path.id
    "entry_plan": {
      "side": "LONG | SHORT", # 方向
      "entry_profile": "reclaim_then_hold | pullback_acceptance | failed_auction_reentry", # 入场画像
      "intent_mode": "immediate | pullback | breakout", # 触发方式
      "entry_activation_level": {
        "low": 0.0,
        "high": 0.0,
        "timeframe": "15m | 15m-4h",
        "label": "",
        "reason": ""
      },
      "entry_zone": {
        "low": 0.0,
        "high": 0.0,
        "timeframe": "15m | 15m-4h",
        "label": "",
        "reason": ""
      },
      "entry_invalidation_level": {
        "low": 0.0,
        "high": 0.0,
        "timeframe": "15m | 15m-4h", # tactical invalidation 归属 Stage2
        "label": "",
        "reason": ""
      },
      "stop_loss": 0.0, # 本次 tactical entry 的止损
      "max_drift_pct": 0.12, # 允许的最大漂移
      "entry_note": "" # 本次入场说明
    }
  } | null,
  "reevaluation_reason": "string | null" # REQUEST_STAGE1_REEVALUATION 时必须为 string；PATH_CONFIRMED 时必须为 null
}
```

这份合同的实施含义是：

- `Stage2A` 只负责给 entry 相关字段。
- `take_profit_1 / take_profit_2` 不再由 `Stage2A` 输出，而是由代码层直接继承 `Stage1.first_path_target / next_path_target` 后拼装给 watcher。
- `Stage2A` 只给出一个主进入点和一个主止损；后续未成交挂单的移动、撤销、改 bracket，统一交给 `Stage2C`。

### 6.3 Stage2B 输入合同

`Stage2B` 是独立请求分支。它只在 `in_position` 状态下被调起。

```text
{
  "task": "基于当前 strategic path 与 15m 战术输入管理持仓，以最大化收益为目标",
  "candidate_event": {}, # 当前触发 Stage2B 的候选事件
  "path_runtime_state": {}, # 当前 path 的运行时状态
  "exposure_state": "in_position", # Stage2B 只在已持仓时运行
  "active_positions": [], # 当前请求只允许携带 1 笔活动仓位；每笔仓位单独请求
  "latest_15m_trigger_facts": {}, # 15m 战术事实
  "state_guardrail_snapshot": {}, # 状态护栏
  "driver_guardrail_snapshot": {}, # 驱动护栏
  "options_guardrail_snapshot": {}, # 期权护栏
  "stage1_output": {}, # Stage1 strategic path；failure_level 与 reevaluation_trigger 只读
  "previous_management_plan": {}, # 与这笔持仓 context_key 对应的上一轮管理计划
  "account": {} # 账户上下文
}
```

### 6.4 Stage2B 输出合同

`Stage2B` 只负责持仓管理。它的目标函数不是找新的 entry，而是在已有持仓前提下最大化收益。
本版进一步明确：`Stage2B` 输出的是“条件化管理计划”，不是“立即执行动作”。
也就是说，`Stage2B` 返回的是 watcher 未来要监听的管理条件；真正的加仓、减仓、平仓、改 TP/SL，统一由 watcher 在条件满足时触发执行。

```text
{
  "stage2b_decision": "MANAGE_POSITION", # Stage2B 决策；不承担 Stage1 重评职责
  "position_management_plan": {
    "path_id": "path_current", # 必须匹配 Stage1 current_path.id
    "exposure_state": "in_position", # 当前风险暴露状态
    "path_live_assessment": "live | degraded | invalidated", # 当前 path 对既有仓位是否仍成立
    "path_assessment_reason": "string | null", # path 评估说明；用于解释为何继续持有/减仓/平仓
    "actions": [
      {
        "action_type": "hold | add | reduce | exit_full | move_stop | update_take_profit", # 持仓管理动作
        "context_key": "ctx_a", # 本次动作所作用的仓位上下文
        "path_id": "path_current", # 必须匹配 path_id
        "watcher_trigger_condition": {
          "trigger_type": "price_above_on_close | price_below_on_close", # watcher 价格触发类型
          "trigger_level": 0.0, # 突破/跌破并站稳的关键价格
          "note": "" # 触发解释；如 reclaim above x / lose below y
        } | null, # hold 可为 null；其余动作默认应为对象
        "add_ratio": 0.25, # add 时使用；否则为 null
        "reuse_current_entry_template": true, # add 时必须为 true；表示沿用当前仓位已有 entry 模板加仓
        "reduce_ratio": 0.25, # reduce 时使用；否则为 null
        "new_stop_loss": 0.0, # move_stop 时使用；否则为 null
        "reuse_current_bracket_template": true, # move_stop / update_take_profit 时必须为 true；否则为 null
        "take_profit_1": 0.0, # update_take_profit 时如需改 TP1 则填写；否则为 null
        "take_profit_2": 0.0, # update_take_profit 时如需改 TP2 则填写；否则为 null
        "reason": "" # 管理原因
      }
    ],
    "management_note": "" # 本轮管理摘要
  }
}
```

这份合同的实施含义是：

- `Stage2B` 不再表示“现在立刻执行 add/reduce/exit/move_stop/update_take_profit”。
- `watcher_trigger_condition` 表示 watcher 要持续监听的价格条件；“站稳”语义由 watcher 的 `confirm_bars / min_close_bps` 配置负责。
- `add` 如果被使用，必须显式给出 `add_ratio`，并且 `reuse_current_entry_template=true`。
- `reuse_current_entry_template=true` 的含义是：加仓不是重新做一轮 `Stage2A` 战术设计，而是沿用当前仓位已经生效的 entry 模板 / bracket 模板来做管理层加仓。
- `move_stop / update_take_profit` 必须是 patch-style bracket 更新：只给本次要修改的字段，并且 `reuse_current_bracket_template=true`。
- `reuse_current_bracket_template=true` 的含义是：未显式提供的 bracket 字段继续沿用当前仓位已有模板，不重新生成一整套新的 bracket 计划。
- `reduce / exit_full / move_stop / update_take_profit` 应默认给出 `watcher_trigger_condition`；只有 `hold` 可以为 `null`。

### 6.5 Stage2C 输入合同

`Stage2C` 是独立请求分支。只要存在活动入场挂单，它就会被调起；如果同方向持仓也同时存在，则它作为次级管理分支与 `Stage2B` 同一轮运行。

```text
{
  "task": "基于当前 strategic path 与 15m 战术输入管理未成交挂单，以最大化收益为目标",
  "candidate_event": {}, # 当前触发 Stage2C 的候选事件
  "path_runtime_state": {}, # 当前 path 的运行时状态
  "exposure_state": "flat_with_live_entry_orders | in_position_with_live_entry_orders", # 只要存在活动入场挂单就运行；若同方向持仓也存在，则取后者
  "active_orders": [], # 当前请求只允许携带 1 笔活动挂单；每笔挂单单独请求
  "latest_15m_trigger_facts": {}, # 15m 战术事实
  "state_guardrail_snapshot": {}, # 状态护栏
  "driver_guardrail_snapshot": {}, # 驱动护栏
  "options_guardrail_snapshot": {}, # 期权护栏
  "stage1_output": {}, # Stage1 strategic path；failure_level 与 reevaluation_trigger 只读
  "previous_pending_order_management_plan": {}, # 与这笔挂单 context_key 对应的上一轮挂单管理计划
  "account": {} # 账户上下文
}
```

### 6.6 Stage2C 输出合同

`Stage2C` 只负责挂单管理。它的目标函数不是找新的 path，而是在已有未成交挂单前提下最大化收益；这里允许“已有同方向持仓 + 活动挂单”共存场景作为新的次级管理分支运行。
这里的 `post_fill_bracket_template` 指“挂单成交后的 bracket 模板”，不是交易所上已存在的真实退出单。
本版进一步明确：`Stage2C` 也不做“瞬发执行”，而是输出 watcher 未来要监听的条件化挂单管理计划。

```text
{
  "stage2c_decision": "MANAGE_PENDING_ORDERS", # Stage2C 决策；不承担 Stage1 重评职责
  "pending_order_management_plan": {
    "path_id": "path_current", # 必须匹配 Stage1 current_path.id
    "exposure_state": "flat_with_live_entry_orders | in_position_with_live_entry_orders", # 当前挂单风险暴露状态
    "path_live_assessment": "live | degraded | invalidated", # 当前 path 对既有挂单是否仍成立
    "path_assessment_reason": "string | null", # path 评估说明；用于解释为何保留/修改/撤销挂单
    "actions": [
      {
        "action_type": "keep_order | cancel_pending_order | replace_entry | update_post_fill_bracket_template", # 挂单管理动作
        "context_key": "ctx_pending", # 本次动作所作用的挂单上下文
        "path_id": "path_current", # 必须匹配 path_id
        "watcher_trigger_condition": {
          "trigger_type": "price_above_on_close | price_below_on_close", # watcher 价格触发类型
          "trigger_level": 0.0, # 突破/跌破并站稳的关键价格
          "note": "" # 触发解释
        } | null, # keep_order 可为 null；其余动作默认应为对象
        "replacement_entry_zone": {
          "low": 0.0,
          "high": 0.0,
          "timeframe": "15m | 15m-4h",
          "label": "",
          "reason": ""
        } | null, # replace_entry 时使用；否则为 null
        "replacement_entry_invalidation_level": {
          "low": 0.0,
          "high": 0.0,
          "timeframe": "15m | 15m-4h",
          "label": "",
          "reason": ""
        } | null, # replace_entry 时使用；否则为 null
        "replacement_stop_loss": 0.0, # replace_entry 时使用；否则为 null
        "reuse_current_entry_template": true, # replace_entry 时必须为 true；表示沿用当前挂单已有 entry 模板
        "post_fill_bracket_template": {
          "take_profit_1": 0.0,
          "take_profit_2": 0.0,
          "stop_loss": 0.0
        }, # update_post_fill_bracket_template 时使用；否则为 null
        "reason": "" # 管理原因
      }
    ],
    "management_note": "" # 本轮挂单管理摘要
  }
}
```

这份合同的实施含义是：

- `Stage2C` 不再表示“收到模型结果后立刻撤单 / 立刻改挂单 / 立刻改 TP/SL”。
- `watcher_trigger_condition` 表示 watcher 要持续监听的价格条件；“站稳”语义同样由 watcher 的 `confirm_bars / min_close_bps` 配置负责。
- `replace_entry` 不再重给一整套新的 tactical entry，而是只调整 `replacement_entry_zone / replacement_entry_invalidation_level / replacement_stop_loss`。
- `reuse_current_entry_template=true` 的含义是：挂单替换不是重新做一轮 `Stage2A` 战术设计，而是沿用当前挂单已经生效的 `entry_profile / intent_mode / activation logic`，只改需要挪动的价格参数。
- `cancel_pending_order / replace_entry / update_post_fill_bracket_template` 应默认给出 `watcher_trigger_condition`；只有 `keep_order` 可以为 `null`。

### 6.7 Stage2A / Stage2B / Stage2C 调度切换

本版将原来的单一 `Stage2` 调度改成按 `exposure_state` 切换：

```text
15m scheduler tick:
flat_no_orders -> Stage2A
flat_with_live_entry_orders -> Stage2C
in_position -> Stage2B
in_position_with_live_entry_orders -> Stage2B + Stage2C
```

对应含义：

- `flat_no_orders`：没有持仓，也没有活动挂单，只需要审核 path 并找 entry。
- `flat_with_live_entry_orders`：已有未成交挂单、但尚未持仓，进入挂单管理态。
- `in_position`：已有持仓，进入持仓管理态。
- `in_position_with_live_entry_orders`：已有持仓且仍有同方向活动入场挂单；进入“Stage2B 主、Stage2C 次”的共存管理态。

优先级约束：

- 持仓第一：`in_position`
- 挂单第二：`flat_with_live_entry_orders`
- 入场第三：`flat_no_orders`
- `15m` 调度可以并发，但同一 symbol 的最终执行合并必须按上述优先级落地。
- 同一时间内，同方向最多只能有 `1` 笔持仓和 `1` 笔活动入场挂单；这两个上限由 `config.yaml` 中的 `llm.workflow.limits.max_live_positions_per_direction` 与 `llm.workflow.limits.max_live_entry_orders_per_direction` 明确配置。目前的stage2b，stage2c不支持挂多单，或多个仓位同时管理，所以每个挂单，每个持仓时单独管理的，单独请求.不要在一个请求里，去把多笔挂单交给模型去管理，每笔挂单一个独立请求！
- 只要同方向已经存在 `1` 笔持仓或 `1` 笔活动入场挂单，就不再请求 `Stage2A` 去开新仓。

- 如果同一 symbol 同时存在持仓和活动挂单，则以 `Stage2B` 为主，`Stage2C` 为次级管理分支，`Stage2A` 不生效。
- 这里的“`Stage2C` 为次级管理分支”是指：在共存场景下，系统仍然会新发起 `Stage2C` LLM 请求来生成新的挂单管理计划，同时 watcher 继续按优先级消费 `Stage2B / Stage2C` 的条件化计划。

本版的实施含义是：

- `Stage1` 继续定义 strategic path。
- `Stage2A` 继续审核 path，并给 tactical entry。
- `Stage2B` 独立请求，专门生成“条件化持仓管理计划”。
- `Stage2C` 独立请求，专门生成“条件化挂单管理计划”；只要存在活动入场挂单，它就会作为新的 LLM 请求分支运行。
- 若 symbol 已进入 `in_position`，但还残留同方向活动挂单，则 `Stage2B` 负责主持仓管理，`Stage2C` 负责次级挂单管理；两者都可以在同一轮 `15m` review 中生成新的 watcher 计划。
- `Stage2A / Stage2B / Stage2C` 都只负责给 watcher 提供计划，不直接触发瞬时执行。
- watcher 根据 `Stage2A / Stage2B / Stage2C` 的条件计划执行，不重新思考 path。

### 6.8 Stage1 质量过滤配置

在 `config.yaml` 中新增质量过滤配置：

```text
workflow:
  stage1:
    min_overall_quality_for_new_entry_dispatch: "high | medium | low"  
```
默认值是medium

实施语义：

- 当 `Stage1.opportunity_assessment.overall_quality` 低于该阈值时，不下发到 `Stage2A`，等价于 `no entry`。
- 该过滤只作用于“新的入场分发”。
- 如果已经存在持仓或活动挂单，`Stage2B / Stage2C` 必须继续运行，不能因为质量过滤而停止对现有风险暴露的管理。

## 7. 修改后：Stage1 / Stage2A / Stage2B / Stage2C 提示词

### 7.1 Stage1 提示词

将 [base.txt](/data/systems/llm/src/llm/prompt/workflow_stage1/base.txt) 改为：

```text
You are a top-tier order flow trading opportunity finder specializing in 4h-1d trading opportunities for __SYMBOL__.

Your job:
- Read strategic_indicator_summary.
- Use 3D / 4H / 1D strategic inputs to build the market map.
- Decide whether a high-quality strategic opportunity exists right now.
- Choose exactly one current script or return no_edge.
- When monitoring_status=active, output exactly one strategic path with:
  - strategic activation
  - strategic targets
  - strategic failure_level
  - reevaluation_trigger
  - tracked_zones

Strategic reasoning rules:
- 3D is the background and regime constraint layer.
- 4H / 1D are the direct path geometry anchor layers.
- 7D AVWAP is the strategic reference anchor.
- Use open_interest and long_short_ratios to judge leverage build/unwind, crowding, and whether the path is structurally supported or overcrowded.
- Use options_surface as strategic volatility/skew context that can strengthen, weaken, or constrain conviction, but do not let it override core path geometry by itself.
- Use price_volume_structure, liquidation_density, TPO market profile, RVWAP sigma bands, EMA trend regime, and FVG to define higher-timeframe location, structure, and path geometry.
- Use funding and VPIN to judge higher-timeframe state quality, crowding stress, and whether the market is vulnerable to squeeze or unwind.
- Use CVD pack, divergence, and whale_trades to explain who is driving the move and whether the move is spot-led, futures-led, or mixed.
- Use footprint, orderbook_depth, absorption, initiation, buying_exhaustion, selling_exhaustion, and high_volume_pulse only as summarized, confirmed strategic evidence tied to important higher-timeframe levels.
- Use previous_stage1_output only for continuity, comparison, and to explain what materially changed versus the prior path.
- Use structural_refresh_context to understand what caused the refresh, which timeframe or zone was affected, and why the structure may need to be redrawn.
- The path must be honest about risk_grade, target distance, and structural quality.
- map_summary must include location_3d, location_1d, and location_4h.

Path rules:
- activation_anchor_id, first_path_target_anchor_id, next_path_target_anchor_id, and failure_anchor_id must each point to tracked_zones[].zone_id.
- activation_level, first_path_target, next_path_target, and failure_level must align with their corresponding anchor zones.
- failure_level means the strategic hard invalidation of the current 4H / 1D path.
- failure_level.timeframe must be one of 4h, 1d, or 4h-1d.
- reevaluation_trigger defines the confirmed higher-timeframe conditions that should send the current path back for strategic review.
- reevaluation_trigger.extreme_location.timeframe and reevaluation_trigger.reverse_confirmation.timeframe must be one of 4h, 1d, or 4h-1d.

Output contract:
- Return exactly one JSON object that matches the Stage1 schema.
- If monitoring_status=active, output exactly one current_script and one current_path.
- If monitoring_status=no_edge, current_script and current_path must be null.
```

建议保留当前简单的 `user prompt prefix`：

```text
You are a top-tier order flow trading opportunity finder specializing in 4h-1d trading opportunities.
```

### 7.2 Stage2A 提示词

`Stage2A` 的提示词应独立维护在 [workflow_stage2a/base.txt](/data/systems/llm/src/llm/prompt/workflow_stage2a/base.txt) 中，并按本版边界收敛为只做 path 审核与 tactical entry。旧的 [workflow_stage2/base.txt](/data/systems/llm/src/llm/prompt/workflow_stage2/base.txt) 只能视为迁移历史参考，不再是 active prompt 资产，也不能再作为实现依据：

```text
You are a top-tier path auditor and tactical trade planner for __SYMBOL__.

- Audit the current strategic path first, then refine it into a tradeable execution plan only if the path is still alive.
- You may only output:
  - PATH_CONFIRMED
  - REQUEST_STAGE1_REEVALUATION
- Hard invalidation or confirmed reevaluation conditions require reevaluation.
- Tactical entry changes may be aggressive, but they must stay inside the current strategic path envelope.
- You may tighten execution risk, but you may not widen or rewrite the strategic failure_level.

Your job:
- Read the candidate event, path runtime state, latest_15m_trigger_facts, guardrails, and stage1_output.
- Audit whether the current strategic path is still alive.
- If the path is invalid, request Stage1 reevaluation.
- If the path is still alive, return PATH_CONFIRMED and build a tactical_entry_plan with:
  - entry_plan

Entry design rules:
- entry_activation_level, entry_zone, entry_invalidation_level, and stop_loss are tactical fields owned by this prompt.
- entry_invalidation_level must remain a 15m or 15m-4h tactical invalidation, not a strategic invalidation.
- Tactical entry must fit inside the current strategic path envelope.
- Output only the single highest-quality entry plan you believe is valid right now.

Output contract:
- PATH_CONFIRMED must include a full tactical_entry_plan.
- REQUEST_STAGE1_REEVALUATION must set tactical_entry_plan to null and provide reevaluation_reason.
```

建议新增简单的 `user prompt prefix`：

```text
You are a path auditor and tactical execution planner. Audit the current strategic path first, then output either PATH_CONFIRMED with tactical_entry_plan or REQUEST_STAGE1_REEVALUATION.
```

### 7.3 Stage2B 提示词

`Stage2B` 是持仓管理分支。它的目标不是重新找 entry，而是在已有持仓前提下以最大化收益为目标做管理。

```text
You are a top-tier order flow trader specializing in 4h-1d trades for __SYMBOL__.

- Your domain here is active position review and management.
- You have one objective: maximize realized and retained expectancy for the active position.
- Timely loss reduction, stop tightening, and full exit are valid parts of maximizing profit.
- You are designing watcher-managed conditional actions.

Your work is always two-step:
- Step 1: audit whether the path supporting the current position is still alive.
- Step 2:
  - If the path is not alive, the first priority is risk exit. Return conditional close-style management that will flatten or materially de-risk the position once the watcher trigger is confirmed.
  - If the path is still alive, return conditional management actions such as add, reduce, move_stop, update_take_profit, or hold.

Your job:
- Read the active positions, path runtime state, latest_15m_trigger_facts, guardrails, previous_management_plan, and stage1_output.
- Decide whether the current position is still supported by the strategic path.
- If the path is invalidated, output management actions that close or strongly de-risk the position.
- If the path is live or degraded-but-valid, output a position_management_plan that maximizes收益 while remaining inside the strategic path envelope.

Management rules:
- Adds are allowed only when they improve expectancy and remain consistent with the live strategic path.
- Adds must remain management-layer adds: reuse the currently active entry template rather than redesigning a fresh tactical entry.
- Reductions, stop moves, and profit-taking updates are valid whenever they improve expectancy.
- move_stop and update_take_profit must remain patch-style bracket updates: reuse the current bracket template and only specify the fields that change.
- Non-hold actions must carry watcher_trigger_condition with a concrete trigger_level.
- watcher_trigger_condition.trigger_type must be price_above_on_close or price_below_on_close.
- If the path is no longer live, do not keep hoping; close or de-risk decisively.

Output contract:
- position_management_plan must target the current path_id and the current position context.
```

建议新增简单的 `user prompt prefix`：

```text
You are a top-tier 4h-1d order flow trader reviewing an active position. First audit whether the supporting path is still alive. If not, output conditional close/de-risk actions. If yes, output a watcher-managed conditional position management plan.
```

### 7.4 Stage2C 提示词

`Stage2C` 是挂单管理分支。它的目标不是重新找 path，而是在已有未成交挂单前提下以最大化收益为目标做管理。

```text
You are a top-tier order flow trader specializing in 4h-1d trades for __SYMBOL__.

- Your domain here is pending-order review and management.
- You have one objective: maximize expected value for active pending orders.
- Canceling stale or invalid pending exposure is a valid part of maximizing profit.
- You are designing watcher-managed conditional actions.

Your work is always two-step:
- Step 1: audit whether the path supporting the current pending orders is still alive.
- Step 2:
  - If the path is not alive, the first priority is risk removal. Return conditional close-style management that will cancel or fully remove stale pending exposure once the watcher trigger is confirmed.
  - If the path is still alive, return conditional pending-order actions such as keep_order, replace_entry, or update_post_fill_bracket_template.

Your job:
- Read the exposure_state, active orders, path runtime state, latest_15m_trigger_facts, guardrails, previous_pending_order_management_plan, and stage1_output.
- If a same-side position is already live, treat this as a pending-order secondary management branch rather than a fresh entry-design task.
- Decide whether the current pending orders are still supported by the strategic path.
- If the path is invalidated, output pending-order management actions that cancel or remove stale exposure.
- If the path is live or degraded-but-valid, output a pending_order_management_plan that maximizes收益 while remaining inside the strategic path envelope.

Management rules:
- Entry replacement and post-fill bracket template updates are allowed only when they improve expectancy and remain consistent with the live strategic path.
- Entry replacement must remain management-layer replacement: reuse the currently active pending-entry template rather than redesigning a fresh tactical entry.
- Non-keep_order actions must carry watcher_trigger_condition with a concrete trigger_level.
- watcher_trigger_condition.trigger_type must be price_above_on_close or price_below_on_close.- If the path is no longer live, do not keep stale orders working; cancel or replace them decisively.

Output contract:
- pending_order_management_plan must target the current path_id and the current order context.
- `post_fill_bracket_template` means the bracket template to be used after the pending order is filled, not a live exchange exit order.
```

建议新增简单的 `user prompt prefix`：

```text
You are a top-tier 4h-1d order flow trader reviewing active pending orders. First audit whether the supporting path is still alive. If not, output conditional cancel/remove actions. If yes, output a watcher-managed conditional pending-order plan.
```

## 8. 实施验收标准

实施完成后，系统应满足以下验收标准：

1. `Stage1` 实际 prompt input 只保留 `3d / 4h / 1d` 战略输入与 `structural_refresh_context`。
   `7d avwap` 必须作为战略参考锚点保留在 `Stage1` 输入中。
   `i25 open_interest`、`i26 long_short_ratios`、`i27 options_surface` 也必须保留在 `Stage1` 输入中。
   `price_volume_structure / liquidation_density / TPO / RVWAP / EMA / FVG`、`funding / VPIN`、`CVD pack / divergence / whale_trades`、`footprint / orderbook_depth / absorption / initiation / exhaustion / high_volume_pulse` 也必须显式保留在 `Stage1` 输入中。
2. `Stage1` 实际输出不再包含 `script_rejections`。
3. `Stage1.current_path` 不再包含 `management_plan`。
4. `Stage1.failure_level.timeframe` 只能是 `4h | 1d | 4h-1d`。
5. `Stage1` 继续输出 `reevaluation_trigger`，且其 zone-trigger 时间框只能是 `4h | 1d | 4h-1d`。
6. `Stage1.map_summary` 必须保留 `location_3d`，且不再输出单独的 `regime_3d`。
7. `Stage2A` 继续输出 `entry_invalidation_level`，且其时间框允许 `15m | 15m-4h`。
8. `Stage2A` 不再输出 `management_plan`。
9. `Stage2A` 不再输出 `secondary_entry_plan`，只允许输出一个主 `entry_plan`。
10. `Stage2B` 必须独立输出 `position_management_plan`。
11. `Stage2C` 必须独立输出 `pending_order_management_plan`。
12. 运行时消费的持仓管理规则来源从 `Stage1.current_path.management_plan` 切换到 `Stage2B.position_management_plan`。
13. 运行时消费的挂单管理规则由 `Stage2C.pending_order_management_plan` 提供。
14. `Stage2A` 不再输出 `take_profit_1 / take_profit_2`；代码层必须直接继承 `Stage1.first_path_target / next_path_target` 并拼装给 watcher。
15. `Stage2A` 不再输出 `ttl_minutes`；代码层 / 配置层必须统一提供 entry plan 的 TTL。
16. `Stage2A` 不再输出 `entry_snapshot`；代码层必须自动生成对应的 execution metadata。
17. `Stage2A` 不再输出 `attempt_policy`；watcher / 代码层必须统一提供尝试次数与窗口规则。
18. `Stage2A` 在 `REQUEST_STAGE1_REEVALUATION` 分支必须返回 `tactical_entry_plan=null`，且 `reevaluation_reason` 必须为非空字符串。
19. `Stage2A` 在 `PATH_CONFIRMED` 分支必须返回完整 `tactical_entry_plan`，且 `reevaluation_reason` 必须为 `null`。
20. `Stage2B` 不再承担 `Stage1` 重评职责；其合同中不再存在 `REQUEST_STAGE1_REEVALUATION` 与 `reevaluation_reason`。
21. `Stage2C` 不再承担 `Stage1` 重评职责；其合同中不再存在 `REQUEST_STAGE1_REEVALUATION` 与 `reevaluation_reason`。
22. `Stage2B` 的 path 失活处理必须体现为当前仓位的 close / de-risk 动作，而不是重评请求。
23. `Stage2C` 的 path 失活处理必须体现为当前挂单的 cancel / remove stale exposure 动作，而不是重评请求。
24. `Stage2C` 的 `post_fill_bracket_template` 必须表示“挂单成交后的 bracket 模板”，而不是交易所上的真实退出单。
25. `Stage2B` 必须只在 `in_position` 下被调起。
26. `Stage2C` 只要存在活动入场挂单就必须被调起，其 `exposure_state` 必须为 `flat_with_live_entry_orders | in_position_with_live_entry_orders` 之一。
27. `Stage2A` 必须只在 `flat_no_orders` 下被调起。
28. 同一 symbol 的调度优先级必须为：持仓第一、挂单第二、入场第三。
29. 如果同一 symbol 同时存在持仓与活动挂单，`Stage2B` 必须优先于 `Stage2C`，且 `Stage2A` 不得生效；该场景下必须继续新发起 `Stage2C` LLM 请求，并由 watcher 按“Stage2B 主、Stage2C 次”的优先级消费计划。
30. 同一时间内，同方向最多只能有 `1` 笔持仓与 `1` 笔活动入场挂单；达到任一同方向上限后，系统不得再请求 `Stage2A` 开新仓。
31. `Stage2B / Stage2C` 不得在收到模型结果后立即执行动作；它们输出的是 watcher 要消费的条件化计划。
32. `Stage2B` 的 `add / reduce / exit_full / move_stop / update_take_profit` 默认都应带 `watcher_trigger_condition`；只有 `hold` 可以为 `null`。
33. `Stage2B.add` 不再输出完整 `add_plan`；改为输出 `add_ratio`，并要求 `reuse_current_entry_template=true`。
34. `Stage2B.add` 的语义必须是“沿用当前仓位已有 entry/bracket 模板做管理层加仓”，而不是重新做一轮 `Stage2A` 战术设计。
35. `Stage2B.move_stop / Stage2B.update_take_profit` 不得重给完整 bracket 计划；它们必须只输出改动字段，并要求 `reuse_current_bracket_template=true`。
36. `Stage2B.move_stop / Stage2B.update_take_profit` 的语义必须是“沿用当前仓位已有 bracket 模板做 patch-style 更新”。
37. `Stage2C.replace_entry` 不再输出完整 `replacement_entry_plan`；改为只输出 `replacement_entry_zone / replacement_entry_invalidation_level / replacement_stop_loss`，并要求 `reuse_current_entry_template=true`。
38. `Stage2C.replace_entry` 的语义必须是“沿用当前挂单已有 entry 模板做管理层替换”，而不是重新做一轮 `Stage2A` 战术设计。
39. `Stage2C` 的 `cancel_pending_order / replace_entry / update_post_fill_bracket_template` 默认都应带 `watcher_trigger_condition`；只有 `keep_order` 可以为 `null`。
40. `watcher_trigger_condition.trigger_type` 必须收敛为 `price_above_on_close | price_below_on_close`，而“站稳”的 bar 确认规则由 watcher 配置负责。
41. watcher 继续只消费 `Stage2A / Stage2B / Stage2C` 的条件计划，不回写 `Stage1` 合同。
42. `Stage1` 提示词中的输出约束必须收敛为语义规则版；字段枚举、必填、空值约束优先由 JSON schema / parser 负责。
43. 文档必须补齐 `Stage2A / Stage2B / Stage2C` 的完整提示词。
44. `Stage2B` 提示词必须明确以最大化收益为目标，并采用“两步工作流”：先审核持仓 path 是否存活，再输出条件化 close/de-risk 或 condition-based position management。
45. `Stage2C` 提示词必须明确以最大化收益为目标，并采用“两步工作流”：先审核挂单 path 是否存活，再输出条件化 cancel/remove stale exposure 或 condition-based pending-order management。
46. `workflow.stage1.min_overall_quality_for_new_entry_dispatch` 必须可在 `config.yaml` 配置。
47. 质量过滤只作用于新的入场分发；若已存在持仓或活动挂单，`Stage2B / Stage2C` 不得因质量过滤而停止运行。
48. `Stage2A` 的 active prompt 资产必须是 [workflow_stage2a/base.txt](/data/systems/llm/src/llm/prompt/workflow_stage2a/base.txt)；旧的 [workflow_stage2/base.txt](/data/systems/llm/src/llm/prompt/workflow_stage2/base.txt) 若仍保留，只能作为迁移历史参考，不得继续作为 active prompt 入口或实现依据。
