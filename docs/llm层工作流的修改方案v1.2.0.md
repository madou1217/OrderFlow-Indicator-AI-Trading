# LLM层工作流修改方案 v1.2.0

基于 /data/docs/订单流交易员交易流程V1.md 的工作流，将原有的 stage1 scan + stage2 entry 架构重构为三层架构。

## 版本变更记录

- v1.0.0：初版三层架构设计（代码层 + Stage1 + Stage2），基于26个指标
- v1.1.0：新增 i27 options_surface 指标融入，指标总数变为27个
- v1.2.0：修复 v1.1.0 复核中发现的7个问题

### v1.2.0 变更内容

1. 修复"path有具体价位就不会过时"的错误假设 — path可以因状态层/驱动层变化而过时，即使价格仍在图内
2. 重新定义Stage2为"有限自主的交易员"，增加thesis validity check步骤，不再是纯checklist执行器
3. 承认代码层同时输出原始数值和确定性映射标签，明确映射规则，不再宣称"无判断"
4. 将activation/failure的condition从自然语言改为结构化谓词
5. 补全管理层闭环：entry_snapshot + 结构化management_rules + reduce/exit/hold决策
6. 收紧gate设计：hard gate增加实时位置检查和失效点明确；soft gate从4选3改为3选2
7. 承认验证局限性，明确后续验证计划

---

## 修改动机

### 旧架构的问题（基于2026-03-27 ETH行情复盘）

1. Stage1 scan 每15分钟调用LLM，输出抽象标签（`sellers control`、`inside_range`、`conflicted`），Stage2 把这些标签当成方向指令服从
2. Scan 的高时框状态标签太"粘"——价格从2033涨到2080，scan仍然输出 `sellers control`
3. Scan 同时承担了"数据压缩"和"分析判断"两个职责，导致压缩出来的信息带有方向偏见
4. Stage2 有实时flow数据，但不敢覆盖scan的标签（19:39 UTC stage2看到 `reversal_to_buying` 但scan说 `sellers control`，选择服从scan）
5. 没有剧本切换机制——scan每次重新输出完整分析，但标签不变等于剧本不变

### 核心设计原则

- 数据压缩由代码完成，不调用LLM
- 所有分析判断集中在LLM调用中，不分散
- 慢思考（剧本+路径）和快思考（触发+执行+管理）分离到不同频率
- Stage1 输出价格锚定的 path objects，不输出抽象标签
- Stage2 是有限自主的交易员：对照 path 检查条件，同时有权评估 path thesis 是否仍然成立，在满足剧本切换规则时可以主动请求刷新
- Path objects 会过时——不仅当价格出图时，也在状态层或驱动层发生 material change 时

---

## 新架构：三层设计

```
indicator_engine (27个指标，含 i27 options_surface)
       │
       ▼
   代码层 (每15分钟，无LLM)
   输出: indicator_summary JSON
       │
       ├──────────────────────────┐
       ▼                          ▼
   Stage1 (每4H，LLM)         Stage2 (每15分钟，LLM)
   输入: indicator_summary      输入: indicator_summary (最新)
         + previous_paths              + stage1的paths
   输出: paths[]                       + stage1的thesis_premises
         + driver_attribution          + entry_snapshots (如有持仓)
         + thesis_premises       输出: decision + plan
         + switch_triggers             + path_evaluation
       │                              + thesis_validity_check
       └──► paths + premises ──►─┘     + management_action (如有持仓)
```

---

## 第一层：代码层（数据压缩）

### 调用频率
每个15分钟cycle，由代码执行，不调用LLM。

### 职责
将indicator_engine输出的27个指标，按工作流的四层结构（位置/状态/驱动/触发）压缩成固定schema的JSON。

### 设计原则
- 输出两类内容：**原始数值**和**确定性映射标签**
- 原始数值：直接从指标读取的数字（如 `oi_change_4h_pct: -2.3`、`current_rate: 0.0001`）
- 确定性映射标签：由固定规则从数值计算得出（如 `oi_label: long_unwind` 来自 `price_down + oi_down`）
- 确定性映射标签**不是**LLM的主观判断，但仍然是解释性标签，其映射规则必须明确记录，可审计、可修改
- 事件带 `confirmed_at` 时间戳，让Stage2知道信号的新鲜度

### 确定性映射规则表

以下标签由代码层通过固定规则生成。Stage1/Stage2消费这些标签时，应知道它们背后的映射逻辑：

| 字段 | 映射规则 | 可能的值 |
|------|---------|---------|
| `funding.bias` | rate > 0.0002 → long_heavy; rate < -0.0002 → short_heavy; else → neutral 或 slightly_long/short | neutral, slightly_long, slightly_short, long_heavy, short_heavy |
| `vpin.regime` | percentile_1d > 90 → extreme; > 75 → elevated; > 25 → normal; else → suppressed | suppressed, normal, elevated, extreme |
| `open_interest.oi_label` | price↑+oi↑ → leveraged_long_build; price↑+oi↓ → short_cover; price↓+oi↓ → long_unwind; price↓+oi↑ → fresh_short_build | leveraged_long_build, short_cover, long_unwind, fresh_short_build |
| `long_short_ratios.regime` | global_ratio > 2.0 → crowded_long; < 0.8 → crowded_short; else → balanced 或 unwind | crowded_long, crowded_short, balanced, unwind |
| `options_surface.atm_iv_regime` | 30日分位数 > 95p → extreme; > 75p → elevated; > 25p → normal; < 25p → compressed | compressed, normal, elevated, extreme |
| `options_surface.skew_state` | rr_25d < -3 → put_skewed; > 3 → call_skewed; else → neutral | put_skewed, call_skewed, neutral |
| `options_surface.term_structure_state` | front - second > 2 → front_rich; second - front > 2 → back_rich; else → flat | front_rich, back_rich, flat |
| `cvd_pack.spot_futures_relation` | sign(spot_4h_delta) == sign(futures_4h_delta) → aligned_buying/selling; else → divergent | aligned_buying, aligned_selling, divergent |
| `cvd_pack.flow_driver` | abs(futures_delta) > 2×abs(spot_delta) → futures_led; abs(spot_delta) > 2×abs(futures_delta) → spot_led; else → mixed | spot_led, futures_led, mixed |
| `ema_regime.regime` | price > ema100 && price > ema200 → above_both; price < both → below_both; else → between | above_both, between, below_both |
| `price_location.vs_*_value` | price > vah → above; price < val → below; else → inside | above, inside, below |
| `orderbook_depth.fake_order_risk` | 基于orderbook变化速率和撤单率的确定性阈值 | low, medium, high |

这些映射规则是固定的、可测试的。如果映射规则本身有问题（比如 `oi_label` 的四象限在某些场景下不够准确），应该修改映射规则的阈值，而不是让LLM去覆盖它。

### 输入
indicator_engine 输出的27个指标原始数据（已存在于 `feat.indicator_snapshot`、事件表及 `feat.options_surface_feature` 中）

### 输出：`indicator_summary` JSON

```json
{
  "meta": {
    "symbol": "ETHUSDT",
    "ts": "2026-03-26T18:30:00Z",
    "current_price": 2040.88
  },

  "位置层": {
    "pvs": {
      "4h": { "poc": 2070.11, "vah": 2077.48, "val": 2062.66, "hvn": [2070, 2045], "lvn": [2055, 2085] },
      "1d": { "poc": 2071.45, "vah": 2085.12, "val": 2058.15, "hvn": [2071, 2040], "lvn": [2055, 2090] }
    },
    "tpo": {
      "4h": { "poc": 2068.86, "ib_high": 2075.3, "ib_low": 2062.5, "single_prints": [{"low": 2055, "high": 2058}] },
      "1d": { "poc": 2070.0, "ib_high": 2080.0, "ib_low": 2065.0, "single_prints": [] }
    },
    "avwap": {
      "anchors": [
        { "anchor_event": "session_open", "price": 2066.18 },
        { "anchor_event": "swing_low_2033", "price": 2045.50 },
        { "anchor_event": "weekly_open", "price": 2071.99 }
      ]
    },
    "rvwap": {
      "15m": { "mid": 2043.12, "p1s": 2048.50, "m1s": 2037.74, "p2s": 2053.88, "m2s": 2032.36 },
      "4h":  { "mid": 2066.55, "p1s": 2078.30, "m1s": 2054.80, "p2s": 2090.05, "m2s": 2043.05 },
      "1d":  { "mid": 2071.00, "p1s": 2085.00, "m1s": 2057.00, "p2s": 2099.00, "m2s": 2043.00 }
    },
    "ema_regime": {
      "4h": { "ema100": 2080.5, "ema200": 2095.3, "regime": "below_both", "price_vs_ema100_pct": -1.95 },
      "1d": { "ema100": 2120.0, "ema200": 2150.0, "regime": "below_both", "price_vs_ema100_pct": -3.88 }
    },
    "liquidation_density": {
      "long_peaks": [{"price": 2030, "usd_millions": 12.5}, {"price": 2020, "usd_millions": 8.3}],
      "short_peaks": [{"price": 2080, "usd_millions": 6.1}, {"price": 2100, "usd_millions": 15.2}]
    },
    "fvg": [
      { "type": "bearish", "high": 2058.0, "low": 2050.0, "filled": false },
      { "type": "bullish", "high": 2042.0, "low": 2038.0, "filled": false }
    ],
    "price_location": {
      "vs_4h_value": "below",
      "vs_1d_value": "below",
      "vs_4h_rvwap_band": "between_m1s_and_m2s",
      "vs_1d_rvwap_band": "between_m1s_and_m2s",
      "vs_ema_regime": "below_both"
    }
  },

  "状态层": {
    "funding": {
      "current_rate": 0.0001,
      "8h_avg": 0.00008,
      "bias": "slightly_long"
    },
    "vpin": {
      "current": 0.62,
      "percentile_1d": 75,
      "regime": "elevated"
    },
    "open_interest": {
      "oi_usd": 485000000,
      "oi_change_4h_pct": -2.3,
      "oi_change_1d_pct": -5.1,
      "price_vs_oi_regime": "price_down_oi_down",
      "oi_label": "long_unwind"
    },
    "long_short_ratios": {
      "global_ratio": 1.85,
      "top_trader_position_ratio": 1.42,
      "top_trader_account_ratio": 1.15,
      "regime": "crowded_long",
      "change_4h": -0.12
    },
    "options_surface": {
      "atm_iv_front": 58.2,
      "atm_iv_second": 52.1,
      "atm_iv_regime": "elevated",
      "rr_25d_front": -4.8,
      "skew_state": "put_skewed",
      "term_structure_state": "front_rich"
    }
  },

  "驱动层": {
    "cvd_pack": {
      "futures": {
        "15m_delta": -10143.57,
        "1h_delta": -25000,
        "4h_cum_delta": -37314.12,
        "4h_cvd_slope": "falling"
      },
      "spot": {
        "15m_delta": -3200,
        "1h_delta": -8500,
        "4h_cum_delta": -12000,
        "4h_cvd_slope": "falling"
      },
      "spot_futures_relation": "aligned_selling",
      "flow_driver": "futures_led"
    },
    "divergence_events": [
      {
        "type": "bullish",
        "timeframe": "4h",
        "confirmed_at": "2026-03-26T18:20:00Z",
        "price_at_confirm": 2033.22,
        "spot_led": true,
        "z_score": 2.4,
        "significance": "high"
      }
    ],
    "whale_trades": {
      "4h_net_usd": -1426012,
      "4h_buy_usd": 800000,
      "4h_sell_usd": 2226012,
      "bias": "selling",
      "recent_large_trades": [
        { "ts": "2026-03-26T18:15:00Z", "side": "sell", "usd": 450000, "market": "futures" }
      ]
    }
  },

  "触发层": {
    "footprint": {
      "15m": {
        "stacked_buy_imbalances": [{"price": 2039.5, "count": 4}],
        "stacked_sell_imbalances": [],
        "unfinished_auction": { "side": "buy", "price": 2041.0 },
        "delta": 1811.45
      }
    },
    "orderbook_depth": {
      "obi_fut": -0.27,
      "ofi_norm_fut": -0.84,
      "microprice_fut": 2040.55,
      "spot_confirm": false,
      "fake_order_risk": "low",
      "exec_confirm_fut": false,
      "nearest_bid_wall": { "price": 2033.8, "size_usd": 2500000 },
      "nearest_ask_wall": { "price": 2043.15, "size_usd": 1800000 }
    },
    "absorption_events": [
      {
        "type": "bearish",
        "price": 2074.10,
        "confirmed_at": "2026-03-26T16:45:00Z",
        "spot_confirmed": true
      }
    ],
    "initiation_events": [],
    "exhaustion_events": [
      {
        "type": "selling",
        "price": 2042.09,
        "confirmed_at": "2026-03-26T18:25:00Z",
        "spot_confirmed": true
      }
    ],
    "high_volume_pulse": {
      "last_pulse_ts": "2026-03-26T18:18:00Z",
      "intrabar_poc": 2039.8,
      "direction": "sell",
      "volume_zscore": 3.2
    }
  }
}
```

---

## 第二层：Stage1（建筑师 — 慢思考，LLM）

### 调用频率
- 每个4H K线边界（00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC）
- 或：Stage2 标记 `request_stage1_refresh = true` 时（见Stage2的thesis validity check）

### 职责
执行工作流第2步（剧本选择）+ 第2.5步（scenario/path object构建）+ 第3步（驱动归因）

### 输入

```json
{
  "task": "基于以下数据，执行工作流第2步+第2.5步+第3步",
  "indicator_summary": { /* 代码层输出的完整JSON */ },
  "previous_paths": { /* 上一次stage1输出的path objects，如果有的话 */ },
  "recent_trade_history": {
    "last_trade": null,
    "account_balance": 5000,
    "active_positions": []
  }
}
```

### Stage1 要做的四件事

**第一：选剧本（工作流第2步）**

基于状态层数据判断当前属于哪种剧本：
- 延续：OI同向扩张 + ratio不拥挤 + funding没反噬
- 拥挤反转：极限位置 + ratio拥挤 + OI堆积但推进效率下降
- 回归价值：离开value但没接受

options_surface 在剧本选择中的作用：
- `atm_iv_regime = extreme` + `skew_state = put_skewed` + `term_structure_state = front_rich` → 市场定价短期恐慌，增强"拥挤反转"剧本的可信度
- `atm_iv_regime = compressed` → 波动率压缩，增强"延续"或"回归价值"剧本的可信度
- `skew_state = call_skewed` + price在高位 → 做多拥挤的额外确认信号

options_surface 不改变剧本选择的核心逻辑（仍由 OI/ratio/funding/位置 决定），只作为辅助权重调节。

**第二：构建 scenario/path objects（工作流第2.5步）**

输出1-2条path，每条包含六个价格化字段：
- thesis：当前主剧本是什么，为什么是它
- activation_level：哪个价格带被确认后剧本激活（结构化谓词）
- first_path_target：激活后第一段目标
- next_path_target：第一段走出后下一层目标
- failure_level：哪个价格带失守则剧本失效（结构化谓词）
- failure_switch：失效后切换到哪个替代path

options_surface 在 path 构建中的作用：
- thesis 的 why 里可以引用 IV/skew 状态作为佐证
- options_surface 不作为 activation/target/failure 的锚点（这些仍由位置层的 PVS/TPO/RVWAP/EMA 决定）

**第三：驱动归因（工作流第3步）**

基于驱动层数据判断 spot-led / futures-led / mixed，以及当前驱动是否支撑剧本。

options_surface 不参与驱动归因（驱动归因只看 CVD/divergence/whale，不看期权）。

**第四：声明 thesis 成立的前提条件（thesis_premises）**

Stage1 必须显式声明当前剧本依赖哪些状态层/驱动层前提。这些前提由Stage2在每个cycle检查——如果前提不再成立，Stage2应请求Stage1刷新，即使价格仍在path的图内。

### 输出

```json
{
  "meta": {
    "stage1_ts": "2026-03-26T16:05:00Z",
    "next_scheduled_refresh": "2026-03-26T20:00:00Z"
  },

  "script_selection": {
    "selected": "continuation_bearish",
    "why": "price below 4h/1d value, OI is unwinding (long_unwind), ratio still crowded_long, spot+futures CVD aligned selling, EMA regime below_both. options: IV elevated + put_skewed confirms downside fear is being priced, but not yet at extreme levels that would favor reversal"
  },

  "driver_attribution": {
    "flow_driver": "futures_led",
    "spot_confirming": false,
    "driver_quality": "price falling with futures-led selling but spot not fully confirming — move is aggressive but potentially fragile if futures exhaustion appears",
    "divergence_active": false
  },

  "thesis_premises": {
    "description": "当前剧本成立依赖以下前提。如果任意一条不再成立，Stage2应 request_stage1_refresh",
    "premises": [
      {
        "id": "p1",
        "field": "open_interest.oi_label",
        "expected": ["long_unwind", "fresh_short_build"],
        "violation": "OI regime翻转为leveraged_long_build或short_cover，说明仓位属性根本改变"
      },
      {
        "id": "p2",
        "field": "long_short_ratios.regime",
        "expected": ["crowded_long"],
        "violation": "ratio不再crowded，squeeze前提消失"
      },
      {
        "id": "p3",
        "field": "cvd_pack.flow_driver",
        "expected": ["futures_led", "mixed"],
        "violation": "flow_driver翻转为spot_led buying，驱动属性根本改变"
      },
      {
        "id": "p4",
        "field": "cvd_pack.spot_futures_relation",
        "expected": ["aligned_selling", "divergent"],
        "violation": "spot和futures同时翻转为aligned_buying，bearish thesis不再成立"
      }
    ]
  },

  "paths": [
    {
      "id": "path_a",
      "role": "primary",
      "thesis": "4H bearish continuation — long_unwind driving price through value lows, EMA regime bearish, ratio still crowded. IV elevated + put_skewed supports downside thesis",
      "activation_level": {
        "low": 2050,
        "high": 2055,
        "condition": {
          "type": "price_below_on_close",
          "timeframe": "15m",
          "reference": "low",
          "bars_held": 1
        }
      },
      "first_path_target": {
        "low": 2040,
        "high": 2042,
        "reason": "4h HVN + 1d sigma2 support"
      },
      "next_path_target": {
        "low": 2033,
        "high": 2035,
        "reason": "liquidation density peak + 4h sigma2 floor"
      },
      "failure_level": {
        "low": 2063,
        "high": 2068,
        "condition": {
          "type": "price_above_on_close",
          "timeframe": "15m",
          "reference": "high",
          "bars_held": 2,
          "requires": {
            "flow_driver_in": ["spot_led"],
            "spot_futures_relation_in": ["aligned_buying"]
          }
        }
      },
      "failure_switch": "path_b",
      "setup_type": "A_continuation",
      "entry_side": "SHORT"
    },
    {
      "id": "path_b",
      "role": "alternate",
      "thesis": "4H reclaim reversal — if bearish continuation exhausts at sigma2/liq zone, crowded longs get flushed then price reclaims",
      "activation_level": {
        "low": 2046,
        "high": 2051,
        "condition": {
          "type": "price_above_on_close",
          "timeframe": "15m",
          "reference": "high",
          "bars_held": 1,
          "requires": {
            "precondition": "price_visited_below",
            "precondition_level": 2035,
            "trigger_events": {
              "any": [
                { "event_type": "exhaustion", "subtype": "selling" },
                { "event_type": "divergence", "subtype": "bullish" }
              ]
            }
          }
        }
      },
      "first_path_target": {
        "low": 2064,
        "high": 2068,
        "reason": "4h/1d value low and AVWAP"
      },
      "next_path_target": {
        "low": 2072,
        "high": 2080,
        "reason": "4h POC + 1d value mid + bearish absorption cluster"
      },
      "failure_level": {
        "low": 2030,
        "high": 2033,
        "condition": {
          "type": "price_below_on_close",
          "timeframe": "15m",
          "reference": "low",
          "bars_held": 2
        }
      },
      "failure_switch": "path_a",
      "setup_type": "B_reversal",
      "entry_side": "LONG"
    }
  ],

  "script_switch_triggers": {
    "description": "Stage2 应在以下条件满足时请求Stage1刷新",
    "request_refresh_conditions": [
      "thesis_premises 中任意一条前提不再成立",
      "price moves beyond ALL defined path targets/failures (completely outside the map)",
      "当前 paths 已超过 4 小时且新的 4H K线已形成",
      "Stage2 三次连续评估 path thesis 为 weakened 或 invalid"
    ]
  }
}
```

### Stage1 输出设计原则

1. 所有价位都是具体数字，不是 "near support" 或 "at resistance"
2. activation/failure 的 condition 使用结构化谓词（见下方谓词类型说明），不使用自然语言
3. failure_switch 是预定义的，Stage2不需要自己想"失败了该怎么办"
4. setup_type 直接绑定到工作流第4步的三种setup（A/B/C），Stage2知道该用哪套触发条件检查
5. thesis_premises 显式声明剧本前提，让Stage2可以结构化地检查前提是否仍然成立
6. options_surface 只出现在 thesis 的 why/佐证中，不作为 activation/target/failure 的价格锚点

### 结构化谓词类型说明

condition 字段使用以下标准化谓词类型，Stage2可以确定性地评估true/false：

| 谓词类型 | 含义 | 参数 |
|---------|------|------|
| `price_below_on_close` | 价格在指定timeframe的close低于reference水平 | `timeframe`, `reference`(low/high), `bars_held`(连续N根) |
| `price_above_on_close` | 价格在指定timeframe的close高于reference水平 | 同上 |
| `requires.flow_driver_in` | 当前flow_driver必须属于指定集合 | 值列表 |
| `requires.spot_futures_relation_in` | 当前spot_futures_relation必须属于指定集合 | 值列表 |
| `requires.precondition` | 在满足主条件之前，必须先满足前提 | `price_visited_below/above` + level |
| `requires.trigger_events.any` | 必须有指定事件中的至少一个已confirmed | 事件类型列表 |

Stage2评估这些谓词时，只需要对照 indicator_summary 里的数值做比较，不需要"理解"自然语言。

---

## 第三层：Stage2（交易员 — 有限自主，LLM）

### 定位

Stage2 不是纯 checklist 执行器，而是**有限自主的交易员**：
- 它对照 path 的结构化条件检查 activation/failure — 这部分是确定性的
- 它评估 path thesis 是否仍然成立（thesis validity check）— 这部分需要LLM判断
- 它在满足剧本切换规则（工作流V1 L43-49）时，有权主动请求Stage1刷新
- 它**不会**自己重新设计path或选择新剧本 — 这是Stage1的职责

### 调用频率
每个15分钟cycle。

### 职责
- 评估 thesis 有效性（thesis validity check）
- 执行工作流第4步（触发确认）+ 第5步（执行）
- 执行工作流第6步（持仓管理，如有持仓）

### 输入

```json
{
  "task": "执行thesis validity check + 工作流第4步+第5步+第6步",
  "indicator_summary": { /* 代码层输出的最新JSON */ },
  "paths": { /* Stage1输出的path objects */ },
  "thesis_premises": { /* Stage1输出的前提条件 */ },
  "stage1_ts": "2026-03-26T16:05:00Z",
  "active_positions": [
    {
      "side": "LONG",
      "entry_price": 2046.5,
      "current_price": 2058.0,
      "unrealized_pnl_pct": 0.49,
      "path_id": "path_b",
      "entry_snapshot": {
        "ts": "2026-03-26T19:30:00Z",
        "flow_driver": "mixed",
        "spot_futures_relation": "divergent",
        "oi_label": "long_unwind",
        "ratio_regime": "crowded_long",
        "atm_iv_regime": "elevated",
        "skew_state": "put_skewed"
      },
      "management_rules": {
        "reduce_50pct": {
          "any": [
            { "field": "cvd_pack.flow_driver", "becomes": ["futures_led"], "and_direction": "selling" },
            { "field": "open_interest.oi_label", "becomes": ["fresh_short_build"] },
            { "field": "price_location.vs_4h_value", "becomes": ["below"], "after_was": "inside" }
          ]
        },
        "exit_full": {
          "any": [
            { "type": "price_below", "level": 2032.0 },
            { "type": "path_failure_level_hit", "path_id": "path_b" }
          ]
        }
      }
    }
  ],
  "account": { "balance": 5000, "max_leverage": 5 }
}
```

### Stage2 执行流程

**第零步：thesis validity check（每次必做）**

在做任何交易评估之前，Stage2先检查Stage1的thesis是否仍然成立：

1. 逐条检查 `thesis_premises`：对比 indicator_summary 中的实际值与 premise 的 expected 值
2. 评估整体thesis健康度：
   - `valid`：所有前提仍然成立
   - `weakened`：1条前提不成立，但核心逻辑尚可
   - `invalid`：2条以上前提不成立，或核心前提（如OI regime）翻转

```json
"thesis_validity_check": {
  "premises_check": [
    { "id": "p1", "field": "open_interest.oi_label", "expected": ["long_unwind","fresh_short_build"], "actual": "short_cover", "pass": false },
    { "id": "p2", "field": "long_short_ratios.regime", "expected": ["crowded_long"], "actual": "balanced", "pass": false },
    { "id": "p3", "field": "cvd_pack.flow_driver", "expected": ["futures_led","mixed"], "actual": "spot_led", "pass": false },
    { "id": "p4", "field": "cvd_pack.spot_futures_relation", "expected": ["aligned_selling","divergent"], "actual": "aligned_buying", "pass": false }
  ],
  "thesis_status": "invalid",
  "request_stage1_refresh": true,
  "reason": "4/4 premises violated: OI翻转为short_cover, ratio不再crowded, flow翻转为spot_led aligned_buying — bearish continuation thesis完全失效"
}
```

如果 `thesis_status = invalid` 且 `request_stage1_refresh = true`：
- Stage2 本轮不做新开仓
- 等Stage1刷新后拿到新的paths再继续
- 但如果有持仓，仍然执行管理步骤

**第一步：判断哪条path处于激活状态**

```
对每条path:
  1. 评估 activation_level 的结构化谓词是否为true
  2. 评估 failure_level 的结构化谓词是否为true
  3. 如果failure为true → 执行failure_switch
  4. 如果activation为true → 进入触发检查
  5. 如果都不满足 → path仍在等待中
```

注意：path之间的切换可以在failure_level未被击穿时发生——当alternate path的activation条件被满足时（包括其precondition和trigger_events），Stage2可以判定"primary path已经跑完了它的路径，alternate path的激活条件已满足"，这是合法的切换，不需要primary path先hit failure。

**第二步：如果有path激活，执行工作流第4步的触发检查**

根据path的 `setup_type` 选择对应的触发条件集：

如果是 `A_continuation`（延续单），从 indicator_summary 的触发层检查：
- initiation 事件？
- footprint stacked imbalance？
- OBI / OFI / microprice 同向？
- spot_confirm = true？
- fake_order_risk 低？
- OI 状态仍支持？

如果是 `B_reversal`（反转单），从 indicator_summary 的触发层检查：
- absorption 或 exhaustion 已确认？
- 有效 divergence？
- spot 没有继续推同方向？
- footprint 里出现失衡失败 / 推不动价格？

如果是 `C_value_return`（回归价值单），从 indicator_summary 的触发层检查：
- 先突破了 VAH/VAL/IB 外沿？
- 但没有 OI 扩张、spot 确认、持续 OFI？
- 已经收回 value 内？

**第三步：过硬过滤器**

Hard gate（必须全部满足）：
- **位置仍在边缘**：当前价格仍在 value edge / sigma band / liq zone / IB / single print 一类真边缘，不是从activation后已经回到了区间中间（用 indicator_summary 的 price_location + 位置层数据实时判断，不依赖activation时的快照）
- **触发已确认**：上面的checklist通过
- **失效点明确**：path的failure_level + stop_loss已定义且是可执行的价位

Soft gate（满足2/3）：
- 状态清楚：OI/ratio/funding/VPIN/options_surface 综合判断当前是 build、unwind、crowding 还是恐慌见顶
- 驱动清楚：spot-led / futures-led / mixed，且驱动方向与交易方向一致或至少不矛盾
- 盘口真实：OBI/OFI 与成交同向，spot_confirm 在，fake_order_risk 不高

**第四步：如果通过，执行工作流第5步**

用触发层的1m/100ms数据（footprint intrabar_poc、OFI、microprice）优化入场点。

**第五步：如有持仓，执行工作流第6步管理**

持仓管理在trigger check之前独立执行，不受新开仓逻辑影响。

管理流程：
1. 对比 entry_snapshot vs 当前 indicator_summary，检查 management_rules 中的每条规则
2. 如果任何 `exit_full` 条件满足 → 输出 exit 指令
3. 如果任何 `reduce_50pct` 条件满足 → 输出 reduce 指令
4. 如果都不满足 → hold，可选附加判断："驱动是否在减弱但尚未翻转"

### 输出

#### 无持仓、无交易时：

```json
{
  "thesis_validity_check": {
    "premises_check": [
      { "id": "p1", "actual": "long_unwind", "pass": true },
      { "id": "p2", "actual": "crowded_long", "pass": true },
      { "id": "p3", "actual": "futures_led", "pass": true },
      { "id": "p4", "actual": "aligned_selling", "pass": true }
    ],
    "thesis_status": "valid",
    "request_stage1_refresh": false
  },

  "path_evaluation": {
    "path_a": {
      "status": "activation_pending",
      "activation_predicate": { "price_below_on_close_15m": 2050, "current_close": 2058, "met": false }
    },
    "path_b": {
      "status": "waiting",
      "detail": "precondition not met (price has not visited below 2035)"
    }
  },

  "decision": "NO_TRADE",
  "reason": "Path A activation not yet triggered (15m close still above 2055); Path B precondition not met"
}
```

#### 无持仓、有交易时：

```json
{
  "thesis_validity_check": {
    "premises_check": [
      { "id": "p1", "actual": "long_unwind", "pass": true },
      { "id": "p2", "actual": "crowded_long", "pass": true },
      { "id": "p3", "actual": "mixed", "pass": true },
      { "id": "p4", "actual": "divergent", "pass": true }
    ],
    "thesis_status": "valid",
    "request_stage1_refresh": false
  },

  "path_evaluation": {
    "path_b": {
      "status": "activated",
      "activation_predicate": {
        "price_above_on_close_15m": 2051,
        "current_close": 2050.5,
        "met": true,
        "precondition_met": true,
        "trigger_events_met": ["selling_exhaustion at 2042.09", "bullish_divergence at 2033.22"]
      }
    }
  },

  "trigger_checklist": {
    "setup_type": "B_reversal",
    "absorption_or_exhaustion": { "pass": true, "detail": "selling_exhaustion at 2042.09, confirmed 18:25Z" },
    "valid_divergence": { "pass": true, "detail": "bullish divergence at 2033.22, z_score=2.4, spot_led=true" },
    "spot_not_continuing": { "pass": true, "detail": "spot 4h CVD slope no longer falling" },
    "footprint_failure": { "pass": true, "detail": "stacked_buy_imbalances at 2039.5, unfinished_auction buy at 2041.0" }
  },

  "hard_gate": {
    "location_at_edge": { "pass": true, "detail": "current price 2050.5 at 4h RVWAP -1σ to -2σ zone + near liq density peak at 2030" },
    "trigger_confirmed": { "pass": true, "detail": "4/4 reversal checklist conditions met" },
    "invalidation_defined": { "pass": true, "detail": "stop_loss=2032.0 (path_b failure_level), clearly executable" }
  },

  "soft_gate": {
    "status_clear": { "pass": true, "detail": "long_unwind + crowded_long + IV elevated + put_skewed = panic priced in, squeeze potential" },
    "driver_clear": { "pass": true, "detail": "futures_led selling exhausting, spot divergence confirmed, flow turning mixed" },
    "orderbook_real": { "pass": false, "detail": "obi_fut=-0.27, exec_confirm=false; bid_wall at 2033.8 holds but orderbook not confirming buy side" },
    "score": "2/3 pass"
  },

  "decision": "LONG",
  "plan": {
    "entry": 2046.5,
    "stop_loss": 2032.0,
    "take_profit_1": 2064.0,
    "take_profit_2": 2075.0,
    "leverage": 2,
    "horizon": "4h to 1d",
    "path_id": "path_b",
    "entry_snapshot": {
      "ts": "2026-03-26T19:30:00Z",
      "flow_driver": "mixed",
      "spot_futures_relation": "divergent",
      "oi_label": "long_unwind",
      "ratio_regime": "crowded_long",
      "atm_iv_regime": "elevated",
      "skew_state": "put_skewed"
    },
    "management_rules": {
      "reduce_50pct": {
        "any": [
          { "field": "cvd_pack.flow_driver", "becomes": ["futures_led"], "and_direction": "selling" },
          { "field": "open_interest.oi_label", "becomes": ["fresh_short_build"] },
          { "field": "price_location.vs_4h_value", "becomes": ["below"], "after_was": "inside" }
        ]
      },
      "exit_full": {
        "any": [
          { "type": "price_below", "level": 2032.0 },
          { "type": "path_failure_level_hit", "path_id": "path_b" }
        ]
      }
    }
  }
}
```

#### 有持仓时的管理输出：

```json
{
  "management_evaluation": {
    "position": {
      "side": "LONG",
      "entry_price": 2046.5,
      "current_price": 2058.0,
      "unrealized_pnl_pct": 0.49,
      "path_id": "path_b"
    },
    "driver_comparison": {
      "entry": { "flow_driver": "mixed", "oi_label": "long_unwind", "spot_futures_relation": "divergent" },
      "current": { "flow_driver": "spot_led", "oi_label": "short_cover", "spot_futures_relation": "aligned_buying" },
      "assessment": "driver improved — shifted from mixed/divergent at entry to spot_led/aligned_buying, supports continued hold"
    },
    "rules_check": {
      "reduce_50pct_triggered": false,
      "exit_full_triggered": false
    },
    "management_action": "HOLD",
    "reason": "driver has improved since entry; price approaching first_path_target 2064-2068; no reduce/exit rules triggered"
  }
}
```

---

## i27 options_surface 融入总结

### 各层使用方式

| 层 | 如何使用 options_surface | 权重 |
|----|------------------------|------|
| 代码层 | 从 `feat.options_surface_feature` 提取 ATM IV / RR / skew / term structure，计算 regime 确定性映射标签 | 纯数据提取 |
| Stage1 剧本选择 | 作为辅助权重：IV extreme + put_skewed 增强反转信心；IV compressed 增强延续/突破信心 | 辅助，不改变核心逻辑 |
| Stage1 path 构建 | 只出现在 thesis 的 why 佐证中，不作为 activation/target/failure 的价格锚点 | 仅佐证 |
| Stage1 thesis_premises | 不作为前提条件（options变化不构成剧本失效的理由） | 无 |
| Stage1 驱动归因 | 不参与 | 无 |
| Stage2 触发检查 | 不参与（触发层由 footprint/orderbook/absorption/exhaustion 负责） | 无 |
| Stage2 hard gate | 不参与 | 无 |
| Stage2 soft gate | 增强"状态清楚"这一条的判断质量 | 辅助 |
| Stage2 management | 不作为 reduce/exit 的规则条件 | 无 |

---

## 与旧架构的对比

| 维度 | 旧架构 | v1.2.0 |
|------|-------|--------|
| 指标数量 | 26个 | 27个（+i27 options_surface） |
| 数据压缩 | Stage1 scan (LLM) | 代码层（无LLM，输出原始数值 + 确定性映射标签） |
| Stage1频率 | 每15分钟 | 每4H或Stage2请求刷新 |
| Stage1输出 | 抽象标签 | 价格化path objects + 结构化谓词 + thesis_premises |
| Stage2定位 | 服从scan标签的执行器 | 有限自主的交易员（可评估thesis有效性，可请求刷新） |
| Path过时检测 | 无 | thesis_premises逐条检查 + 价格出图 + 时间超限 |
| 剧本切换 | 无机制 | failure_switch预定义 + thesis validity驱动的refresh请求 |
| Condition格式 | 自然语言 | 结构化谓词（price_below_on_close等） |
| 数据新鲜度 | Stage2比scan晚7分钟 | Stage2直接拿代码层实时数据 |
| 持仓管理 | 无闭环 | entry_snapshot + management_rules + driver对比 |
| Gate设计 | hard 2条 + soft 4选3 | hard 3条（含实时位置检查）+ soft 3选2 |
| LLM调用次数/小时 | 8次 | 5次（Stage1 1次 + Stage2 4次） |
| 期权数据 | 无 | 状态层增加 IV/skew/term structure |

---

## 用2026-03-27 ETH行情验证

### 旧系统实际表现
17:00-20:55 UTC，16次调用，全部NO_TRADE。价格从2066跌到2033又涨到2080，一笔未做。

### v1.2.0 理论表现

| 时间(北京) | 价格 | 系统行为 |
|-----------|------|---------|
| 01:00 | 2063 | Stage1建立Path A(空延续) + Path B(reclaim反转) + thesis_premises(4条前提) |
| 01:45 | 2058→跌 | Stage2: thesis valid, Path A activation谓词检查(15m close < 2050)尚未触发 |
| 02:00 | 2045 | Stage2: thesis valid, Path A activated(15m close < 2050), trigger check通过, hard gate(位置在4h RVWAP -1σ~-2σ), 做空 target=2042/2033 |
| 02:15 | 2033 | Stage2: Path A next_target到达; management check: driver仍aligned_selling → hold到target; 同时检测exhaustion+divergence → Path B precondition met |
| 02:30 | 2040 | Stage2: 空单管理 — 接近target, 考虑止盈; thesis_premises开始weakened(OI从long_unwind变化中) |
| 03:00 | 2043 | Stage2: thesis_premises p1(oi_label)变为short_cover, p4变为divergent → thesis_status=weakened; Path B activation谓词尚未触发(需站上2046-2051) |
| 03:30 | 2050 | Stage2: thesis_premises 3/4 violated → thesis_status=invalid → request_stage1_refresh; 同时Path B activation谓词=true(15m close > 2051 + precondition met + trigger_events met); 空单已平, Path B trigger check通过, hard gate通过(位置在4h RVWAP -1σ zone), 做多 target=2064-2068 |
| 04:00 | 2065 | Stage1刷新: 新paths基于当前状态(short_cover, spot_led buying); Stage2: 多单management check — driver improved(spot_led aligned_buying), hold |
| 04:15 | 2070 | Stage2: first_target 2064-2068到达, management评估driver是否支持hold到next_target 2072-2080 |

### 与v1.1.0的差异

v1.1.0在03:30时刻，Stage2只能"对照path checklist打勾"。v1.2.0在03:30时刻，Stage2通过thesis validity check发现3/4 premises violated，主动触发request_stage1_refresh，同时识别Path B activation满足，可以在不等Stage1刷新的情况下执行切换。

### 验证局限性说明

以上验证基于单日单品种的理论回放，用于说明设计逻辑的可行性。它不能证明"稳定高质量"。完整验证需要：
1. 对多个不同行情类型（趋势延续、V型反转、区间震荡、假突破）做理论回放
2. 实现后在shadow mode下跑真实行情，对比新旧系统的决策差异
3. 建立评估指标体系（不仅是PnL，还包括：decision quality score、thesis accuracy、switch latency等）
