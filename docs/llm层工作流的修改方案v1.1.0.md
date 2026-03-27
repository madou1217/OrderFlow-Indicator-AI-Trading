# LLM层工作流修改方案 v1.1.0

基于 /data/docs/订单流交易员交易流程V1.md 的工作流，将原有的 stage1 scan + stage2 entry 架构重构为三层架构。

## 版本变更记录

- v1.0.0：初版三层架构设计（代码层 + Stage1 + Stage2），基于26个指标
- v1.1.0：新增 i27 options_surface 指标融入，指标总数变为27个

### v1.1.0 变更内容

1. 代码层 indicator_summary 状态层新增 `options_surface` 字段
2. Stage1 剧本选择逻辑纳入 IV/skew 作为辅助判断
3. Stage2 soft gate 的"状态清楚"条件纳入 options_surface

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
- 慢思考（剧本+路径）和快思考（触发+执行）分离到不同频率
- Stage1 输出价格锚定的 path objects，不输出抽象标签
- Stage2 对照 path 检查条件，不需要自己"理解市场"

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
   输出: paths[]                输出: decision + plan
         + driver_attribution          + path_evaluation
         + switch_triggers             + script_switch
       │                          │
       └──► paths ──────────────►─┘
```

---

## 第一层：代码层（数据压缩）

### 调用频率
每个15分钟cycle，由代码执行，不调用LLM。

### 职责
将indicator_engine输出的27个指标，按工作流的四层结构（位置/状态/驱动/触发）压缩成固定schema的JSON。

### 设计原则
- 全是数字和事实，不包含任何分析判断性标签（不输出 `sellers control`、`fragile`、`conflicted`）
- `price_location` 是唯一的"计算结果"，但它是纯数学比较（价格和价位的大小关系），不是分析判断
- 事件带 `confirmed_at` 时间戳，让Stage2知道信号的新鲜度

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

### i27 options_surface 在代码层的说明

`options_surface` 位于状态层，与 funding/vpin/open_interest/long_short_ratios 并列。

字段来源：
- `atm_iv_front` / `atm_iv_second`：从 `feat.options_surface_feature` 读取前月和次月 ATM IV
- `atm_iv_regime`：代码层根据 ATM IV 的30日分位数计算，离散为 `compressed`（<25p）/ `normal`（25-75p）/ `elevated`（>75p）/ `extreme`（>95p）
- `rr_25d_front`：25-delta risk reversal，正值=call偏贵，负值=put偏贵
- `skew_state`：代码层根据 rr_25d 幅度离散为 `put_skewed`（<-3）/ `call_skewed`（>3）/ `neutral`（-3到3）
- `term_structure_state`：代码层比较 front vs second ATM IV，离散为 `front_rich`（front > second + 2）/ `back_rich`（second > front + 2）/ `flat`

所有计算均为纯数学比较，不涉及LLM判断。

---

## 第二层：Stage1（建筑师 — 慢思考，LLM）

### 调用频率
- 每个4H K线边界（00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC）
- 或：Stage2 标记 `request_stage1_refresh = true` 时

### 职责
执行工作流第2步（剧本选择）+ 第2.5步（scenario/path object构建）+ 第3步（驱动归因）

### 输入

```json
{
  "task": "基于以下数据，执行工作流第2步+第2.5步+第3步",
  "indicator_summary": { /* 代码层输出的完整JSON */ },
  "previous_paths": { /* 上一次stage1输出的path objects，如果有的话，用于判断是否需要修改还是全部重建 */ },
  "recent_trade_history": {
    "last_trade": null,
    "account_balance": 5000,
    "active_positions": []
  }
}
```

### Stage1 要做的三件事

**第一：选剧本（工作流第2步）**

基于状态层数据判断当前属于哪种剧本：
- 延续：OI同向扩张 + ratio不拥挤 + funding没反噬
- 拥挤反转：极限位置 + ratio拥挤 + OI堆积但推进效率下降
- 回归价值：离开value但没接受

options_surface 在剧本选择中的作用：
- `atm_iv_regime = extreme` + `skew_state = put_skewed` + `term_structure_state = front_rich` → 市场定价短期恐慌，增强"拥挤反转"剧本的可信度（恐慌见顶的概率在上升）
- `atm_iv_regime = compressed` → 波动率压缩，增强"延续"或"回归价值"剧本的可信度（市场处于低波蓄力状态，breakout更可能是真突破）
- `skew_state = call_skewed` + price在高位 → 做多拥挤的额外确认信号

options_surface 不改变剧本选择的核心逻辑（仍由 OI/ratio/funding/位置 决定），只作为辅助权重调节。

**第二：构建 scenario/path objects（工作流第2.5步）**

输出1-2条path，每条包含六个价格化字段：
- thesis：当前主剧本是什么，为什么是它
- activation_level：哪个价格带被确认后剧本激活
- first_path_target：激活后第一段目标
- next_path_target：第一段走出后下一层目标
- failure_level：哪个价格带失守则剧本失效
- failure_switch：失效后切换到哪个替代path

options_surface 在 path 构建中的作用：
- thesis 的 why 里可以引用 IV/skew 状态作为佐证（例如："put_skewed + IV elevated 确认下行恐慌已被定价"）
- options_surface 不作为 activation/target/failure 的锚点（这些仍由位置层的 PVS/TPO/RVWAP/EMA 决定）

**第三：驱动归因（工作流第3步）**

基于驱动层数据判断 spot-led / futures-led / mixed，以及当前驱动是否支撑剧本。

options_surface 不参与驱动归因（驱动归因只看 CVD/divergence/whale，不看期权）。

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

  "paths": [
    {
      "id": "path_a",
      "role": "primary",
      "thesis": "4H bearish continuation — long_unwind driving price through value lows, EMA regime bearish, ratio still crowded. IV elevated + put_skewed supports downside thesis",
      "activation_level": {
        "low": 2050,
        "high": 2055,
        "condition": "price accepts below this zone on 15m close"
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
        "condition": "price reclaims and holds above on 15m close with spot-led buying"
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
        "condition": "price reclaims this zone after a flush below 2035, with exhaustion + divergence confirmed"
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
        "condition": "price falls back below and accepts on 15m close"
      },
      "failure_switch": "path_a",
      "setup_type": "B_reversal",
      "entry_side": "LONG"
    }
  ],

  "script_switch_triggers": {
    "description": "Stage2 应在以下条件满足时切换path或请求Stage1刷新",
    "auto_switch_conditions": [
      "price hits path failure_level AND trigger layer confirms (exhaustion + divergence, or absorption)",
      "driver attribution flips (e.g. futures_led_selling → spot_led_buying)"
    ],
    "request_refresh_conditions": [
      "price moves beyond ALL defined path targets/failures (completely outside the map)",
      "a new 4H candle prints and current paths are > 4 hours old"
    ]
  }
}
```

### Stage1 输出设计原则

1. 所有价位都是具体数字，不是 "near support" 或 "at resistance"
2. 每条path的activation都带condition，不只是价格到了就算——还要求15m close确认或特定触发信号
3. failure_switch是预定义的，Stage2不需要自己想"失败了该怎么办"
4. setup_type直接绑定到工作流第4步的三种setup（A/B/C），Stage2知道该用哪套触发条件检查
5. script_switch_triggers告诉Stage2什么时候可以自己切换、什么时候需要请求刷新
6. options_surface 只出现在 thesis 的 why/佐证中，不作为 activation/target/failure 的价格锚点

---

## 第三层：Stage2（交易员 — 快思考，LLM）

### 调用频率
每个15分钟cycle。

### 职责
执行工作流第4步（触发确认）+ 第5步（执行）+ 第6步（管理，如有持仓）

### 输入

```json
{
  "task": "基于以下数据，执行工作流第4步+第5步，评估是否有可执行交易",
  "indicator_summary": { /* 代码层输出的最新JSON — 比stage1的数据更新 */ },
  "paths": { /* Stage1输出的path objects — 可能是几小时前建的，但有具体价位不会过时 */ },
  "active_positions": [],
  "account": { "balance": 5000, "max_leverage": 5 }
}
```

Stage2 拿到两份数据：
- 代码层**最新的** indicator_summary（本cycle的数据）
- Stage1 的 path objects（可能是几小时前建的，但价位是具体的，不会过时）

### Stage2 执行流程

**第一步：判断哪条path处于激活状态**

```
对每条path:
  1. activation_level 是否已被触发？（看 current_price vs activation 价位 + condition 是否满足）
  2. failure_level 是否已被触发？（如果是 → 执行 failure_switch）
  3. 如果都没触发 → 这条path还在等待中
```

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

Hard gate（必须全满足）：
- 位置够好（path的activation_level本身就保证了这一点）
- 触发已确认（上面的checklist）

Soft gate（满足3/4）：
- 状态清楚：OI/ratio/funding/VPIN/options_surface 综合判断当前是 build、unwind、crowding 还是恐慌见顶
- 驱动清楚：spot-led / futures-led / mixed
- 盘口真实：OBI/OFI同向 + spot_confirm + fake_order_risk低
- 失效点明确：path的failure_level已定义

options_surface 在 soft gate 中的作用：
- 增强"状态清楚"这一条的判断质量，不增加新的 gate 条件
- 例如：OI=long_unwind + ratio=crowded_long + IV=elevated + skew=put_skewed → 状态非常清楚，pass
- 例如：OI=long_unwind 但 IV=compressed + skew=neutral → 状态层信号有矛盾（仓位在减但期权市场没有定价恐慌），需要更谨慎

**第四步：如果通过，执行工作流第5步**

用触发层的1m/100ms数据（footprint intrabar_poc、OFI、microprice）优化入场点。

### 输出

无交易时：

```json
{
  "path_evaluation": {
    "path_a": {
      "status": "target_reached",
      "detail": "price已到first_path_target 2040-2042区域"
    },
    "path_b": {
      "status": "activation_pending",
      "detail": "等待price reclaim 2046-2051，当前2040.88未到activation"
    }
  },

  "script_switch": {
    "triggered": false,
    "request_stage1_refresh": false,
    "reason": null
  },

  "decision": "NO_TRADE",
  "reason": "Path A target已到达但没有新的continuation setup; Path B activation未触发（需站上2046-2051）"
}
```

有交易时：

```json
{
  "path_evaluation": {
    "path_b": {
      "status": "activated",
      "detail": "price reclaimed 2050.5, above activation 2046-2051; selling_exhaustion confirmed at 2042.09, bullish_divergence confirmed at 2033.22"
    }
  },

  "script_switch": {
    "triggered": true,
    "from": "path_a",
    "to": "path_b",
    "reason": "path_a failure_level未被击穿但target已exhausted; path_b activation conditions met with exhaustion+divergence"
  },

  "trigger_checklist": {
    "setup_type": "B_reversal",
    "absorption_or_exhaustion": { "pass": true, "detail": "selling_exhaustion at 2042.09, confirmed 18:25Z" },
    "valid_divergence": { "pass": true, "detail": "bullish divergence at 2033.22, z_score=2.4, spot_led=true" },
    "spot_not_continuing": { "pass": true, "detail": "spot 4h CVD slope no longer falling, spot_confirm=false for sell side" },
    "footprint_failure": { "pass": true, "detail": "stacked_buy_imbalances at 2039.5, unfinished_auction buy at 2041.0" }
  },

  "hard_gate": {
    "location": { "pass": true, "detail": "4h RVWAP -2σ zone + liquidation density peak" },
    "trigger_confirmed": { "pass": true, "detail": "4/4 reversal conditions met" }
  },

  "soft_gate": {
    "status_clear": { "pass": true, "detail": "long_unwind + crowded_long + IV elevated + put_skewed = downside panic priced in, squeeze potential" },
    "driver_clear": { "pass": true, "detail": "futures_led selling exhausting, spot divergence confirmed" },
    "orderbook_real": { "pass": false, "detail": "obi_fut=-0.27, exec_confirm=false, but bid_wall at 2033.8 holding" },
    "invalidation_clear": { "pass": true, "detail": "failure = below 2033 accept, path_b.failure_level defined" },
    "score": "3/4 pass"
  },

  "decision": "LONG",
  "plan": {
    "entry": 2046.5,
    "stop_loss": 2032.0,
    "take_profit_1": 2064.0,
    "take_profit_2": 2075.0,
    "leverage": 2,
    "horizon": "4h to 1d",
    "management_note": "如果驱动翻回futures_led_selling或price跌回2040下方，提前减仓"
  }
}
```

---

## i27 options_surface 融入总结

### 各层使用方式

| 层 | 如何使用 options_surface | 权重 |
|----|------------------------|------|
| 代码层 | 从 `feat.options_surface_feature` 提取 ATM IV / RR / skew / term structure，计算 regime 离散标签 | 纯数据提取 |
| Stage1 剧本选择 | 作为辅助权重：IV extreme + put_skewed 增强反转信心；IV compressed 增强延续/突破信心 | 辅助，不改变核心逻辑 |
| Stage1 path 构建 | 只出现在 thesis 的 why 佐证中，不作为 activation/target/failure 的价格锚点 | 仅佐证 |
| Stage1 驱动归因 | 不参与 | 无 |
| Stage2 触发检查 | 不参与（触发层由 footprint/orderbook/absorption/exhaustion 负责） | 无 |
| Stage2 soft gate | 增强"状态清楚"这一条的判断质量 | 辅助 |

### 设计原则

- options_surface 是状态层指标，定位与 funding/VPIN/OI/ratio 相同
- 它回答"期权市场如何定价当前风险"，不回答"该不该入场"
- 它不产生触发信号，不定义价格锚点，不改变 hard gate
- 它的核心价值是在特定场景（IV极端、skew急剧偏移、大到期日前后）提供额外的状态确认

---

## 与旧架构的对比

| 维度 | 旧架构 | 新架构 |
|------|-------|-------|
| 指标数量 | 26个 | 27个（+i27 options_surface） |
| 数据压缩 | Stage1 scan (LLM) | 代码层（无LLM） |
| Stage1频率 | 每15分钟 | 每4H或事件触发 |
| Stage1输出 | 抽象标签 (`sellers control`, `conflicted`) | 价格化的path objects |
| Stage2自由度 | 受标签约束，不敢覆盖scan | 只受path的activation/failure约束，有明确切换规则 |
| 剧本切换 | 没有机制 | 预定义在failure_switch里 + 切换触发条件 |
| 数据新鲜度 | Stage2的数据比scan晚7分钟 | Stage2直接拿代码层的实时数据 |
| LLM调用次数/小时 | 8次（scan+entry各4） | 5次（Stage1 1次 + Stage2 4次） |
| Stage2的任务复杂度 | 需要理解市场+做交易 | 只需对照path检查条件+做交易 |
| 期权数据 | 无 | 状态层增加 IV/skew/term structure |

---

## 用2026-03-27 ETH行情验证

### 旧系统实际表现
17:00-20:55 UTC，16次调用，全部NO_TRADE。价格从2066跌到2033又涨到2080，一笔未做。

### 新系统理论表现

| 时间(北京) | 价格 | 新系统应该做什么 |
|-----------|------|----------------|
| 01:00 | 2063 | Stage1建立Path A(空延续, activation=2050-2055) + Path B(reclaim反转, activation=2046-2051 after flush) |
| 01:45 | 2058→跌 | Stage2: Path A的activation(2055-2050)即将触发，准备 |
| 02:00 | 2045 | Stage2: Path A激活，触发条件检查通过 → 做空，target=2042/2033 |
| 02:15 | 2033 | Stage2: Path A的next_target到达；exhaustion+divergence触发 → 切换到Path B，等activation |
| 03:30 | 2050 | Stage2: Path B的activation(站上2046-2051)确认 + exhaustion + divergence → 做多，target=2064-2068 |
| 04:15 | 2070 | Stage2: Path B的first_target到达，看驱动是否支持继续持有到2072-2080 |

理论上支持做出至少两笔高质量交易（空头2055→2033 和 反转多头2050→2068），旧系统做了0笔。
