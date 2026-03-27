# LLM层工作流修改方案 v1.3.0

基于 /data/docs/订单流交易员交易流程V1.md 的工作流，将原有的 stage1 scan + stage2 entry 架构重构为三层架构。

## 版本变更记录

- v1.0.0：初版三层架构设计（代码层 + Stage1 + Stage2），基于26个指标
- v1.1.0：新增 i27 options_surface 指标融入，指标总数变为27个
- v1.2.0：修复 v1.1.0 复核中发现的7个问题
- v1.3.0：修复 v1.2.0 复核中发现的6个阻塞点

### v1.3.0 变更内容

1. **修复 invalid thesis 处理矛盾**：thesis_status=invalid 时严格禁止新开仓，删除越权示例；新增紧急刷新通道（Stage2 请求后 Stage1 在下一个 15m cycle 内响应，而非等 4H）
2. **触发事件绑定新鲜度和位置**：谓词库扩展 `max_age_minutes`、`near_level`、`since_activation`，确保确认信号发生在对的位置且足够新
3. **1m/100ms 执行帧从 LLM 剥离**：Stage2 输出 `execution_intent`（方向、区间、止损），精细入场由独立的确定性执行引擎完成，LLM 不碰 1m 数据
4. **支持 paths=[] 和 market_not_tradeable**：Stage1 允许输出空 paths，价格在 value 中间时显式拒绝编故事
5. **thesis_premises 扩展为可变长度**：Stage1 根据剧本类型声明 3-8 条前提，覆盖 funding、VPIN、位置漂移、value acceptance 等维度
6. **新增路径质量门槛**：path object 增加 `path_quality` 子结构，gate 之后执行路径质量检查（RR 比值、障碍密度、路径阻力）

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
- Path objects 会过时——不仅当价格出图时，也在 thesis_premises 被违反时
- **thesis_status=invalid 时 Stage2 严格不做新开仓**——这是系统安全边界，不允许例外
- **确认信号必须发生在对的位置且足够新**——脱离位置和时间的确认是噪音
- **1m/100ms 执行由确定性引擎完成**——LLM 负责"该不该做"和"做什么"，不负责"怎么进"
- **允许不做**——价格在 value 中间时 paths=[] 是合法输出，系统不强迫编故事
- **开仓前必须证明路径质量**——信号齐全不等于值得做，到目标的路径必须优于到止损的路径

### 第一性原理约束

高质量交易系统必须同时满足四件事：

1. **地图不过时**：thesis_premises 可变长度，覆盖位置+状态+驱动的完整前提；invalid 时严格不开仓并触发紧急刷新
2. **确认信号必须发生在对的位置且足够新**：谓词绑定 max_age_minutes + near_level + since_activation
3. **执行层有足够分辨率**：1m/100ms 执行由独立引擎完成，不受 LLM 响应延迟限制
4. **开仓前已证明路径质量**：path_quality（RR、障碍密度、路径阻力）作为最终开仓门槛

---

## 新架构：三层 + 执行引擎

```
indicator_engine (27个指标，含 i27 options_surface)
       │
       ▼
   代码层 (每15分钟，无LLM)
   输出: indicator_summary JSON
       │
       ├──────────────────────────┐
       ▼                          ▼
   Stage1 (每4H / 紧急刷新，LLM)  Stage2 (每15分钟，LLM)
   输入: indicator_summary         输入: indicator_summary (最新)
         + previous_paths                + stage1的paths
   输出: paths[]                         + stage1的thesis_premises
         + driver_attribution            + entry_snapshots (如有持仓)
         + thesis_premises         输出: decision
         + switch_triggers               + execution_intent (如开仓)
       │                                + path_evaluation
       └──► paths + premises ──►─┘       + thesis_validity_check
                                         + management_action (如有持仓)
                                               │
                                               ▼
                                         执行引擎 (实时，无LLM)
                                         输入: execution_intent
                                               + 1m/100ms数据流
                                         输出: 精确入场指令
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
- 事件带 `confirmed_at` 时间戳和 `confirmed_price`，让Stage2知道信号的新鲜度和发生位置

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
| `price_location.at_edge` | price在 VAH/VAL ±0.3%、sigma band ±0.2%、liq peak ±0.5%、IB ±0.2%、single print 内 → true | true, false |
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
      "vs_ema_regime": "below_both",
      "at_edge": true,
      "edge_type": "4h_rvwap_m1s_to_m2s"
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
        "confirmed_price": 2033.22,
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
        "confirmed_price": 2074.10,
        "spot_confirmed": true
      }
    ],
    "initiation_events": [],
    "exhaustion_events": [
      {
        "type": "selling",
        "price": 2042.09,
        "confirmed_at": "2026-03-26T18:25:00Z",
        "confirmed_price": 2042.09,
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

**v1.3.0 变更**：所有事件（divergence_events、absorption_events、exhaustion_events、initiation_events）统一增加 `confirmed_price` 字段，确保 Stage2 可以评估事件发生的位置。`price_location` 增加 `at_edge` 和 `edge_type` 布尔字段，代码层实时计算。

---

## 第二层：Stage1（建筑师 — 慢思考，LLM）

### 调用频率
- 常规：每个4H K线边界（00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC）
- **紧急刷新**：Stage2 标记 `request_stage1_refresh = true` 时，Stage1 在下一个 15m cycle 内响应（不等 4H 边界）

紧急刷新设计说明：当 thesis_status=invalid 时，Stage2 禁止开仓并请求刷新。如果必须等 4H 才能刷新，最坏情况下 Stage2 要空转近 4 小时。紧急刷新通道确保系统在 thesis 失效后最多 15 分钟内拿到新地图，而不是在"没有地图"的状态下无限等待或被迫越权。

### 职责
执行工作流第1步（地图）+ 第2步（剧本选择）+ 第2.5步（scenario/path object构建）+ 第3步（驱动归因）

### 输入

```json
{
  "task": "基于以下数据，执行工作流第1步+第2步+第2.5步+第3步",
  "indicator_summary": { /* 代码层输出的完整JSON */ },
  "previous_paths": { /* 上一次stage1输出的path objects，如果有的话 */ },
  "refresh_reason": "scheduled_4h | emergency_thesis_invalid | emergency_premises_violated",
  "recent_trade_history": {
    "last_trade": null,
    "account_balance": 5000,
    "active_positions": []
  }
}
```

### Stage1 要做的五件事

**第零：评估是否值得做（工作流第1步末尾判断）**

Stage1 在画完地图后，先判断价格当前是否在 value 中间、没有边缘可做。如果是，输出 `market_tradeable: false`，不构建 path。

判断标准：
- 价格在 4H 和 1D value area 的 POC ±30% range 内（即不在 VAH/VAL 边缘）
- 没有邻近的 RVWAP sigma band、liq density peak、IB 边缘、single print
- 没有极端的 ratio/funding/OI 状态（不存在潜在的 squeeze 或 unwind）
- EMA regime 不明确（between）

当满足以上全部条件时，market_tradeable = false。只要有一条不满足，market_tradeable = true，继续构建 paths。

**第一：选剧本（工作流第2步）**

基于位置层+状态层数据判断当前属于哪种剧本：
- 延续：OI同向扩张 + ratio不拥挤 + funding没反噬 + 价格顺着趋势结构
- 拥挤反转：极限位置 + ratio拥挤 + OI堆积但推进效率下降
- 回归价值：离开value但没接受

options_surface 在剧本选择中的作用：
- `atm_iv_regime = extreme` + `skew_state = put_skewed` + `term_structure_state = front_rich` → 市场定价短期恐慌，增强"拥挤反转"剧本的可信度
- `atm_iv_regime = compressed` → 波动率压缩，增强"延续"或"回归价值"剧本的可信度
- `skew_state = call_skewed` + price在高位 → 做多拥挤的额外确认信号

options_surface 不改变剧本选择的核心逻辑（仍由 OI/ratio/funding/位置 决定），只作为辅助权重调节。

**第二：构建 scenario/path objects（工作流第2.5步）**

输出0-2条path。每条包含七个价格化字段（v1.3.0 新增 path_quality）：
- thesis：当前主剧本是什么，为什么是它
- activation_level：哪个价格带被确认后剧本激活（结构化谓词，含新鲜度和位置约束）
- first_path_target：激活后第一段目标
- next_path_target：第一段走出后下一层目标
- failure_level：哪个价格带失守则剧本失效（结构化谓词）
- failure_switch：失效后切换到哪个替代path
- **path_quality**：从当前价到目标的路径质量评估

options_surface 在 path 构建中的作用：
- thesis 的 why 里可以引用 IV/skew 状态作为佐证
- options_surface 不作为 activation/target/failure 的价格锚点（这些仍由位置层的 PVS/TPO/RVWAP/EMA 决定）

**第三：驱动归因（工作流第3步）**

基于驱动层数据判断 spot-led / futures-led / mixed，以及当前驱动是否支撑剧本。

options_surface 不参与驱动归因（驱动归因只看 CVD/divergence/whale，不看期权）。

**第四：声明 thesis 成立的前提条件（thesis_premises）**

Stage1 必须根据当前剧本类型，声明 3-8 条前提条件。不同剧本类型有不同的关键前提维度：

| 剧本类型 | 必须覆盖的前提维度 | 说明 |
|---------|------------------|------|
| 延续 | OI regime、flow_driver、funding 方向、价格 vs value 位置、趋势结构 | 延续剧本依赖"趋势仍在"——OI 在扩张、驱动顺向、funding 没反噬、价格没回到 value 中间 |
| 拥挤反转 | ratio 极端度、funding 偏向、价格在极限位、VPIN 水平、OI 堆积方向 | 反转剧本依赖"拥挤仍在"——ratio 没回落、funding 没平衡、价格没从极限位回到中间 |
| 回归价值 | 价格仍在 value 外、未形成新 acceptance（TPO）、OI 未扩张、spot 未确认 | 回归剧本依赖"假突破仍成立"——没有被 value 重新接受、没有真正的 OI 扩张 |

前提条件的声明格式支持以下类型：

| 前提类型 | 格式 | 说明 |
|---------|------|------|
| 离散标签匹配 | `{"field": "xxx", "operator": "in", "expected": [...]}` | 标签必须属于指定集合 |
| 数值范围 | `{"field": "xxx", "operator": "gt"/"lt"/"between", "value": ...}` | 数值必须在指定范围内 |
| 位置条件 | `{"field": "price_location.xxx", "operator": "eq"/"neq", "expected": "..."}` | 价格位置状态匹配 |
| 复合条件 | `{"all": [...]}` 或 `{"any": [...]}` | 多条件组合 |

### 输出

#### 当 market_tradeable = false 时：

```json
{
  "meta": {
    "stage1_ts": "2026-03-26T16:05:00Z",
    "next_scheduled_refresh": "2026-03-26T20:00:00Z",
    "refresh_type": "scheduled_4h"
  },

  "market_tradeable": false,
  "not_tradeable_reason": "price at 2055, inside 4H VA (2062-2077) mid-range, no nearby sigma band / liq peak / IB edge; ratio balanced; EMA regime between — no edge",
  "recheck_conditions": [
    "price approaches VAL 2062 or VAH 2077",
    "ratio shifts to crowded_long or crowded_short",
    "VPIN spikes to extreme"
  ],

  "paths": [],
  "thesis_premises": null,
  "driver_attribution": null
}
```

Stage2 收到 `market_tradeable = false` 时，只做持仓管理（如有），不寻找新开仓机会。

#### 当 market_tradeable = true 时：

```json
{
  "meta": {
    "stage1_ts": "2026-03-26T16:05:00Z",
    "next_scheduled_refresh": "2026-03-26T20:00:00Z",
    "refresh_type": "scheduled_4h"
  },

  "market_tradeable": true,

  "script_selection": {
    "selected": "continuation_bearish",
    "why": "price below 4h/1d value, OI is unwinding (long_unwind), ratio still crowded_long, spot+futures CVD aligned selling, EMA regime below_both, funding still slightly_long (longs paying). options: IV elevated + put_skewed confirms downside fear is being priced, but not yet at extreme levels that would favor reversal"
  },

  "driver_attribution": {
    "flow_driver": "futures_led",
    "spot_confirming": false,
    "driver_quality": "price falling with futures-led selling but spot not fully confirming — move is aggressive but potentially fragile if futures exhaustion appears",
    "divergence_active": false
  },

  "thesis_premises": {
    "description": "当前剧本成立依赖以下前提。Stage2 逐条检查：任意 critical 前提违反 → invalid；2条以上 supporting 前提违反 → weakened",
    "premises": [
      {
        "id": "p1",
        "weight": "critical",
        "field": "open_interest.oi_label",
        "operator": "in",
        "expected": ["long_unwind", "fresh_short_build"],
        "violation": "OI regime翻转为leveraged_long_build或short_cover，说明仓位属性根本改变"
      },
      {
        "id": "p2",
        "weight": "supporting",
        "field": "long_short_ratios.regime",
        "operator": "in",
        "expected": ["crowded_long"],
        "violation": "ratio不再crowded，squeeze前提消失"
      },
      {
        "id": "p3",
        "weight": "critical",
        "field": "cvd_pack.flow_driver",
        "operator": "in",
        "expected": ["futures_led", "mixed"],
        "violation": "flow_driver翻转为spot_led buying，驱动属性根本改变"
      },
      {
        "id": "p4",
        "weight": "supporting",
        "field": "cvd_pack.spot_futures_relation",
        "operator": "in",
        "expected": ["aligned_selling", "divergent"],
        "violation": "spot和futures同时翻转为aligned_buying，bearish thesis不再成立"
      },
      {
        "id": "p5",
        "weight": "supporting",
        "field": "funding.bias",
        "operator": "in",
        "expected": ["slightly_long", "long_heavy", "neutral"],
        "violation": "funding翻转为short_heavy，空方在付钱——bearish拥挤而非bullish拥挤"
      },
      {
        "id": "p6",
        "weight": "supporting",
        "field": "price_location.vs_4h_value",
        "operator": "in",
        "expected": ["below"],
        "violation": "价格回到value内部，bearish continuation的位置前提不再成立"
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
      "entry_side": "SHORT",
      "path_quality": {
        "rr_ratio": 1.8,
        "distance_to_target_pct": 0.6,
        "distance_to_stop_pct": 0.9,
        "obstacles_to_target": [],
        "obstacles_to_stop": ["AVWAP session_open 2066.18", "4h VAL 2062.66"],
        "path_resistance": "low",
        "quality_note": "到target路径经过LVN 2055，低阻力；到stop需要重新站上VAL和AVWAP，阻力高。路径质量有利于做空"
      }
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
                {
                  "event_type": "exhaustion",
                  "subtype": "selling",
                  "max_age_minutes": 60,
                  "near_level": { "reference": 2035, "max_distance_pct": 0.5 }
                },
                {
                  "event_type": "divergence",
                  "subtype": "bullish",
                  "max_age_minutes": 120,
                  "near_level": { "reference": 2035, "max_distance_pct": 1.0 }
                }
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
      "entry_side": "LONG",
      "path_quality": {
        "rr_ratio": 2.1,
        "distance_to_target_pct": 1.1,
        "distance_to_stop_pct": 0.8,
        "obstacles_to_target": ["bearish FVG 2050-2058", "AVWAP session_open 2066.18"],
        "obstacles_to_stop": [],
        "path_resistance": "medium",
        "quality_note": "到target需穿越bearish FVG和AVWAP，阻力中等；但如果flush后reclaim，这些阻力位可能已被invalidated。到stop路径无阻力（自由落体）。RR=2.1可接受但需结合FVG穿越确认"
      }
    }
  ],

  "script_switch_triggers": {
    "description": "Stage2 应在以下条件满足时请求Stage1刷新",
    "request_refresh_conditions": [
      "任意 critical premise 不再成立 → thesis_status=invalid → 紧急刷新",
      "2条以上 supporting premises 不成立 → thesis_status=weakened → 常规刷新请求",
      "price moves beyond ALL defined path targets/failures (completely outside the map)",
      "当前 paths 已超过 4 小时且新的 4H K线已形成"
    ]
  }
}
```

### Stage1 输出设计原则

1. 所有价位都是具体数字，不是 "near support" 或 "at resistance"
2. activation/failure 的 condition 使用结构化谓词（见下方谓词类型说明），不使用自然语言
3. failure_switch 是预定义的，Stage2不需要自己想"失败了该怎么办"
4. setup_type 直接绑定到工作流第4步的三种setup（A/B/C），Stage2知道该用哪套触发条件检查
5. thesis_premises 显式声明剧本前提，按 critical/supporting 分权重，让Stage2可以结构化地检查前提是否仍然成立
6. options_surface 只出现在 thesis 的 why/佐证中，不作为 activation/target/failure 的价格锚点
7. **market_tradeable = false 是合法输出**——不强迫在没有 edge 的行情里编 path
8. **trigger_events 必须带 max_age_minutes 和 near_level**——确保确认信号在对的位置且足够新
9. **path_quality 必须为每条 path 评估路径质量**——包括 RR、障碍密度、路径阻力

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
| `trigger_events[].max_age_minutes` | 事件的 confirmed_at 距当前时间不超过 N 分钟 | 整数 |
| `trigger_events[].near_level` | 事件的 confirmed_price 距指定价格不超过 max_distance_pct | `reference`(价格), `max_distance_pct`(百分比) |

**v1.3.0 新增的谓词约束说明**：

- `max_age_minutes`：解决"历史确认当当前触发"的问题。一个 2 小时前的 selling exhaustion 不应该触发当前的反转入场。具体数值由 Stage1 根据当前波动率和 timeframe 决定，不是固定常量。
- `near_level`：解决"远处的确认当近处的触发"的问题。一个发生在 2055 的 exhaustion 不应该触发 2035 附近的反转。`max_distance_pct` 表示 confirmed_price 距 reference 的最大允许偏离百分比。
- 两个约束是 AND 关系：事件必须**既在时间窗口内，又在位置窗口内**才算有效。

Stage2评估这些谓词时，只需要对照 indicator_summary 里的数值做比较，不需要"理解"自然语言。

---

## 第三层：Stage2（交易员 — 有限自主，LLM）

### 定位

Stage2 不是纯 checklist 执行器，而是**有限自主的交易员**：
- 它对照 path 的结构化条件检查 activation/failure — 这部分是确定性的
- 它评估 path thesis 是否仍然成立（thesis validity check）— 这部分需要LLM判断
- 它在满足剧本切换规则（工作流V1 L43-49）时，有权主动请求Stage1刷新
- 它**不会**自己重新设计path或选择新剧本 — 这是Stage1的职责
- **thesis_status=invalid 时严格不做新开仓** — 这是系统安全边界，Stage2 无权越过

### 调用频率
每个15分钟cycle。

### 职责
- 评估 thesis 有效性（thesis validity check）
- 执行工作流第4步（触发确认）
- 输出 execution_intent（如决定开仓），由执行引擎完成工作流第5步
- 执行工作流第6步（持仓管理，如有持仓）

### 输入

```json
{
  "task": "执行thesis validity check + 工作流第4步 + 输出execution_intent + 工作流第6步",
  "indicator_summary": { /* 代码层输出的最新JSON */ },
  "market_tradeable": true,
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

1. 检查 `market_tradeable`：如果为 false，跳过所有开仓逻辑，只做持仓管理
2. 逐条检查 `thesis_premises`：对比 indicator_summary 中的实际值与 premise 的 expected 值
3. 评估整体thesis健康度：
   - `valid`：所有前提仍然成立
   - `weakened`：1条 supporting 前提不成立，但所有 critical 前提仍成立
   - `invalid`：任意 critical 前提不成立，或 2条以上 supporting 前提不成立

```json
"thesis_validity_check": {
  "premises_check": [
    { "id": "p1", "weight": "critical", "field": "open_interest.oi_label", "expected": ["long_unwind","fresh_short_build"], "actual": "short_cover", "pass": false },
    { "id": "p2", "weight": "supporting", "field": "long_short_ratios.regime", "expected": ["crowded_long"], "actual": "balanced", "pass": false },
    { "id": "p3", "weight": "critical", "field": "cvd_pack.flow_driver", "expected": ["futures_led","mixed"], "actual": "spot_led", "pass": false },
    { "id": "p4", "weight": "supporting", "field": "cvd_pack.spot_futures_relation", "expected": ["aligned_selling","divergent"], "actual": "aligned_buying", "pass": false },
    { "id": "p5", "weight": "supporting", "field": "funding.bias", "expected": ["slightly_long","long_heavy","neutral"], "actual": "slightly_short", "pass": false },
    { "id": "p6", "weight": "supporting", "field": "price_location.vs_4h_value", "expected": ["below"], "actual": "inside", "pass": false }
  ],
  "thesis_status": "invalid",
  "request_stage1_refresh": true,
  "refresh_urgency": "emergency",
  "reason": "critical premise p1(oi_label) violated: short_cover; critical premise p3(flow_driver) violated: spot_led; plus 4 supporting premises failed — bearish continuation thesis完全失效"
}
```

**thesis_status 处理规则（严格执行，无例外）：**

| thesis_status | 新开仓 | 持仓管理 | Stage1 刷新 |
|---------------|--------|---------|------------|
| valid | 允许（通过后续 gate） | 正常执行 | 等下一个 4H |
| weakened | 允许但降低仓位（leverage 减半或不超过 1x） | 正常执行 | 常规请求刷新 |
| invalid | **禁止** | 正常执行 | **紧急刷新**（下一个 15m cycle） |

**为什么 invalid 时不允许按旧 path 开仓**：旧地图的 thesis 已被证伪，它的所有 path（包括 alternate path）都是基于那个已证伪的 thesis 画的。用一张作废地图的备选路径开仓，等于没有地图。如果市场确实在快速变化中提供了机会，紧急刷新通道保证 Stage1 在 15 分钟内给出新地图——这是正确的获取新地图的方式，而不是越权使用作废地图。

**第一步：判断哪条path处于激活状态**

```
对每条path:
  1. 评估 activation_level 的结构化谓词是否为true
     - 如果有 trigger_events 要求，检查 max_age_minutes 和 near_level：
       a. confirmed_at 距 meta.ts 不超过 max_age_minutes
       b. confirmed_price 距 near_level.reference 的偏离不超过 max_distance_pct
       c. 两个条件必须同时满足，事件才算有效
  2. 评估 failure_level 的结构化谓词是否为true
  3. 如果failure为true → 执行failure_switch
  4. 如果activation为true → 进入触发检查
  5. 如果都不满足 → path仍在等待中
```

注意：path之间的切换可以在failure_level未被击穿时发生——当alternate path的activation条件被满足时（包括其precondition和trigger_events的freshness/location检查全部通过），Stage2可以判定"primary path已经跑完了它的路径，alternate path的激活条件已满足"，这是合法的切换，不需要primary path先hit failure。**但前提是 thesis_status 不是 invalid**——如果 thesis 已经失效，任何基于该 thesis 的 path 切换都不允许。

**第二步：如果有path激活，执行工作流第4步的触发检查**

根据path的 `setup_type` 选择对应的触发条件集：

如果是 `A_continuation`（延续单），从 indicator_summary 的触发层检查：
- initiation 事件？（必须 max_age ≤ 当前 15m bar 内，即 confirmed_at 在当前 bar 范围内）
- footprint stacked imbalance？
- OBI / OFI / microprice 同向？
- spot_confirm = true？
- fake_order_risk 低？
- OI 状态仍支持？

如果是 `B_reversal`（反转单），从 indicator_summary 的触发层检查：
- absorption 或 exhaustion 已确认？（必须满足 path 里声明的 max_age_minutes 和 near_level）
- 有效 divergence？（同上）
- spot 没有继续推同方向？
- footprint 里出现失衡失败 / 推不动价格？

如果是 `C_value_return`（回归价值单），从 indicator_summary 的触发层检查：
- 先突破了 VAH/VAL/IB 外沿？
- 但没有 OI 扩张、spot 确认、持续 OFI？
- 已经收回 value 内？

**第三步：过硬过滤器**

Hard gate（必须全部满足）：
- **位置仍在边缘**：`price_location.at_edge = true`（代码层实时计算），不是从activation后已经回到了区间中间
- **触发已确认**：上面的checklist通过，且所有事件满足 max_age 和 near_level 约束
- **失效点明确**：path的failure_level + stop_loss已定义且是可执行的价位

Soft gate（满足2/3）：
- 状态清楚：OI/ratio/funding/VPIN/options_surface 综合判断当前是 build、unwind、crowding 还是恐慌见顶
- 驱动清楚：spot-led / futures-led / mixed，且驱动方向与交易方向一致或至少不矛盾
- 盘口真实：OBI/OFI 与成交同向，spot_confirm 在，fake_order_risk 不高

**第四步：路径质量检查（v1.3.0 新增）**

Gate 通过后，在生成 execution_intent 之前，Stage2 必须评估路径质量：

1. **RR 比值**：path_quality.rr_ratio ≥ 1.5 才允许开仓。如果 1.0 ≤ rr_ratio < 1.5 且其他条件极好，允许但必须降低仓位
2. **障碍密度**：obstacles_to_target 中的关键级别（HVN/POC/AVWAP/RVWAP中枢）超过 2 个 → 路径质量降级
3. **路径阻力综合判断**：
   - `path_resistance = low` + `rr_ratio ≥ 1.5` → 正常仓位
   - `path_resistance = medium` + `rr_ratio ≥ 2.0` → 正常仓位（高 RR 补偿中等阻力）
   - `path_resistance = medium` + `1.5 ≤ rr_ratio < 2.0` → 减半仓位
   - `path_resistance = high` → 不做，除非 rr_ratio ≥ 3.0

注意：path_quality 是 Stage1 在画地图时评估的，但 Stage2 应结合当前 indicator_summary 验证是否仍然准确（比如 FVG 可能已被 fill，obstacle 可能已被突破）。如果当前数据显示某个 obstacle 已不存在，Stage2 可以将其从 obstacles 中移除并重新评估 path_resistance。

**第五步：如果通过，输出 execution_intent**

Stage2 不直接决定具体入场价格。它输出一个 `execution_intent`，由执行引擎完成工作流第5步。

**第六步：如有持仓，执行工作流第6步管理**

持仓管理在trigger check之前独立执行，不受新开仓逻辑影响。**即使 thesis_status=invalid，持仓管理仍然正常执行**。

管理流程：
1. 对比 entry_snapshot vs 当前 indicator_summary，检查 management_rules 中的每条规则
2. 如果任何 `exit_full` 条件满足 → 输出 exit 指令
3. 如果任何 `reduce_50pct` 条件满足 → 输出 reduce 指令
4. 如果都不满足 → hold，可选附加判断："驱动是否在减弱但尚未翻转"

### 输出

#### 无持仓、market_tradeable=false 或 thesis_status=invalid 时：

```json
{
  "thesis_validity_check": {
    "premises_check": [
      { "id": "p1", "weight": "critical", "actual": "short_cover", "pass": false },
      { "id": "p2", "weight": "supporting", "actual": "balanced", "pass": false },
      { "id": "p3", "weight": "critical", "actual": "spot_led", "pass": false },
      { "id": "p4", "weight": "supporting", "actual": "aligned_buying", "pass": false },
      { "id": "p5", "weight": "supporting", "actual": "slightly_short", "pass": false },
      { "id": "p6", "weight": "supporting", "actual": "inside", "pass": false }
    ],
    "thesis_status": "invalid",
    "request_stage1_refresh": true,
    "refresh_urgency": "emergency"
  },

  "decision": "NO_TRADE",
  "reason": "thesis_status=invalid — critical premises p1(oi_label) and p3(flow_driver) violated. 紧急刷新已请求，等待Stage1在下一个15m cycle内提供新paths。不使用作废地图的任何path开仓。"
}
```

#### 无持仓、有交易时：

```json
{
  "thesis_validity_check": {
    "premises_check": [
      { "id": "p1", "weight": "critical", "actual": "long_unwind", "pass": true },
      { "id": "p2", "weight": "supporting", "actual": "crowded_long", "pass": true },
      { "id": "p3", "weight": "critical", "actual": "mixed", "pass": true },
      { "id": "p4", "weight": "supporting", "actual": "divergent", "pass": true },
      { "id": "p5", "weight": "supporting", "actual": "slightly_long", "pass": true },
      { "id": "p6", "weight": "supporting", "actual": "below", "pass": true }
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
        "trigger_events_check": [
          {
            "event": "selling_exhaustion",
            "confirmed_at": "2026-03-26T18:25:00Z",
            "confirmed_price": 2042.09,
            "max_age_minutes": 60,
            "age_minutes": 5,
            "age_ok": true,
            "near_level_reference": 2035,
            "distance_pct": 0.20,
            "max_distance_pct": 0.5,
            "location_ok": true,
            "valid": true
          }
        ]
      }
    }
  },

  "trigger_checklist": {
    "setup_type": "B_reversal",
    "absorption_or_exhaustion": { "pass": true, "detail": "selling_exhaustion at 2042.09, confirmed 18:25Z, age=5min (≤60min), distance=0.20% from 2035 (≤0.5%)" },
    "valid_divergence": { "pass": true, "detail": "bullish divergence at 2033.22, confirmed 18:20Z, age=10min (≤120min), distance=0.06% from 2035 (≤1.0%), z_score=2.4, spot_led=true" },
    "spot_not_continuing": { "pass": true, "detail": "spot 4h CVD slope no longer falling" },
    "footprint_failure": { "pass": true, "detail": "stacked_buy_imbalances at 2039.5, unfinished_auction buy at 2041.0" }
  },

  "hard_gate": {
    "location_at_edge": { "pass": true, "detail": "price_location.at_edge=true, edge_type=4h_rvwap_m1s_to_m2s" },
    "trigger_confirmed": { "pass": true, "detail": "4/4 reversal checklist conditions met, all events within age and location bounds" },
    "invalidation_defined": { "pass": true, "detail": "stop_loss=2032.0 (path_b failure_level), clearly executable" }
  },

  "soft_gate": {
    "status_clear": { "pass": true, "detail": "long_unwind + crowded_long + IV elevated + put_skewed = panic priced in, squeeze potential" },
    "driver_clear": { "pass": true, "detail": "futures_led selling exhausting, spot divergence confirmed, flow turning mixed" },
    "orderbook_real": { "pass": false, "detail": "obi_fut=-0.27, exec_confirm=false; bid_wall at 2033.8 holds but orderbook not confirming buy side" },
    "score": "2/3 pass"
  },

  "path_quality_check": {
    "path_id": "path_b",
    "rr_ratio": 2.1,
    "rr_ok": true,
    "obstacles_to_target": ["bearish FVG 2050-2058", "AVWAP session_open 2066.18"],
    "obstacles_current_validity": [
      { "obstacle": "bearish FVG 2050-2058", "still_valid": true, "note": "FVG未被fill" },
      { "obstacle": "AVWAP session_open 2066.18", "still_valid": true, "note": "AVWAP仍在上方" }
    ],
    "effective_obstacles": 2,
    "path_resistance": "medium",
    "sizing_adjustment": "rr_ratio=2.1 ≥ 2.0 且 path_resistance=medium → 正常仓位",
    "pass": true
  },

  "decision": "LONG",
  "execution_intent": {
    "side": "LONG",
    "entry_zone": { "low": 2039.0, "high": 2046.5 },
    "stop_loss": 2032.0,
    "take_profit_1": 2064.0,
    "take_profit_2": 2075.0,
    "leverage": 2,
    "sizing_note": "正常仓位（path_quality check通过，rr=2.1, resistance=medium）",
    "path_id": "path_b",
    "entry_snapshot": {
      "ts": "2026-03-26T18:30:00Z",
      "flow_driver": "mixed",
      "spot_futures_relation": "divergent",
      "oi_label": "long_unwind",
      "ratio_regime": "crowded_long",
      "funding_bias": "slightly_long",
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

## 第四层：执行引擎（实时，无LLM）

### 设计原因

工作流第5步要求用 1m/100ms 数据优化入场（footprint intrabar_poc、OFI、microprice）。但 LLM 的 15min 调用频率和响应延迟（秒级）天然不适合做 1m/100ms 级别的执行优化——等 LLM 返回时 1m 数据已经过时。

从第一性原理看：**"该不该做"是判断题（LLM擅长），"怎么进"是速度题（代码擅长）**。将两者分离，各用最适合的工具。

### 输入

```json
{
  "execution_intent": {
    "side": "LONG",
    "entry_zone": { "low": 2039.0, "high": 2046.5 },
    "stop_loss": 2032.0,
    "take_profit_1": 2064.0,
    "take_profit_2": 2075.0,
    "leverage": 2,
    "path_id": "path_b"
  },
  "realtime_data": {
    "1m_footprint": { /* 实时1m footprint数据流 */ },
    "100ms_orderbook": { /* 实时100ms盘口数据流 */ },
    "ofi_realtime": 0.0,
    "microprice_realtime": 0.0
  }
}
```

### 执行规则（确定性，无LLM）

1. **入场价优化**：在 entry_zone 范围内，用 intrabar_poc 作为 limit price 参考
2. **临门确认**：OFI 和 microprice 与 side 同向时才下单；如果反向，等待下一个 1m bar 重新检查
3. **下单方式**：优先 limit order at intrabar_poc，如果 N 分钟（可配置）内未成交，转 market
4. **止损/止盈**：下单后立即挂 stop_loss 和 take_profit orders
5. **超时取消**：如果 entry_zone 已不再包含当前价格（价格已远离），取消执行意图

### 输出

```json
{
  "execution_result": {
    "status": "filled",
    "fill_price": 2041.2,
    "fill_ts": "2026-03-26T18:31:05Z",
    "method": "limit_at_intrabar_poc",
    "stop_loss_order": "placed_at_2032.0",
    "take_profit_order": "placed_at_2064.0"
  }
}
```

### 执行引擎不做的事

- 不判断方向（由 Stage2 决定）
- 不评估是否值得做（由 Stage2 的 gate + path_quality 决定）
- 不修改 stop_loss / take_profit（由 Stage2 管理层决定）
- 不持有状态超过单次执行（执行完成后清空 intent）

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
| Stage2 path quality check | 不参与（路径质量由位置层结构决定） | 无 |
| Stage2 management | 不作为 reduce/exit 的规则条件 | 无 |
| 执行引擎 | 不参与 | 无 |

---

## 与旧架构的对比

| 维度 | 旧架构 | v1.3.0 |
|------|-------|--------|
| 指标数量 | 26个 | 27个（+i27 options_surface） |
| 数据压缩 | Stage1 scan (LLM) | 代码层（无LLM，输出原始数值 + 确定性映射标签） |
| Stage1频率 | 每15分钟 | 每4H或紧急刷新（thesis invalid后 ≤15min） |
| Stage1输出 | 抽象标签 | market_tradeable + 价格化path objects + 结构化谓词 + 可变长度thesis_premises + path_quality |
| Stage2定位 | 服从scan标签的执行器 | 有限自主的交易员（可评估thesis有效性，可请求刷新，但invalid时严格不开仓） |
| Path过时检测 | 无 | thesis_premises逐条检查（critical/supporting分权重）+ 价格出图 + 时间超限 |
| 剧本切换 | 无机制 | failure_switch预定义 + thesis validity驱动的紧急刷新 |
| 不做交易 | 不支持（总得给个方向） | market_tradeable=false + paths=[] 是合法输出 |
| Condition格式 | 自然语言 | 结构化谓词 + max_age_minutes + near_level |
| 数据新鲜度 | Stage2比scan晚7分钟 | Stage2直接拿代码层实时数据 |
| 持仓管理 | 无闭环 | entry_snapshot + management_rules + driver对比 |
| Gate设计 | hard 2条 + soft 4选3 | hard 3条（含实时位置检查）+ soft 3选2 + path_quality检查 |
| 路径质量 | 无 | RR比值 + 障碍密度 + 路径阻力 → 仓位调节或拒绝 |
| 执行层 | LLM说"怎么进" | 执行引擎（代码，实时1m/100ms数据，确定性规则） |
| LLM调用次数/小时 | 8次 | 5次（Stage1 1次 + Stage2 4次），执行引擎无LLM |
| 期权数据 | 无 | 状态层增加 IV/skew/term structure |

---

## 用2026-03-27 ETH行情验证

### 旧系统实际表现
17:00-20:55 UTC，16次调用，全部NO_TRADE。价格从2066跌到2033又涨到2080，一笔未做。

### v1.3.0 理论表现

| 时间(北京) | 价格 | 系统行为 |
|-----------|------|---------|
| 01:00 | 2063 | Stage1建立Path A(空延续) + Path B(reclaim反转) + thesis_premises(6条，含funding和位置前提) + path_quality(A: low resistance RR=1.8; B: medium resistance RR=2.1); market_tradeable=true |
| 01:45 | 2058→跌 | Stage2: thesis valid(6/6 pass), Path A activation谓词检查(15m close < 2050)尚未触发 |
| 02:00 | 2045 | Stage2: thesis valid, Path A activated(15m close < 2050), trigger check通过, hard gate通过(at_edge=true), path_quality通过(RR=1.8, resistance=low) → 输出execution_intent做空; 执行引擎: OFI同向, limit at intrabar_poc |
| 02:15 | 2033 | Stage2: Path A next_target到达; management check: driver仍aligned_selling → hold; 同时检测到selling_exhaustion at 2042(age=0min, near 2035=0.20%) → Path B precondition met |
| 02:30 | 2040 | Stage2: 空单管理 — 接近target, 考虑止盈; thesis_premises p6(位置)开始变化 |
| 03:00 | 2043 | Stage2: p1(oi_label)变为short_cover → **critical premise violated** → thesis_status=invalid → **紧急刷新请求**; 空单管理继续; **不做任何新开仓** |
| 03:15 | 2047 | Stage1紧急刷新响应: 基于当前状态(short_cover, spot_led, ratio rebalancing)重新评估 → 新thesis: reclaim reversal, 新paths(含新的activation/failure/path_quality), 新premises(6条, 适配反转剧本) |
| 03:30 | 2050 | Stage2: 新thesis valid, 新Path activation谓词检查 — selling_exhaustion at 2042(age=65min ≤ 新path的max_age_minutes=90, near 2035 distance=0.20% ≤ 1.0%) → activated, trigger check通过, hard gate通过(at_edge=true, 4h RVWAP -1σ zone), path_quality通过(新评估RR=2.3, resistance=low after FVG partially filled) → 输出execution_intent做多 |
| 04:00 | 2065 | Stage2: 多单management check — driver improved(spot_led aligned_buying), hold |
| 04:15 | 2070 | Stage2: first_target 2064-2068到达, management评估driver是否支持hold到next_target |

### 与v1.2.0的关键差异

| 时间点 | v1.2.0 行为 | v1.3.0 行为 | 差异原因 |
|--------|-----------|-----------|---------|
| 03:00 | thesis_status=weakened, 还在等 | thesis_status=invalid(critical p1 violated), 紧急刷新请求 | v1.3.0 区分 critical/supporting，OI翻转是 critical |
| 03:15 | — | Stage1紧急刷新，15min内响应 | v1.3.0 新增紧急刷新通道 |
| 03:30 | 按旧map的Path B开仓（越权） | 按新map的新Path开仓（合规） | v1.3.0 严禁invalid时用旧map，紧急刷新提供新map |
| 03:30 | exhaustion无age/location检查 | exhaustion age=65min ≤ 90min, distance=0.20% ≤ 1.0% → 有效 | v1.3.0 谓词绑定新鲜度+位置 |
| 02:00 | 无路径质量检查 | path_quality: RR=1.8, resistance=low → 正常仓位 | v1.3.0 新增路径质量门槛 |

### 验证局限性说明

以上验证基于单日单品种的理论回放，用于说明设计逻辑的可行性。它不能证明"稳定高质量"。完整验证需要：
1. 对多个不同行情类型（趋势延续、V型反转、区间震荡、假突破、无边缘震荡）做理论回放
2. 实现后在shadow mode下跑真实行情，对比新旧系统的决策差异
3. 建立评估指标体系（不仅是PnL，还包括：decision quality score、thesis accuracy、switch latency、path_quality accuracy、execution_engine fill quality等）
4. 特别验证：market_tradeable=false 的判定准确率（避免误判导致错过机会，也避免漏判导致强行交易）
