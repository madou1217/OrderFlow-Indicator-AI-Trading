# Scan Prompt & Schema 修改方案 v1.8

基于 2026-03-24T08:45 ETHUSDT 实际 scan 结果的质量评估，提出提示词和 JSON schema 两部分修改。

---

## 第一部分：v1.7 scan 质量问题定位

### 关键问题

| # | 问题 | 详情 | 根因 |
|---|------|------|------|
| 1 | **active_range 机械复制 path range** | 15m=2130.67-2169.9 (1.84%)、4h=2021.58-2199.0 (8.64%)、1d=2021.58-2385.78 (17.42%) 全部照抄 `path_newest_to_oldest` 的 `range_low`/`range_high` | 提示词没有定义 active_range 的来源约束 |
| 2 | **Spot-Futures 负基差被忽视** | spot=2166.9 vs futures=2159.35 (+0.35%)，`spot_flow_dominance`=0.0165，4h delta_spot=-575 vs delta_fut=+55882 | 提示词没有要求 cross-market 检查 |
| 3 | **control_clarity 三个 timeframe 全部 mixed** | 1d 有 +4.78% closed bar、CVD slope 204k、close location 72.95%，应至少为 strong | 提示词没有要求差异化 |
| 4 | **15m invalidation_level = null** | balanced 也应有结构性 invalidation（如 2154.89 RVWAP sigma2） | schema 允许 null，提示词无约束 |
| 5 | **range_width_vs_atr 全部 wide** | 因 active_range 偏大导致的连锁失真 | 问题 1 的下游 |
| 6 | **supporting_context 指标未引用** | funding_rate、liquidation_density、VPIN 均未出现在 validation 中 | 提示词没有显式要求引用 |

---

## 第二部分：JSON Schema 变更（scan_v1_7 → scan_v1_8）

### 变更清单

| 位置 | v1.7 | v1.8 | 原因 |
|------|------|------|------|
| `schema_version` | `"const": "scan_v1_7"` | `"const": "scan_v1_8"` | 版本升级 |
| `structure_map.invalidation_level` | `"type": ["number", "null"]` | `"type": "number"` | 禁止 null，强制给出具体价格 |
| `cross_timeframe_map` 新增字段 | 无 | `cross_market_snapshot` | 必须输出 spot/futures 对比 |

### 完整 schema（仅标注 diff，其余与 v1.7 一致）

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "stage1_scan_v1_8",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "schema_version",
    "meta",
    "timeframes",
    "cross_timeframe_map"
  ],
  "properties": {
    "schema_version": {
      "type": "string",
      "const": "scan_v1_8"
    },
    "meta": {
      "type": "object",
      "additionalProperties": false,
      "required": ["symbol", "scan_ts_bucket"],
      "properties": {
        "symbol": { "type": "string" },
        "scan_ts_bucket": { "type": "string", "format": "date-time" }
      }
    },
    "timeframes": {
      "type": "object",
      "additionalProperties": false,
      "required": ["15m", "4h", "1d"],
      "properties": {
        "15m": { "$ref": "#/$defs/timeframe_scan" },
        "4h":  { "$ref": "#/$defs/timeframe_scan" },
        "1d":  { "$ref": "#/$defs/timeframe_scan" }
      }
    },
    "cross_timeframe_map": { "$ref": "#/$defs/cross_timeframe_map" }
  },
  "$defs": {
    "price_zone": {
      "type": "object",
      "additionalProperties": false,
      "required": ["low", "high", "reason"],
      "properties": {
        "low":    { "type": "number" },
        "high":   { "type": "number" },
        "reason": { "type": "string" }
      }
    },
    "key_level": {
      "type": "object",
      "additionalProperties": false,
      "required": ["price", "type", "reason"],
      "properties": {
        "price": { "type": "number" },
        "type": {
          "type": "string",
          "enum": ["support", "resistance", "pivot", "value_edge", "liquidity_wall", "imbalance_edge"]
        },
        "reason": { "type": "string" }
      }
    },
    "level_reference": {
      "type": "object",
      "additionalProperties": false,
      "required": ["ref_kind", "price", "label"],
      "properties": {
        "ref_kind": {
          "type": "string",
          "enum": ["key_level", "demand_zone_edge", "supply_zone_edge", "active_range_low", "active_range_high", "external_price", "none"]
        },
        "price": { "type": ["number", "null"] },
        "label": {
          "type": "string",
          "description": "Short anchor label only, for example '4h resistance' or '1d demand high'."
        }
      }
    },
    "path_side": {
      "type": "object",
      "additionalProperties": false,
      "required": ["first_objective_ref", "first_barrier_ref"],
      "properties": {
        "first_objective_ref": { "$ref": "#/$defs/level_reference" },
        "first_barrier_ref":   { "$ref": "#/$defs/level_reference" }
      }
    },
    "role_observation": {
      "type": "object",
      "additionalProperties": false,
      "required": ["role", "observed_behavior", "evidence", "confidence"],
      "properties": {
        "role": {
          "type": "string",
          "enum": ["large_directional_flow", "higher_timeframe_sponsorship", "crowd_behavior", "passive_liquidity"]
        },
        "observed_behavior": {
          "type": "string",
          "description": "Observed market behavior only. Do not invent participant identity or narrative intent."
        },
        "evidence": {
          "type": "array",
          "maxItems": 3,
          "items": { "type": "string" }
        },
        "confidence": {
          "type": "string",
          "enum": ["high", "medium", "low"]
        }
      }
    },
    "state_block": {
      "type": "object",
      "additionalProperties": false,
      "required": ["control_side", "control_clarity", "value_location", "range_state", "sponsorship_state"],
      "properties": {
        "control_side": {
          "type": "string",
          "enum": ["buyers", "sellers", "balanced", "unclear"],
          "description": "Observed control on this timeframe. This is not a trade instruction."
        },
        "control_clarity": {
          "type": "string",
          "enum": ["strong", "mixed", "conflicted"]
        },
        "value_location": {
          "type": "string",
          "enum": ["above_value", "below_value", "inside_value", "accepted_above", "accepted_below", "rejected_from_above", "rejected_from_below"]
        },
        "range_state": {
          "type": "string",
          "enum": ["inside_range", "accepting_above_range", "accepting_below_range", "rejecting_above_range", "rejecting_below_range", "testing_range_high", "testing_range_low", "range_unresolved"],
          "description": "Range interaction fact only. Avoid pullback, continuation, or reversal interpretation here."
        },
        "sponsorship_state": {
          "type": "string",
          "enum": ["active", "fragile", "fading", "absent", "unresolved"]
        }
      }
    },
    "flow_map": {
      "type": "object",
      "additionalProperties": false,
      "required": ["aggressive_side", "absorption_side", "trapped_side", "role_observations"],
      "properties": {
        "aggressive_side": {
          "type": "string",
          "enum": ["buyers", "sellers", "balanced", "unclear"],
          "description": "Which side is currently acting aggressively on this timeframe."
        },
        "absorption_side": {
          "type": "string",
          "enum": ["buyers", "sellers", "none", "unclear"],
          "description": "Which side is visibly absorbing opposing flow on this timeframe."
        },
        "trapped_side": {
          "type": "string",
          "enum": ["buyers", "sellers", "none", "unclear"],
          "description": "Which side appears trapped or forced on this timeframe."
        },
        "role_observations": {
          "type": "array",
          "maxItems": 3,
          "items": { "$ref": "#/$defs/role_observation" }
        }
      }
    },
    "structure_map": {
      "type": "object",
      "additionalProperties": false,
      "required": ["active_range", "range_width_vs_atr", "dominant_demand_zone", "dominant_supply_zone", "key_levels", "invalidation_level", "path_map"],
      "properties": {
        "active_range": {
          "type": "object",
          "additionalProperties": false,
          "required": ["low", "high"],
          "properties": {
            "low":  { "type": "number" },
            "high": { "type": "number" }
          }
        },
        "range_width_vs_atr": {
          "type": "string",
          "enum": ["narrow", "normal", "wide"]
        },
        "dominant_demand_zone": {
          "anyOf": [{ "$ref": "#/$defs/price_zone" }, { "type": "null" }]
        },
        "dominant_supply_zone": {
          "anyOf": [{ "$ref": "#/$defs/price_zone" }, { "type": "null" }]
        },
        "key_levels": {
          "type": "array",
          "maxItems": 6,
          "items": { "$ref": "#/$defs/key_level" }
        },
        "invalidation_level": {
          "type": "number",
          "description": "Single numeric anchor where this timeframe read breaks. Must be a concrete price, never null. For balanced reads, use the nearest structural edge where the current equilibrium breaks."
        },
        "path_map": {
          "type": "object",
          "additionalProperties": false,
          "required": ["upside", "downside"],
          "properties": {
            "upside":   { "$ref": "#/$defs/path_side" },
            "downside": { "$ref": "#/$defs/path_side" }
          }
        }
      }
    },
    "validation_block": {
      "type": "object",
      "additionalProperties": false,
      "required": ["read_basis", "supporting_facts", "conflicting_facts", "recent_closed_bars_align_with_read", "cvd_slope_aligns_with_read", "current_partial_bar_aligns_with_read", "fragility_summary"],
      "properties": {
        "read_basis": {
          "type": "string",
          "enum": ["closed_bar_continuation", "live_flow_reversal", "exhaustion_inference", "structural_inference", "mixed"]
        },
        "supporting_facts": {
          "type": "array",
          "maxItems": 4,
          "items": { "type": "string" }
        },
        "conflicting_facts": {
          "type": "array",
          "maxItems": 4,
          "items": { "type": "string" }
        },
        "recent_closed_bars_align_with_read": { "type": "boolean" },
        "cvd_slope_aligns_with_read":         { "type": "boolean" },
        "current_partial_bar_aligns_with_read": { "type": "boolean" },
        "fragility_summary": {
          "type": "string",
          "description": "Short factual note about what makes this timeframe read vulnerable or unresolved."
        }
      }
    },
    "timeframe_scan": {
      "type": "object",
      "additionalProperties": false,
      "required": ["state", "flow_map", "structure_map", "validation"],
      "properties": {
        "state":         { "$ref": "#/$defs/state_block" },
        "flow_map":      { "$ref": "#/$defs/flow_map" },
        "structure_map": { "$ref": "#/$defs/structure_map" },
        "validation":    { "$ref": "#/$defs/validation_block" }
      }
    },
    "timeframe_relationship": {
      "type": "object",
      "additionalProperties": false,
      "required": ["control_relation", "lower_tf_value_location_vs_higher_tf", "lower_tf_range_location_vs_higher_tf"],
      "properties": {
        "control_relation": {
          "type": "string",
          "enum": ["aligned", "opposed", "neutral"],
          "description": "Relative control fact only. Do not encode pullback, reversal, continuation, or trade preference interpretation here."
        },
        "lower_tf_value_location_vs_higher_tf": {
          "type": "string",
          "enum": ["above_higher_tf_value", "inside_higher_tf_value", "below_higher_tf_value"]
        },
        "lower_tf_range_location_vs_higher_tf": {
          "type": "string",
          "enum": ["above_higher_tf_range", "inside_higher_tf_range", "below_higher_tf_range"]
        }
      }
    },
    "relationship_map": {
      "type": "object",
      "additionalProperties": false,
      "required": ["15m_vs_4h", "4h_vs_1d", "15m_vs_1d"],
      "properties": {
        "15m_vs_4h": { "$ref": "#/$defs/timeframe_relationship" },
        "4h_vs_1d":  { "$ref": "#/$defs/timeframe_relationship" },
        "15m_vs_1d": { "$ref": "#/$defs/timeframe_relationship" }
      }
    },
    "ownership_map": {
      "type": "object",
      "additionalProperties": false,
      "required": ["broader_regime_owner", "active_swing_owner", "immediate_owner"],
      "properties": {
        "broader_regime_owner": { "type": "string", "enum": ["buyers", "sellers", "balanced", "unclear"] },
        "active_swing_owner":   { "type": "string", "enum": ["buyers", "sellers", "balanced", "unclear"] },
        "immediate_owner":      { "type": "string", "enum": ["buyers", "sellers", "balanced", "unclear"] }
      }
    },
    "cross_market_snapshot": {
      "type": "object",
      "additionalProperties": false,
      "required": ["spot_vs_futures_gap_pct", "flow_driver", "spot_futures_delta_divergence"],
      "properties": {
        "spot_vs_futures_gap_pct": {
          "type": "number",
          "description": "(spot - futures) / futures * 100. Positive = spot premium, negative = futures premium."
        },
        "flow_driver": {
          "type": "string",
          "enum": ["futures_led", "spot_led", "balanced"],
          "description": "Which market is driving the current move, based on spot_flow_dominance and delta comparison."
        },
        "spot_futures_delta_divergence": {
          "type": "boolean",
          "description": "True if delta_fut and delta_spot disagree in sign on the latest 4h window."
        }
      }
    },
    "cross_timeframe_structure": {
      "type": "object",
      "additionalProperties": false,
      "required": ["main_tension", "key_shared_levels", "main_unresolved_factors"],
      "properties": {
        "main_tension": {
          "type": "string",
          "description": "Core cross-timeframe market conflict only. Do not describe what trade should be taken."
        },
        "key_shared_levels": {
          "type": "array",
          "maxItems": 6,
          "items": { "$ref": "#/$defs/key_level" }
        },
        "main_unresolved_factors": {
          "type": "array",
          "maxItems": 4,
          "items": { "type": "string" }
        }
      }
    },
    "cross_timeframe_map": {
      "type": "object",
      "additionalProperties": false,
      "required": ["ownership_map", "relationship_map", "cross_market_snapshot", "cross_timeframe_structure"],
      "properties": {
        "ownership_map":            { "$ref": "#/$defs/ownership_map" },
        "relationship_map":         { "$ref": "#/$defs/relationship_map" },
        "cross_market_snapshot":    { "$ref": "#/$defs/cross_market_snapshot" },
        "cross_timeframe_structure": { "$ref": "#/$defs/cross_timeframe_structure" }
      }
    }
  }
}
```

### v1.7 → v1.8 schema diff 汇总

**1. `schema_version`**
```diff
- "const": "scan_v1_7"
+ "const": "scan_v1_8"
```

**2. `structure_map.invalidation_level`：禁止 null**
```diff
  "invalidation_level": {
-   "type": ["number", "null"],
-   "description": "Single numeric anchor where this timeframe read breaks. Keep this as price only."
+   "type": "number",
+   "description": "Single numeric anchor where this timeframe read breaks. Must be a concrete price, never null. For balanced reads, use the nearest structural edge where the current equilibrium breaks."
  }
```

**3. `cross_timeframe_map`：新增 `cross_market_snapshot`**
```diff
  "cross_timeframe_map": {
    "required": [
      "ownership_map",
      "relationship_map",
+     "cross_market_snapshot",
      "cross_timeframe_structure"
    ],
    "properties": {
      "ownership_map": ...,
      "relationship_map": ...,
+     "cross_market_snapshot": { "$ref": "#/$defs/cross_market_snapshot" },
      "cross_timeframe_structure": ...
    }
  }
```

新增 `cross_market_snapshot` 定义：
```json
"cross_market_snapshot": {
  "type": "object",
  "additionalProperties": false,
  "required": ["spot_vs_futures_gap_pct", "flow_driver", "spot_futures_delta_divergence"],
  "properties": {
    "spot_vs_futures_gap_pct": {
      "type": "number",
      "description": "(spot - futures) / futures * 100. Positive = spot premium, negative = futures premium."
    },
    "flow_driver": {
      "type": "string",
      "enum": ["futures_led", "spot_led", "balanced"],
      "description": "Which market is driving the current move, based on spot_flow_dominance and delta comparison."
    },
    "spot_futures_delta_divergence": {
      "type": "boolean",
      "description": "True if delta_fut and delta_spot disagree in sign on the latest 4h window."
    }
  }
}
```

### 需同步修改的 Rust 代码位置

| 文件 | 位置 | 修改 |
|------|------|------|
| `provider.rs` | `validate_scan_output_v1_7` (L625) | 增加 `scan_v1_8` 版本判断分支，或重命名为通用 validate |
| `provider.rs` | `expect_number_or_null` 调用 (L808) | v1_8 路径改为 `expect_number`（不允许 null） |
| `provider.rs` | `scan_v1_7_schema_openai()` / `scan_v1_7_schema_gemini()` | 复制为 `scan_v1_8_*` 版本，修改 invalidation_level type + 新增 cross_market_snapshot |
| `provider.rs` | `ml_custom_llm_entry_scan_schema()` (L4247) | 改为调用 `scan_v1_8_schema_openai()` |
| `provider.rs` | `ml_qwen_entry_scan_schema()` | 同上 |
| `provider.rs` | QWEN OUTPUT CONTRACT 字符串 (L2034) | 更新 `schema_version` 为 `scan_v1_8`，新增 `cross_market_snapshot` 说明 |
| `prompt/scan.rs` | `MEDIUM_SCAN_PROMPT` | 指向更新后的 `medium_large_opportunity.txt` |

---

## 第三部分：提示词修改（medium_large_opportunity.txt）

### v1.7 → v1.8 变更说明

| 修改点 | 对应问题 | 说明 |
|--------|---------|------|
| 新增 ACTIVE RANGE CALIBRATION 段 | 问题 1, 5 | 禁止复制 path range，给出宽度基准 |
| 新增 CROSS-MARKET CHECK 段 | 问题 2 | 要求检查 spot/futures gap 和 delta divergence |
| 新增 DIFFERENTIATION RULES 段 | 问题 3, 4 | 要求 control_clarity 差异化、invalidation 非 null |
| BEFORE YOU OUTPUT, VERIFY 扩展 5 条 | 问题 1-6 | 强制 checklist 覆盖所有已知问题 |
| OUTPUT 段版本号更新 | — | `scan_v1_7` → `scan_v1_8` |

### 完整替换稿

```text
You are an expert order-flow market scanner for __SYMBOL__.

Use ONLY the provided scan JSON. Do not invent signals, levels, participant behavior, or cross-timeframe relationships that are not supported by the input.

GOAL
Build an objective multi-timeframe scan of the current market and participant environment for `15m`, `4h`, and `1d`.

If the evidence is mixed, say it is mixed.
If control is unclear, say it is unclear.
If sponsorship is unresolved, say it is unresolved.

HOW TO READ THE INPUT

Read the input as one live market environment:

- `now` gives current location, value state, active structures, live flow, and momentum snapshot
- `path_newest_to_oldest` shows how price arrived here across timeframes
- `events_newest_to_oldest` reveals initiation, absorption, exhaustion, and divergence behavior
- `supporting_context` gives broader flow, cross-market, and background context
- `raw_overflow` is supplemental detail and should be used only when it materially changes the scan

FIRST-PRINCIPLES OPERATING LENS

Scan the market from the bottom up:

1. Identify observed control on each timeframe.
2. Identify how clear or fragile that control is.
3. Identify where price sits relative to value and the active range.
4. Identify where supply and demand appear concentrated.
5. Identify who is acting aggressively, who is absorbing, and which side appears trapped or forced.
6. Identify whether the move is being sponsored, fading, or unresolved.
7. Identify how `15m`, `4h`, and `1d` relate to each other.
8. Identify whether spot and futures agree or diverge.

Treat the three timeframes as one market viewed at three different horizons:

- `15m` is the immediate auction and near-term control
- `4h` is the active swing environment
- `1d` is the broader regime and outer structure

ACTIVE RANGE CALIBRATION

`active_range` must represent the bracket that price is currently rotating within, NOT the full historical range of the lookback window.

Use value areas, sigma bands, bracket_board entries, and structural pivots to define the edges.
Do NOT copy `range_low` / `range_high` from `path_newest_to_oldest` — those are historical extremes across all bars in the window, not current structure.

Sanity check:
- 15m active range should typically be 0.3–1.5% of price
- 4h active range should typically be 1–4% of price
- 1d active range should typically be 2–8% of price
If your range is wider than these, verify that the full width is structurally relevant right now. If not, narrow it to the current rotation bracket.

CROSS-MARKET CHECK

Compare `spot_proxy_price` vs `futures_last_price` from `now.price_anchor`.
If the difference exceeds 0.1%, state which market is leading.

Check `spot_flow_dominance` from `current_flow_snapshot.cvd`:
- If below 0.10, the move is almost entirely futures-driven — flag this as a fragility factor.

Check whether `delta_spot` and `delta_fut` agree in sign on the latest 4h window:
- If they diverge (e.g., futures strongly positive, spot negative), note this in the relevant timeframe's `conflicting_facts`.

Populate `cross_market_snapshot` in `cross_timeframe_map` with these observations.

OBJECTIVITY RULES

- Prefer observed control over narrative explanation.
- Prefer observable participant behavior over identity stories.
- Prefer explicit uncertainty over forced certainty.
- Prefer structural facts over abstract adjectives.
- Do not encode pullback, reversal, continuation, or best-expression trade advice into fields that are meant to be factual.

DIFFERENTIATION RULES

- `control_clarity` must not be identical across all three timeframes unless genuinely indistinguishable. A timeframe with a large last closed bar, strong CVD slope, and high close location likely has `strong` clarity, not `mixed`.
- `invalidation_level` must always be a concrete price. For balanced or unclear control, use the nearest structural edge (value low, sigma band, or TPO boundary) where the current equilibrium breaks.

OUTPUT

Return JSON only and follow `scan_v1_8`.

Top level:
- `schema_version` (must be `scan_v1_8`)
- `meta`
- `timeframes`
- `cross_timeframe_map`

For each timeframe return:
- `state`
- `flow_map`
- `structure_map`
- `validation`

`state` is for control, clarity, value, range interaction, and sponsorship.
`flow_map` is for aggressive side, absorption side, trapped side, and a few factual participant observations.
`structure_map` is for active range, supply, demand, key levels, invalidation, and the nearest structural path in both directions.
`validation` is for read basis, supporting facts, conflicting facts, alignment checks, and fragility.

`cross_timeframe_map` is for:
- `ownership_map`
- `relationship_map`
- `cross_market_snapshot`
- `cross_timeframe_structure`

Keep `relationship_map` factual, not interpretive.
Keep `path_map` structural, not a trade plan.

BEFORE YOU OUTPUT, VERIFY

- Did you keep each timeframe grounded in observable control, value, structure, flow, and validation?
- If the market is mixed, conflicted, or unclear, did you say so explicitly?
- Did you avoid inventing participant motives or identities not supported by the data?
- Did you keep `relationship_map` factual rather than interpretive?
- Did you keep `path_map` structural rather than turning it into a target/entry plan?
- Are `ownership_map` and `cross_timeframe_structure` consistent with the three timeframe blocks?
- Is your `active_range` derived from current structure (value area, sigma band, bracket), not from path range_low/range_high?
- Is `invalidation_level` a concrete price for every timeframe?
- Is `control_clarity` differentiated across timeframes where the evidence warrants it?
- Did you check spot vs futures divergence and populate `cross_market_snapshot`?
- Did you reference at least one signal from `supporting_context` (funding_rate, liquidation_density, vpin, or cvd_path_snapshot) in at least one timeframe's `validation`?

FORMAT

Return JSON only.
Follow the provider schema exactly.
```

---

## 第四部分：QWEN OUTPUT CONTRACT 更新

`provider.rs` L2034 处的 medium_large_opportunity 分支字符串需要同步更新：

```text
QWEN OUTPUT CONTRACT:
- Return exactly one JSON object.
- No extra top-level keys.
- Top-level keys must be `schema_version`, `meta`, `timeframes`, and `cross_timeframe_map`.
- `schema_version` must be `scan_v1_8`.
- `timeframes` must contain `15m`, `4h`, and `1d`.
- Each timeframe must include `state`, `flow_map`, `structure_map`, and `validation`.
- `structure_map.invalidation_level` must be a number, not null.
- `cross_timeframe_map` must include `ownership_map`, `relationship_map`, `cross_market_snapshot`, and `cross_timeframe_structure`.
- `cross_market_snapshot` must include `spot_vs_futures_gap_pct`, `flow_driver`, and `spot_futures_delta_divergence`.
```
