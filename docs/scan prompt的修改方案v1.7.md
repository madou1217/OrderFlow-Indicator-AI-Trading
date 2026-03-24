```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "stage1_scan_v1_7",
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
      "const": "scan_v1_7"
    },
    "meta": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "symbol",
        "scan_ts_bucket"
      ],
      "properties": {
        "symbol": {
          "type": "string"
        },
        "scan_ts_bucket": {
          "type": "string",
          "format": "date-time"
        }
      }
    },
    "timeframes": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "15m",
        "4h",
        "1d"
      ],
      "properties": {
        "15m": {
          "$ref": "#/$defs/timeframe_scan"
        },
        "4h": {
          "$ref": "#/$defs/timeframe_scan"
        },
        "1d": {
          "$ref": "#/$defs/timeframe_scan"
        }
      }
    },
    "cross_timeframe_map": {
      "$ref": "#/$defs/cross_timeframe_map"
    }
  },
  "$defs": {
    "price_zone": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "low",
        "high",
        "reason"
      ],
      "properties": {
        "low": {
          "type": "number"
        },
        "high": {
          "type": "number"
        },
        "reason": {
          "type": "string"
        }
      }
    },
    "key_level": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "price",
        "type",
        "reason"
      ],
      "properties": {
        "price": {
          "type": "number"
        },
        "type": {
          "type": "string",
          "enum": [
            "support",
            "resistance",
            "pivot",
            "value_edge",
            "liquidity_wall",
            "imbalance_edge"
          ]
        },
        "reason": {
          "type": "string"
        }
      }
    },
    "level_reference": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "ref_kind",
        "price",
        "label"
      ],
      "properties": {
        "ref_kind": {
          "type": "string",
          "enum": [
            "key_level",
            "demand_zone_edge",
            "supply_zone_edge",
            "active_range_low",
            "active_range_high",
            "external_price",
            "none"
          ]
        },
        "price": {
          "type": [
            "number",
            "null"
          ]
        },
        "label": {
          "type": "string",
          "description": "Short anchor label only, for example '4h resistance' or '1d demand high'."
        }
      }
    },
    "path_side": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "first_objective_ref",
        "first_barrier_ref"
      ],
      "properties": {
        "first_objective_ref": {
          "$ref": "#/$defs/level_reference"
        },
        "first_barrier_ref": {
          "$ref": "#/$defs/level_reference"
        }
      }
    },
    "role_observation": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "role",
        "observed_behavior",
        "evidence",
        "confidence"
      ],
      "properties": {
        "role": {
          "type": "string",
          "enum": [
            "large_directional_flow",
            "higher_timeframe_sponsorship",
            "crowd_behavior",
            "passive_liquidity"
          ]
        },
        "observed_behavior": {
          "type": "string",
          "description": "Observed market behavior only. Do not invent participant identity or narrative intent."
        },
        "evidence": {
          "type": "array",
          "maxItems": 3,
          "items": {
            "type": "string"
          }
        },
        "confidence": {
          "type": "string",
          "enum": [
            "high",
            "medium",
            "low"
          ]
        }
      }
    },
    "state_block": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "control_side",
        "control_clarity",
        "value_location",
        "range_state",
        "sponsorship_state"
      ],
      "properties": {
        "control_side": {
          "type": "string",
          "enum": [
            "buyers",
            "sellers",
            "balanced",
            "unclear"
          ],
          "description": "Observed control on this timeframe. This is not a trade instruction."
        },
        "control_clarity": {
          "type": "string",
          "enum": [
            "strong",
            "mixed",
            "conflicted"
          ]
        },
        "value_location": {
          "type": "string",
          "enum": [
            "above_value",
            "below_value",
            "inside_value",
            "accepted_above",
            "accepted_below",
            "rejected_from_above",
            "rejected_from_below"
          ]
        },
        "range_state": {
          "type": "string",
          "enum": [
            "inside_range",
            "accepting_above_range",
            "accepting_below_range",
            "rejecting_above_range",
            "rejecting_below_range",
            "testing_range_high",
            "testing_range_low",
            "range_unresolved"
          ],
          "description": "Range interaction fact only. Avoid pullback, continuation, or reversal interpretation here."
        },
        "sponsorship_state": {
          "type": "string",
          "enum": [
            "active",
            "fragile",
            "fading",
            "absent",
            "unresolved"
          ]
        }
      }
    },
    "flow_map": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "aggressive_side",
        "absorption_side",
        "trapped_side",
        "role_observations"
      ],
      "properties": {
        "aggressive_side": {
          "type": "string",
          "enum": [
            "buyers",
            "sellers",
            "balanced",
            "unclear"
          ],
          "description": "Which side is currently acting aggressively on this timeframe."
        },
        "absorption_side": {
          "type": "string",
          "enum": [
            "buyers",
            "sellers",
            "none",
            "unclear"
          ],
          "description": "Which side is visibly absorbing opposing flow on this timeframe."
        },
        "trapped_side": {
          "type": "string",
          "enum": [
            "buyers",
            "sellers",
            "none",
            "unclear"
          ],
          "description": "Which side appears trapped or forced on this timeframe."
        },
        "role_observations": {
          "type": "array",
          "maxItems": 3,
          "items": {
            "$ref": "#/$defs/role_observation"
          }
        }
      }
    },
    "structure_map": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "active_range",
        "range_width_vs_atr",
        "dominant_demand_zone",
        "dominant_supply_zone",
        "key_levels",
        "invalidation_level",
        "path_map"
      ],
      "properties": {
        "active_range": {
          "type": "object",
          "additionalProperties": false,
          "required": [
            "low",
            "high"
          ],
          "properties": {
            "low": {
              "type": "number"
            },
            "high": {
              "type": "number"
            }
          }
        },
        "range_width_vs_atr": {
          "type": "string",
          "enum": [
            "narrow",
            "normal",
            "wide"
          ]
        },
        "dominant_demand_zone": {
          "anyOf": [
            {
              "$ref": "#/$defs/price_zone"
            },
            {
              "type": "null"
            }
          ]
        },
        "dominant_supply_zone": {
          "anyOf": [
            {
              "$ref": "#/$defs/price_zone"
            },
            {
              "type": "null"
            }
          ]
        },
        "key_levels": {
          "type": "array",
          "maxItems": 6,
          "items": {
            "$ref": "#/$defs/key_level"
          }
        },
        "invalidation_level": {
          "type": [
            "number",
            "null"
          ],
          "description": "Single numeric anchor where this timeframe read breaks. Keep this as price only."
        },
        "path_map": {
          "type": "object",
          "additionalProperties": false,
          "required": [
            "upside",
            "downside"
          ],
          "properties": {
            "upside": {
              "$ref": "#/$defs/path_side"
            },
            "downside": {
              "$ref": "#/$defs/path_side"
            }
          }
        }
      }
    },
    "validation_block": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "read_basis",
        "supporting_facts",
        "conflicting_facts",
        "recent_closed_bars_align_with_read",
        "cvd_slope_aligns_with_read",
        "current_partial_bar_aligns_with_read",
        "fragility_summary"
      ],
      "properties": {
        "read_basis": {
          "type": "string",
          "enum": [
            "closed_bar_continuation",
            "live_flow_reversal",
            "exhaustion_inference",
            "structural_inference",
            "mixed"
          ]
        },
        "supporting_facts": {
          "type": "array",
          "maxItems": 4,
          "items": {
            "type": "string"
          }
        },
        "conflicting_facts": {
          "type": "array",
          "maxItems": 4,
          "items": {
            "type": "string"
          }
        },
        "recent_closed_bars_align_with_read": {
          "type": "boolean"
        },
        "cvd_slope_aligns_with_read": {
          "type": "boolean"
        },
        "current_partial_bar_aligns_with_read": {
          "type": "boolean"
        },
        "fragility_summary": {
          "type": "string",
          "description": "Short factual note about what makes this timeframe read vulnerable or unresolved."
        }
      }
    },
    "timeframe_scan": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "state",
        "flow_map",
        "structure_map",
        "validation"
      ],
      "properties": {
        "state": {
          "$ref": "#/$defs/state_block"
        },
        "flow_map": {
          "$ref": "#/$defs/flow_map"
        },
        "structure_map": {
          "$ref": "#/$defs/structure_map"
        },
        "validation": {
          "$ref": "#/$defs/validation_block"
        }
      }
    },
    "timeframe_relationship": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "control_relation",
        "lower_tf_value_location_vs_higher_tf",
        "lower_tf_range_location_vs_higher_tf"
      ],
      "properties": {
        "control_relation": {
          "type": "string",
          "enum": [
            "aligned",
            "opposed",
            "neutral"
          ],
          "description": "Relative control fact only. Do not encode pullback, reversal, continuation, or trade preference interpretation here."
        },
        "lower_tf_value_location_vs_higher_tf": {
          "type": "string",
          "enum": [
            "above_higher_tf_value",
            "inside_higher_tf_value",
            "below_higher_tf_value"
          ]
        },
        "lower_tf_range_location_vs_higher_tf": {
          "type": "string",
          "enum": [
            "above_higher_tf_range",
            "inside_higher_tf_range",
            "below_higher_tf_range"
          ]
        }
      }
    },
    "relationship_map": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "15m_vs_4h",
        "4h_vs_1d",
        "15m_vs_1d"
      ],
      "properties": {
        "15m_vs_4h": {
          "$ref": "#/$defs/timeframe_relationship"
        },
        "4h_vs_1d": {
          "$ref": "#/$defs/timeframe_relationship"
        },
        "15m_vs_1d": {
          "$ref": "#/$defs/timeframe_relationship"
        }
      }
    },
    "ownership_map": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "broader_regime_owner",
        "active_swing_owner",
        "immediate_owner"
      ],
      "properties": {
        "broader_regime_owner": {
          "type": "string",
          "enum": [
            "buyers",
            "sellers",
            "balanced",
            "unclear"
          ]
        },
        "active_swing_owner": {
          "type": "string",
          "enum": [
            "buyers",
            "sellers",
            "balanced",
            "unclear"
          ]
        },
        "immediate_owner": {
          "type": "string",
          "enum": [
            "buyers",
            "sellers",
            "balanced",
            "unclear"
          ]
        }
      }
    },
    "cross_timeframe_structure": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "main_tension",
        "key_shared_levels",
        "main_unresolved_factors"
      ],
      "properties": {
        "main_tension": {
          "type": "string",
          "description": "Core cross-timeframe market conflict only. Do not describe what trade should be taken."
        },
        "key_shared_levels": {
          "type": "array",
          "maxItems": 6,
          "items": {
            "$ref": "#/$defs/key_level"
          }
        },
        "main_unresolved_factors": {
          "type": "array",
          "maxItems": 4,
          "items": {
            "type": "string"
          }
        }
      }
    },
    "cross_timeframe_map": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "ownership_map",
        "relationship_map",
        "cross_timeframe_structure"
      ],
      "properties": {
        "ownership_map": {
          "$ref": "#/$defs/ownership_map"
        },
        "relationship_map": {
          "$ref": "#/$defs/relationship_map"
        },
        "cross_timeframe_structure": {
          "$ref": "#/$defs/cross_timeframe_structure"
        }
      }
    }
  }
}
```

---

## 基于 v1.7 的 `scan/medium_large_opportunity.txt` 全量替换稿

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

Treat the three timeframes as one market viewed at three different horizons:

- `15m` is the immediate auction and near-term control
- `4h` is the active swing environment
- `1d` is the broader regime and outer structure

OBJECTIVITY RULES

- Prefer observed control over narrative explanation.
- Prefer observable participant behavior over identity stories.
- Prefer explicit uncertainty over forced certainty.
- Prefer structural facts over abstract adjectives.
- Do not encode pullback, reversal, continuation, or best-expression trade advice into fields that are meant to be factual.

OUTPUT

Return JSON only and follow `scan_v1_7`.

Top level:
- `schema_version`
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

FORMAT

Return JSON only.
Follow the provider schema exactly.
```
