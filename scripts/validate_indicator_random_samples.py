#!/usr/bin/env python3
"""
Random-sample indicator validation for orderflow-indicator-engine.

What this script validates:
1. Snapshot completeness for all 24 indicators on each sampled minute.
2. Exact source-vs-snapshot checks for indicators that can be directly
   reconstructed from canonical 1m source tables.
3. Exact snapshot-vs-feature-table checks where a dedicated feature table exists.

This is intended as a reusable smoke/regression validator after indicator-engine
changes, especially compute-ms and backfill optimizations.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import random
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


EXPECTED_CODES = [
    "absorption",
    "avwap",
    "bearish_absorption",
    "bearish_initiation",
    "bullish_absorption",
    "bullish_initiation",
    "buying_exhaustion",
    "cvd_pack",
    "divergence",
    "ema_trend_regime",
    "footprint",
    "funding_rate",
    "fvg",
    "high_volume_pulse",
    "initiation",
    "kline_history",
    "liquidation_density",
    "orderbook_depth",
    "price_volume_structure",
    "rvwap_sigma_bands",
    "selling_exhaustion",
    "tpo_market_profile",
    "vpin",
    "whale_trades",
]

EXACT_CHECK_NAMES = [
    "price_volume_structure.source",
    "footprint.source",
    "cvd_pack.source",
    "cvd_pack.feature_1m",
    "orderbook_depth.source",
    "orderbook_depth.feature_1m",
    "funding_rate.feature_1m",
    "avwap.feature_1m",
    "whale_trades.feature_1m",
]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Randomly validate indicator output slices against source/feature tables."
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--recent-hours", type=int, default=168)
    parser.add_argument("--max-candidates", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--config", default="/data/config/config.yaml")
    parser.add_argument("--dsn", default=None, help="Optional PostgreSQL DSN override")
    parser.add_argument("--since-ts", default=None, help="Only validate snapshots at or after this UTC timestamp")
    parser.add_argument(
        "--created-since-ts",
        default=None,
        help="Only validate snapshot minutes whose snapshot rows were created at or after this UTC timestamp",
    )
    parser.add_argument("--output-json", default=None)
    return parser.parse_args()


def load_dsn(config_path: str, override: str | None) -> str:
    if override:
        return override
    cfg = yaml.safe_load(Path(config_path).read_text())
    db = cfg["database"]
    return (
        f"postgresql://{db['user']}:{db['password_env']}@"
        f"{db['host']}:{db['port']}/{db['database']}"
    )


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def psql_query(dsn: str, sql: str) -> list[list[str]]:
    proc = subprocess.run(
        ["psql", dsn, "-X", "-q", "-t", "-A", "-F", "\t", "-c", sql],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    rows: list[list[str]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(line.split("\t"))
    return rows


def fetch_candidate_timestamps(
    dsn: str,
    symbol: str,
    recent_hours: int,
    max_candidates: int,
    since_ts: str | None,
    created_since_ts: str | None,
) -> list[str]:
    since_clause = ""
    if since_ts:
        since_clause = f"AND ts_snapshot >= {sql_literal(since_ts)}"
    created_since_clause = ""
    if created_since_ts:
        created_since_clause = f"AND created_at >= {sql_literal(created_since_ts)}"
    sql = f"""
    SELECT ts_snapshot::text
    FROM (
        SELECT ts_snapshot, COUNT(*) AS cnt
        FROM feat.indicator_snapshot
        WHERE symbol = {sql_literal(symbol)}
          AND ts_snapshot >= now() - interval {sql_literal(f"{recent_hours} hours")}
          {since_clause}
          {created_since_clause}
        GROUP BY ts_snapshot
        HAVING COUNT(*) = {len(EXPECTED_CODES)}
        ORDER BY ts_snapshot DESC
        LIMIT {max_candidates}
    ) s
    ORDER BY ts_snapshot ASC
    """
    return [row[0] for row in psql_query(dsn, sql)]


def fetch_snapshot_payloads(dsn: str, symbol: str, ts: str) -> dict[str, Any]:
    sql = f"""
    SELECT indicator_code, payload_json::text
    FROM feat.v_indicator_snapshot_hydrated
    WHERE symbol = {sql_literal(symbol)}
      AND ts_snapshot = {sql_literal(ts)}
    ORDER BY indicator_code
    """
    return {code: json.loads(payload) for code, payload in psql_query(dsn, sql)}


def fetch_one_json(dsn: str, sql: str) -> dict[str, Any] | None:
    rows = psql_query(dsn, sql)
    if not rows:
        return None
    return json.loads(rows[0][0])


def fetch_two_market_rows(
    dsn: str,
    table: str,
    symbol: str,
    ts: str,
    cols: list[str],
    ts_col: str = "ts_bucket",
    where_extra: str = "",
) -> dict[str, dict[str, Any]]:
    payload_cols = [col for col in cols if col != "market"]
    col_sql = ", ".join(["market", *payload_cols])
    json_sql = ", ".join(f"'{col}', x.{col}" for col in payload_cols)
    sql = f"""
    SELECT x.market::text, json_build_object({json_sql})::text
    FROM (
        SELECT {col_sql}
        FROM {table}
        WHERE symbol = {sql_literal(symbol)}
          AND {ts_col} = {sql_literal(ts)}
          AND market IN ('futures', 'spot')
          {where_extra}
    ) x
    ORDER BY x.market
    """
    out: dict[str, dict[str, Any]] = {}
    for market, payload in psql_query(dsn, sql):
        out[market] = json.loads(payload)
    return out


def fetch_single_row(
    dsn: str, table: str, symbol: str, ts: str, where_extra: str, cols: list[str], ts_col: str = "ts_bucket"
) -> dict[str, Any] | None:
    col_sql = ", ".join(cols)
    sql = f"""
    SELECT row_to_json(x)::text
    FROM (
        SELECT {col_sql}
        FROM {table}
        WHERE symbol = {sql_literal(symbol)}
          AND {ts_col} = {sql_literal(ts)}
          {where_extra}
    ) x
    """
    return fetch_one_json(dsn, sql)


def compare_float(actual: Any, expected: Any, abs_tol: float = 1e-9, rel_tol: float = 1e-9) -> bool:
    if actual is None and expected is None:
        return True
    if actual is None or expected is None:
        return False
    return math.isclose(float(actual), float(expected), rel_tol=rel_tol, abs_tol=abs_tol)


def compare_text_timestamp(actual: Any, expected: Any) -> bool:
    if actual is None and expected is None:
        return True
    if actual is None or expected is None:
        return False
    try:
        a = dt.datetime.fromisoformat(str(actual).replace("Z", "+00:00"))
        e = dt.datetime.fromisoformat(str(expected).replace("Z", "+00:00"))
    except ValueError:
        return str(actual) == str(expected)
    return a == e


def normalize_optional_positive(value: Any) -> Any:
    if value is None:
        return None
    num = float(value)
    return None if num <= 0.0 else num


def build_pvs_source(trade_fut: dict[str, Any]) -> dict[str, Any]:
    levels = trade_fut["profile_levels"] or []
    bar_volume = float(trade_fut["buy_qty"]) + float(trade_fut["sell_qty"])
    poc_price = None
    poc_volume = None
    best = None
    for entry in levels:
        price = float(entry[0])
        volume = float(entry[1]) + float(entry[2])
        rank = (volume, -price)
        if best is None or rank > best[0]:
            best = (rank, price, volume)
    if best is not None:
        _, poc_price, poc_volume = best
    return {
        "bar_volume": bar_volume,
        "poc_price": poc_price,
        "poc_volume": poc_volume,
    }


def build_footprint_source(trade_fut: dict[str, Any]) -> dict[str, Any]:
    buy_qty = float(trade_fut["buy_qty"])
    sell_qty = float(trade_fut["sell_qty"])
    return {
        "window_total_qty": buy_qty + sell_qty,
        "window_delta": buy_qty - sell_qty,
    }


def build_cvd_source(trade_fut: dict[str, Any], trade_spot: dict[str, Any]) -> dict[str, Any]:
    def values(row: dict[str, Any]) -> tuple[float, float]:
        buy_qty = float(row["buy_qty"])
        sell_qty = float(row["sell_qty"])
        total = buy_qty + sell_qty
        delta = buy_qty - sell_qty
        rel = None if total == 0 else delta / total
        return delta, rel

    fut_delta, fut_rel = values(trade_fut)
    spot_delta, spot_rel = values(trade_spot)
    return {
        "delta_fut": fut_delta,
        "relative_delta_fut": fut_rel,
        "delta_spot": spot_delta,
        "relative_delta_spot": spot_rel,
    }


def build_orderbook_source(ob_fut: dict[str, Any], ob_spot: dict[str, Any]) -> dict[str, Any]:
    def twa(row: dict[str, Any], sum_col: str) -> float | None:
        sample_count = int(row["sample_count"])
        if sample_count == 0:
            return None
        return float(row[sum_col]) / sample_count

    return {
        "spread_twa_fut": twa(ob_fut, "spread_sum"),
        "topk_depth_twa_fut": twa(ob_fut, "topk_depth_sum"),
        "obi_k_dw_twa_fut": twa(ob_fut, "obi_k_dw_sum"),
        "ofi_fut": float(ob_fut["ofi_sum"]),
        "spread_twa_spot": twa(ob_spot, "spread_sum"),
        "topk_depth_twa_spot": twa(ob_spot, "topk_depth_sum"),
        "obi_k_dw_twa_spot": twa(ob_spot, "obi_k_dw_sum"),
        "ofi_spot": float(ob_spot["ofi_sum"]),
    }


def check_fields(
    failures: list[dict[str, Any]],
    ts: str,
    check_name: str,
    actual: dict[str, Any],
    expected: dict[str, Any],
    fields: list[str],
    timestamp_fields: set[str] | None = None,
) -> None:
    timestamp_fields = timestamp_fields or set()
    for field in fields:
        ok = (
            compare_text_timestamp(actual.get(field), expected.get(field))
            if field in timestamp_fields
            else compare_float(actual.get(field), expected.get(field))
        )
        if not ok:
            failures.append(
                {
                    "ts": ts,
                    "check": check_name,
                    "field": field,
                    "expected": expected.get(field),
                    "actual": actual.get(field),
                }
            )


def validate_sample(dsn: str, symbol: str, ts: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    failures: list[dict[str, Any]] = []
    snapshots = fetch_snapshot_payloads(dsn, symbol, ts)

    code_set = sorted(snapshots.keys())
    if code_set != EXPECTED_CODES:
        failures.append(
            {
                "ts": ts,
                "check": "snapshot.completeness",
                "field": "indicator_codes",
                "expected": EXPECTED_CODES,
                "actual": code_set,
            }
        )

    trade_rows = fetch_two_market_rows(
        dsn,
        "md.agg_trade_1m",
        symbol,
        ts,
        [
            "market",
            "buy_qty",
            "sell_qty",
            "buy_notional",
            "sell_notional",
            "first_price",
            "last_price",
            "high_price",
            "low_price",
            "trade_count",
            "profile_levels",
            "whale_json",
        ],
    )
    ob_rows = fetch_two_market_rows(
        dsn,
        "md.agg_orderbook_1m",
        symbol,
        ts,
        [
            "market",
            "sample_count",
            "spread_sum",
            "topk_depth_sum",
            "obi_k_dw_sum",
            "ofi_sum",
        ],
    )

    trade_fut = trade_rows.get("futures")
    trade_spot = trade_rows.get("spot")
    ob_fut = ob_rows.get("futures")
    ob_spot = ob_rows.get("spot")
    if not (trade_fut and trade_spot and ob_fut and ob_spot):
        failures.append(
            {
                "ts": ts,
                "check": "source.presence",
                "field": "md_rows",
                "expected": "trade_fut,trade_spot,ob_fut,ob_spot present",
                "actual": {
                    "trade_fut": bool(trade_fut),
                    "trade_spot": bool(trade_spot),
                    "ob_fut": bool(ob_fut),
                    "ob_spot": bool(ob_spot),
                },
            }
        )
        return (
            {
                "ts": ts,
                "snapshot_count": len(snapshots),
                "checks_run": [],
            },
            failures,
        )

    pvs_src = build_pvs_source(trade_fut)
    pvs_out = {
        "bar_volume": snapshots["price_volume_structure"].get("bar_volume"),
        "poc_price": snapshots["price_volume_structure"].get("poc_price"),
        "poc_volume": snapshots["price_volume_structure"].get("poc_volume"),
    }
    check_fields(
        failures,
        ts,
        "price_volume_structure.source",
        pvs_out,
        pvs_src,
        ["bar_volume", "poc_price", "poc_volume"],
    )

    footprint_src = build_footprint_source(trade_fut)
    footprint_out = {
        "window_total_qty": snapshots["footprint"].get("window_total_qty"),
        "window_delta": snapshots["footprint"].get("window_delta"),
    }
    check_fields(
        failures,
        ts,
        "footprint.source",
        footprint_out,
        footprint_src,
        ["window_total_qty", "window_delta"],
    )

    cvd_src = build_cvd_source(trade_fut, trade_spot)
    cvd_out = {
        "delta_fut": snapshots["cvd_pack"].get("delta_fut"),
        "relative_delta_fut": snapshots["cvd_pack"].get("relative_delta_fut"),
        "delta_spot": snapshots["cvd_pack"].get("delta_spot"),
        "relative_delta_spot": snapshots["cvd_pack"].get("relative_delta_spot"),
    }
    check_fields(
        failures,
        ts,
        "cvd_pack.source",
        cvd_out,
        cvd_src,
        ["delta_fut", "relative_delta_fut", "delta_spot", "relative_delta_spot"],
    )

    cvd_feature = fetch_single_row(
        dsn,
        "feat.cvd_pack",
        symbol,
        ts,
        "AND window_code = '1m'",
        ["delta_fut", "relative_delta_fut", "delta_spot", "relative_delta_spot"],
    )
    if cvd_feature:
        check_fields(
            failures,
            ts,
            "cvd_pack.feature_1m",
            cvd_out,
            cvd_feature,
            ["delta_fut", "relative_delta_fut", "delta_spot", "relative_delta_spot"],
        )

    ob_src = build_orderbook_source(ob_fut, ob_spot)
    ob_snapshot = {
        "spread_twa_fut": snapshots["orderbook_depth"].get("spread_twa_fut"),
        "topk_depth_twa_fut": snapshots["orderbook_depth"].get("topk_depth_twa_fut"),
        "obi_k_dw_twa_fut": snapshots["orderbook_depth"].get("obi_k_dw_twa_fut"),
        "ofi_fut": snapshots["orderbook_depth"].get("ofi_fut"),
        "spread_twa_spot": snapshots["orderbook_depth"].get("spread_twa_spot"),
        "topk_depth_twa_spot": snapshots["orderbook_depth"].get("topk_depth_twa_spot"),
        "obi_k_dw_twa_spot": snapshots["orderbook_depth"].get("obi_k_dw_twa_spot"),
        "ofi_spot": snapshots["orderbook_depth"].get("ofi_spot"),
    }
    check_fields(
        failures,
        ts,
        "orderbook_depth.source",
        ob_snapshot,
        ob_src,
        [
            "spread_twa_fut",
            "topk_depth_twa_fut",
            "obi_k_dw_twa_fut",
            "ofi_fut",
            "spread_twa_spot",
            "topk_depth_twa_spot",
            "obi_k_dw_twa_spot",
            "ofi_spot",
        ],
    )

    ob_feature = fetch_single_row(
        dsn,
        "feat.orderbook_feature",
        symbol,
        ts,
        "AND bar_interval = '00:01:00'",
        [
            "spread_twa_fut",
            "topk_depth_twa_fut",
            "obi_k_dw_twa_fut",
            "ofi_fut",
            "spread_twa_spot",
            "topk_depth_twa_spot",
            "obi_k_dw_twa_spot",
            "ofi_spot",
        ],
    )
    if ob_feature:
        check_fields(
            failures,
            ts,
            "orderbook_depth.feature_1m",
            ob_snapshot,
            ob_feature,
            [
                "spread_twa_fut",
                "topk_depth_twa_fut",
                "obi_k_dw_twa_fut",
                "ofi_fut",
                "spread_twa_spot",
                "topk_depth_twa_spot",
                "obi_k_dw_twa_spot",
                "ofi_spot",
            ],
        )

    funding_snapshot = snapshots["funding_rate"]
    funding_feature = fetch_single_row(
        dsn,
        "feat.funding_feature",
        symbol,
        ts,
        "AND bar_interval = '00:01:00'",
        [
            "funding_current",
            "funding_current_effective_ts",
            "funding_twa",
            "mark_price_last",
            "mark_price_last_ts",
            "mark_price_twap",
        ],
    )
    if funding_feature:
        funding_actual = {
            "funding_current": funding_snapshot.get("funding_current"),
            "funding_current_effective_ts": funding_snapshot.get("funding_current_effective_ts"),
            "funding_twa": funding_snapshot.get("funding_twa"),
            "mark_price_last": funding_snapshot.get("mark_price_last"),
            "mark_price_last_ts": funding_snapshot.get("mark_price_last_ts"),
            "mark_price_twap": funding_snapshot.get("mark_price_twap"),
        }
        check_fields(
            failures,
            ts,
            "funding_rate.feature_1m",
            funding_actual,
            funding_feature,
            [
                "funding_current",
                "funding_current_effective_ts",
                "funding_twa",
                "mark_price_last",
                "mark_price_last_ts",
                "mark_price_twap",
            ],
            timestamp_fields={"funding_current_effective_ts", "mark_price_last_ts"},
        )

    avwap_snapshot = snapshots["avwap"]
    avwap_feature = fetch_single_row(
        dsn,
        "feat.avwap_feature",
        symbol,
        ts,
        "AND bar_interval = '7 days'",
        [
            "anchor_ts",
            "avwap_fut",
            "avwap_spot",
            "fut_last_price",
            "fut_mark_price",
            "price_minus_avwap_fut",
            "price_minus_spot_avwap_fut",
            "price_minus_spot_avwap_futmark",
            "xmk_avwap_gap_f_minus_s",
            "zavwap_gap",
        ],
    )
    if avwap_feature:
        avwap_actual = {
            "anchor_ts": avwap_snapshot.get("anchor_ts"),
            "avwap_fut": avwap_snapshot.get("avwap_fut"),
            "avwap_spot": avwap_snapshot.get("avwap_spot"),
            "fut_last_price": avwap_snapshot.get("fut_last_price"),
            "fut_mark_price": avwap_snapshot.get("fut_mark_price"),
            "price_minus_avwap_fut": avwap_snapshot.get("price_minus_avwap_fut"),
            "price_minus_spot_avwap_fut": avwap_snapshot.get("price_minus_spot_avwap_fut"),
            "price_minus_spot_avwap_futmark": avwap_snapshot.get("price_minus_spot_avwap_futmark"),
            "xmk_avwap_gap_f_minus_s": avwap_snapshot.get("xmk_avwap_gap_f_minus_s"),
            "zavwap_gap": avwap_snapshot.get("zavwap_gap"),
        }
        check_fields(
            failures,
            ts,
            "avwap.feature_7d",
            avwap_actual,
            avwap_feature,
            [
                "avwap_fut",
                "avwap_spot",
                "fut_last_price",
                "fut_mark_price",
                "price_minus_avwap_fut",
                "price_minus_spot_avwap_fut",
                "price_minus_spot_avwap_futmark",
                "xmk_avwap_gap_f_minus_s",
                "zavwap_gap",
            ],
        )
        if not compare_text_timestamp(avwap_actual["anchor_ts"], avwap_feature["anchor_ts"]):
            failures.append(
                {
                    "ts": ts,
                    "check": "avwap.feature_7d",
                    "field": "anchor_ts",
                    "expected": avwap_feature["anchor_ts"],
                    "actual": avwap_actual["anchor_ts"],
                }
            )

    whale_rows = fetch_two_market_rows(
        dsn,
        "feat.whale_trade_rollup",
        symbol,
        ts,
        [
            "market",
            "threshold_usdt",
            "whale_trade_count",
            "whale_buy_count",
            "whale_sell_count",
            "whale_notional_total",
            "whale_notional_buy",
            "whale_notional_sell",
            "whale_qty_eth_total",
            "max_single_trade_notional",
        ],
        where_extra="AND bar_interval = '00:01:00'",
    )
    whale_snapshot = snapshots["whale_trades"]
    fut_whale = whale_rows.get("futures")
    spot_whale = whale_rows.get("spot")
    if fut_whale and spot_whale:
        whale_expected = {
            "threshold_usdt": float(fut_whale["threshold_usdt"]),
            "fut_count": int(fut_whale["whale_trade_count"]),
            "fut_buy_count": int(fut_whale["whale_buy_count"]),
            "fut_sell_count": int(fut_whale["whale_sell_count"]),
            "fut_notional_sum_usd": float(fut_whale["whale_notional_total"]),
            "fut_whale_delta_notional": float(fut_whale["whale_notional_buy"]) - float(fut_whale["whale_notional_sell"]),
            "fut_whale_qty_total": float(fut_whale["whale_qty_eth_total"]),
            "fut_max_single_trade_notional": fut_whale["max_single_trade_notional"],
            "spot_count": int(spot_whale["whale_trade_count"]),
            "spot_buy_count": int(spot_whale["whale_buy_count"]),
            "spot_sell_count": int(spot_whale["whale_sell_count"]),
            "spot_notional_sum_usd": float(spot_whale["whale_notional_total"]),
            "spot_whale_delta_notional": float(spot_whale["whale_notional_buy"]) - float(spot_whale["whale_notional_sell"]),
            "spot_whale_qty_total": float(spot_whale["whale_qty_eth_total"]),
            "spot_max_single_trade_notional": spot_whale["max_single_trade_notional"],
        }
        whale_actual = {
            "threshold_usdt": whale_snapshot.get("threshold_usdt"),
            "fut_count": whale_snapshot.get("fut_count"),
            "fut_buy_count": whale_snapshot.get("fut_buy_count"),
            "fut_sell_count": whale_snapshot.get("fut_sell_count"),
            "fut_notional_sum_usd": whale_snapshot.get("fut_notional_sum_usd"),
            "fut_whale_delta_notional": whale_snapshot.get("fut_whale_delta_notional"),
            "fut_whale_qty_total": float(whale_snapshot.get("fut_whale_qty_buy") or 0.0)
            + float(whale_snapshot.get("fut_whale_qty_sell") or 0.0),
            "fut_max_single_trade_notional": normalize_optional_positive(
                whale_snapshot.get("fut_max_single_trade_notional")
            ),
            "spot_count": whale_snapshot.get("spot_count"),
            "spot_buy_count": whale_snapshot.get("spot_buy_count"),
            "spot_sell_count": whale_snapshot.get("spot_sell_count"),
            "spot_notional_sum_usd": whale_snapshot.get("spot_notional_sum_usd"),
            "spot_whale_delta_notional": whale_snapshot.get("spot_whale_delta_notional"),
            "spot_whale_qty_total": float(whale_snapshot.get("spot_whale_qty_buy") or 0.0)
            + float(whale_snapshot.get("spot_whale_qty_sell") or 0.0),
            "spot_max_single_trade_notional": normalize_optional_positive(
                whale_snapshot.get("spot_max_single_trade_notional")
            ),
        }
        check_fields(
            failures,
            ts,
            "whale_trades.feature_1m",
            whale_actual,
            whale_expected,
            list(whale_expected.keys()),
        )

    return (
        {
            "ts": ts,
            "snapshot_count": len(snapshots),
            "snapshot_codes_ok": sorted(snapshots.keys()) == EXPECTED_CODES,
            "checks_run": EXACT_CHECK_NAMES,
        },
        failures,
    )


def main() -> int:
    args = parse_args()
    dsn = load_dsn(args.config, args.dsn)
    candidates = fetch_candidate_timestamps(
        dsn,
        args.symbol,
        args.recent_hours,
        args.max_candidates,
        args.since_ts,
        args.created_since_ts,
    )
    if not candidates:
        print("No candidate timestamps found.", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)
    sample_count = min(args.samples, len(candidates))
    sample_ts = sorted(rng.sample(candidates, sample_count))

    per_sample: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for ts in sample_ts:
        sample_info, sample_failures = validate_sample(dsn, args.symbol, ts)
        per_sample.append(sample_info)
        failures.extend(sample_failures)

    by_check: dict[str, int] = {}
    for failure in failures:
        by_check[failure["check"]] = by_check.get(failure["check"], 0) + 1

    summary = {
        "symbol": args.symbol,
        "sample_count": sample_count,
        "candidate_count": len(candidates),
        "seed": args.seed,
        "expected_indicator_codes": EXPECTED_CODES,
        "samples": sample_ts,
        "per_sample": per_sample,
        "failure_count": len(failures),
        "failures_by_check": by_check,
        "failures": failures,
    }

    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))

    if args.output_json:
        Path(args.output_json).write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n"
        )

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
