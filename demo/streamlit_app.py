from __future__ import annotations

import json
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import streamlit as st

JOURNAL_PATH = Path("/data/systems/llm/journal/llm_trade_journal.jsonl")
DEFAULT_SYMBOL = "ETHUSDT"
LOG_WINDOW = timedelta(days=3)
MAX_DECISIONS = 30

LOG_EVENT_TYPES = {
    "workflow_stage1_response": "Stage1",
    "workflow_stage2a_response": "Stage2A",
    "workflow_stage2b_response": "Stage2B",
    "workflow_stage2c_response": "Stage2C",
}

DECISION_EVENT_TYPES = {
    "workflow_execution_report",
    "workflow_stage2b_add_execution_report",
    "workflow_stage2b_management_execution_report",
    "workflow_stage2c_pending_order_execution_report",
}

DECISION_ORDER = [
    "LONG",
    "SHORT",
    "ADD",
    "REDUCE",
    "CLOSE",
    "MODIFY_TPSL",
    "CANCEL_PENDING",
    "REPLACE_PENDING",
]

REFRESH_OPTIONS = {
    "Pause": 0,
    "1s": 1,
    "3s": 3,
    "5s": 5,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_display_timestamp(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M:%S UTC")


def truncate_text(value: Any, limit: int = 180) -> str:
    if not isinstance(value, str):
        return "-"
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3]}..."


def format_optional_price(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return f"{value:g}"


def format_optional_ratio(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return f"{value:.2f}"


def format_optional_leverage(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}"


def decision_heading(decision: str) -> str:
    normalized = (decision or "UNKNOWN").upper()
    return normalized


def build_telegram_style_text(
    *,
    decision: str,
    symbol: str,
    event_dt: datetime | None,
    entry_price: Any = None,
    leverage: Any = None,
    risk_reward_ratio: Any = None,
    take_profit_1: Any = None,
    take_profit_2: Any = None,
    stop_loss: Any = None,
) -> str:
    time_line = "-"
    if event_dt is not None:
        time_line = event_dt.strftime("%H:%M:%S UTC")
    lines = [
        decision_heading(decision),
        f"Symbol: {symbol}",
        f"Entry: {format_optional_price(entry_price)}",
        f"Leverage: {format_optional_leverage(leverage)}",
        f"RR: {format_optional_ratio(risk_reward_ratio)}",
        f"TP1: {format_optional_price(take_profit_1)}",
        f"TP2: {format_optional_price(take_profit_2)}",
        f"SL: {format_optional_price(stop_loss)}",
        f"Time: {time_line}",
    ]
    return "\n".join(lines)


def extract_stage_summary(event_type: str, parsed_value: Any) -> str:
    if not isinstance(parsed_value, dict):
        return "No parsed_value available."

    if event_type == "workflow_stage1_response":
        parts: list[str] = []
        monitoring_status = parsed_value.get("monitoring_status")
        current_script = parsed_value.get("current_script")
        no_trade_reason = parsed_value.get("no_trade_reason")
        if monitoring_status:
            parts.append(f"monitoring_status={monitoring_status}")
        if current_script:
            parts.append(f"current_script={current_script}")
        if no_trade_reason:
            parts.append(f"reason={truncate_text(no_trade_reason)}")
        return " | ".join(parts) if parts else "Stage1 response captured."

    if event_type == "workflow_stage2a_response":
        parts = []
        if parsed_value.get("stage2_decision"):
            parts.append(f"decision={parsed_value['stage2_decision']}")
        if parsed_value.get("wait_reason"):
            parts.append(f"wait_reason={truncate_text(parsed_value['wait_reason'])}")
        if parsed_value.get("reevaluation_reason"):
            parts.append(
                f"reevaluation_reason={truncate_text(parsed_value['reevaluation_reason'])}"
            )
        return " | ".join(parts) if parts else "Stage2A response captured."

    if event_type == "workflow_stage2b_response":
        parts = []
        if parsed_value.get("stage2b_decision"):
            parts.append(f"decision={parsed_value['stage2b_decision']}")
        action_count = (
            parsed_value.get("position_management_plan", {}).get("actions", [])
            if isinstance(parsed_value.get("position_management_plan"), dict)
            else []
        )
        parts.append(f"actions={len(action_count)}")
        return " | ".join(parts)

    if event_type == "workflow_stage2c_response":
        parts = []
        if parsed_value.get("stage2c_decision"):
            parts.append(f"decision={parsed_value['stage2c_decision']}")
        action_count = (
            parsed_value.get("pending_order_management_plan", {}).get("actions", [])
            if isinstance(parsed_value.get("pending_order_management_plan"), dict)
            else []
        )
        parts.append(f"actions={len(action_count)}")
        return " | ".join(parts)

    return "Response captured."


def model_name_for_decision_event(event_type: str) -> str:
    if event_type == "workflow_execution_report":
        return "workflow_watcher_fast"
    if event_type in {
        "workflow_stage2b_add_execution_report",
        "workflow_stage2b_management_execution_report",
    }:
        return "workflow_stage2b_watcher_fast"
    if event_type == "workflow_stage2c_pending_order_execution_report":
        return "workflow_stage2c_watcher_fast"
    return "workflow"


def normalize_management_decision(report_action: Any, action_type: Any) -> str:
    report_text = str(report_action or "").upper()
    action_text = str(action_type or "").upper()
    if report_text in {"LONG", "SHORT", "ADD", "REDUCE", "CLOSE", "MODIFY_TPSL"}:
        return report_text
    if action_text in {"REDUCE_POSITION", "REDUCE"}:
        return "REDUCE"
    if action_text in {"FLATTEN_POSITION", "EXIT_FULL", "CLOSE"}:
        return "CLOSE"
    if action_text in {"MOVE_STOP", "UPDATE_TAKE_PROFIT"}:
        return "MODIFY_TPSL"
    return "HOLD"


def normalize_pending_decision(action_type: Any) -> str:
    action_text = str(action_type or "").upper()
    if "CANCEL" in action_text:
        return "CANCEL_PENDING"
    if "REPLACE" in action_text:
        return "REPLACE_PENDING"
    return action_text or "PENDING_ORDER"


def extract_log_entry(event: dict[str, Any]) -> dict[str, Any] | None:
    event_type = event.get("event_type")
    if event_type not in LOG_EVENT_TYPES:
        return None

    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None

    event_dt = parse_utc_timestamp(event.get("event_ts")) or parse_utc_timestamp(
        event.get("ts_bucket")
    )
    if event_dt is None:
        return None

    parsed_value = payload.get("parsed_value")
    return {
        "event_type": event_type,
        "stage": LOG_EVENT_TYPES[event_type],
        "symbol": str(event.get("symbol") or "UNKNOWN"),
        "event_dt": event_dt,
        "display_ts": format_display_timestamp(event_dt),
        "trigger": payload.get("trigger") or "-",
        "model_name": payload.get("model_name") or "-",
        "provider": payload.get("provider") or "-",
        "model_id": payload.get("model_id") or "-",
        "latency_ms": payload.get("latency_ms"),
        "raw_response_text": payload.get("raw_response_text") or "",
        "parsed_value": parsed_value,
        "error": payload.get("error"),
        "summary": extract_stage_summary(event_type, parsed_value),
        "raw_event": event,
    }


def extract_decision_entry(event: dict[str, Any]) -> dict[str, Any] | None:
    event_type = event.get("event_type")
    if event_type not in DECISION_EVENT_TYPES:
        return None

    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None

    event_dt = parse_utc_timestamp(event.get("event_ts")) or parse_utc_timestamp(
        event.get("ts_bucket")
    )
    if event_dt is None:
        return None

    symbol = str(event.get("symbol") or "UNKNOWN")
    action = payload.get("action") if isinstance(payload.get("action"), dict) else {}
    report = payload.get("report") if isinstance(payload.get("report"), dict) else {}

    decision = "UNKNOWN"
    entry_price = None
    leverage = None
    risk_reward_ratio = None
    take_profit_1 = None
    take_profit_2 = None
    stop_loss = None

    if event_type in {"workflow_execution_report", "workflow_stage2b_add_execution_report"}:
        decision = str(
            report.get("decision") or report.get("position_side") or action.get("action_type") or "UNKNOWN"
        ).upper()
        entry_price = report.get("maker_entry_price")
        leverage = report.get("leverage")
        risk_reward_ratio = report.get("risk_reward_ratio")
        take_profit_1 = report.get("take_profit")
        take_profit_2 = action.get("take_profit_2")
        stop_loss = report.get("stop_loss")
    elif event_type == "workflow_stage2b_management_execution_report":
        decision = normalize_management_decision(report.get("action"), action.get("action_type"))
        take_profit_1 = action.get("take_profit_1")
        take_profit_2 = action.get("take_profit_2")
        stop_loss = action.get("new_stop_loss")
    elif event_type == "workflow_stage2c_pending_order_execution_report":
        decision = normalize_pending_decision(action.get("action_type"))

    message_text = build_telegram_style_text(
        decision=decision,
        symbol=symbol,
        event_dt=event_dt,
        entry_price=entry_price,
        leverage=leverage,
        risk_reward_ratio=risk_reward_ratio,
        take_profit_1=take_profit_1,
        take_profit_2=take_profit_2,
        stop_loss=stop_loss,
    )

    return {
        "event_type": event_type,
        "decision": decision,
        "symbol": symbol,
        "event_dt": event_dt,
        "display_ts": format_display_timestamp(event_dt),
        "model_name": model_name_for_decision_event(event_type),
        "trigger": payload.get("trigger") or "-",
        "reason": action.get("reason"),
        "message_text": message_text,
        "path_id": payload.get("path_id") or action.get("path_id"),
        "context_key": payload.get("context_key") or action.get("context_key"),
        "raw_event": event,
    }


def initialize_cache() -> None:
    if "journal_cache" in st.session_state:
        return
    st.session_state.journal_cache = {
        "inode": None,
        "offset": 0,
        "log_entries": [],
        "decision_entries": deque(maxlen=MAX_DECISIONS),
        "bad_lines": 0,
        "last_loaded_at": None,
    }


def reset_cache() -> None:
    st.session_state.journal_cache = {
        "inode": None,
        "offset": 0,
        "log_entries": [],
        "decision_entries": deque(maxlen=MAX_DECISIONS),
        "bad_lines": 0,
        "last_loaded_at": None,
    }


def prune_log_entries() -> None:
    cache = st.session_state.journal_cache
    cutoff = utc_now() - LOG_WINDOW
    cache["log_entries"] = [
        item for item in cache["log_entries"] if item["event_dt"] >= cutoff
    ]


def ingest_journal_event(raw_line: str) -> None:
    cache = st.session_state.journal_cache
    try:
        event = json.loads(raw_line)
    except json.JSONDecodeError:
        cache["bad_lines"] += 1
        return

    log_entry = extract_log_entry(event)
    if log_entry is not None:
        cache["log_entries"].append(log_entry)
        return

    decision_entry = extract_decision_entry(event)
    if decision_entry is not None:
        cache["decision_entries"].appendleft(decision_entry)


def load_journal_updates() -> None:
    cache = st.session_state.journal_cache
    if not JOURNAL_PATH.exists():
        return

    stat = JOURNAL_PATH.stat()
    inode = (stat.st_dev, stat.st_ino)
    rotated = cache["inode"] != inode or stat.st_size < cache["offset"]
    if rotated:
        reset_cache()
        cache = st.session_state.journal_cache
        cache["inode"] = inode

    with JOURNAL_PATH.open("r", encoding="utf-8") as handle:
        handle.seek(cache["offset"])
        for raw_line in handle:
            ingest_journal_event(raw_line)
        cache["offset"] = handle.tell()

    cache["inode"] = inode
    cache["last_loaded_at"] = utc_now()
    prune_log_entries()


def sorted_symbols(log_entries: list[dict[str, Any]], decision_entries: list[dict[str, Any]]) -> list[str]:
    symbols = {item["symbol"] for item in log_entries}
    symbols.update(item["symbol"] for item in decision_entries)
    if not symbols:
        return [DEFAULT_SYMBOL]
    return sorted(symbols)


def sorted_decisions(decision_entries: list[dict[str, Any]]) -> list[str]:
    available = {item["decision"] for item in decision_entries}
    ordered = [item for item in DECISION_ORDER if item in available]
    remainder = sorted(available - set(ordered))
    return ordered + remainder


def render_log_entry(entry: dict[str, Any], expanded: bool = False) -> None:
    title = f"{entry['stage']} | {entry['symbol']} | {entry['display_ts']}"
    with st.expander(title, expanded=expanded):
        meta = (
            f"trigger={entry['trigger']} | model={entry['model_name']} | "
            f"provider={entry['provider']} | latency_ms={entry['latency_ms'] or '-'}"
        )
        st.caption(meta)
        st.markdown(f"**Summary**: {entry['summary']}")
        if entry["error"]:
            st.error(f"Provider error: {entry['error']}")
        if entry["raw_response_text"]:
            st.markdown("**Raw response**")
            st.code(entry["raw_response_text"], language="json")
        else:
            st.info("No raw_response_text captured for this event.")
        if entry["parsed_value"] is not None:
            st.markdown("**Parsed value**")
            st.json(entry["parsed_value"], expanded=False)


def render_decision_entry(entry: dict[str, Any]) -> None:
    st.markdown(
        f"**{entry['decision']}**  \n"
        f"{entry['symbol']} | {entry['display_ts']}"
    )
    st.code(entry["message_text"], language="text")
    st.caption(
        f"model={entry['model_name']} | trigger={entry['trigger']} | event={entry['event_type']}"
    )
    details: list[str] = []
    if entry.get("path_id"):
        details.append(f"path_id={entry['path_id']}")
    if entry.get("context_key"):
        details.append(f"context_key={entry['context_key']}")
    if entry.get("reason"):
        details.append(f"reason={truncate_text(entry['reason'], 220)}")
    if details:
        st.caption(" | ".join(details))
    with st.expander("Source event", expanded=False):
        st.json(entry["raw_event"], expanded=False)
    st.divider()


def maybe_autorefresh(refresh_label: str) -> None:
    refresh_seconds = REFRESH_OPTIONS.get(refresh_label, 0)
    if refresh_seconds <= 0:
        return
    time.sleep(refresh_seconds)
    st.rerun()


def main() -> None:
    st.set_page_config(
        page_title="AI Trading Decision Console",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.title("AI Trading Decision Console")
    st.caption(
        "Read-only Streamlit sidecar. It only reads the local LLM journal and does not touch live trading services."
    )
    st.info(
        "Right-side cards are Telegram-style reconstructions built from execution and management events. "
        "They are display-only and are not a guaranteed copy of the exact Telegram message that was sent."
    )

    initialize_cache()
    with st.spinner("Loading journal..."):
        load_journal_updates()

    cache = st.session_state.journal_cache
    log_entries = sorted(
        cache["log_entries"], key=lambda item: item["event_dt"], reverse=True
    )
    decision_entries = list(cache["decision_entries"])

    symbols = sorted_symbols(log_entries, decision_entries)
    default_symbol_index = symbols.index(DEFAULT_SYMBOL) if DEFAULT_SYMBOL in symbols else 0
    decision_options = sorted_decisions(decision_entries)

    top_controls = st.columns([1.15, 1.0, 1.15, 0.8, 0.8])
    with top_controls[0]:
        selected_symbol = st.selectbox("Symbol", symbols, index=default_symbol_index)
    with top_controls[1]:
        selected_stage = st.selectbox(
            "Stage",
            ["All", "Stage1", "Stage2A", "Stage2B", "Stage2C"],
            index=0,
        )
    with top_controls[2]:
        selected_decisions = st.multiselect(
            "Decision Filter",
            decision_options,
            default=decision_options,
        )
    with top_controls[3]:
        visible_logs = st.selectbox("Visible Logs", [5, 10, 15, 20, 30], index=1)
    with top_controls[4]:
        refresh_label = st.selectbox("Auto Refresh", list(REFRESH_OPTIONS), index=2)

    metrics = st.columns(4)
    with metrics[0]:
        st.metric("Logs In Memory", len(log_entries))
    with metrics[1]:
        st.metric("Decision Cards", len(decision_entries))
    with metrics[2]:
        st.metric("Bad Journal Lines", cache["bad_lines"])
    with metrics[3]:
        st.metric("Last Loaded", format_display_timestamp(cache["last_loaded_at"]))

    filtered_logs = [
        item
        for item in log_entries
        if item["symbol"] == selected_symbol
        and (selected_stage == "All" or item["stage"] == selected_stage)
    ]
    filtered_decisions = [
        item
        for item in decision_entries
        if item["symbol"] == selected_symbol
        and (not selected_decisions or item["decision"] in selected_decisions)
    ]

    left_col, right_col = st.columns([7, 5], gap="large")

    with left_col:
        st.subheader("LLM Logs")
        st.caption(
            f"Showing stage1/stage2 responses from {JOURNAL_PATH}. In-memory window: last {LOG_WINDOW.days} days."
        )
        if not filtered_logs:
            st.warning("No matching stage responses were found for the current filters.")
        else:
            for index, entry in enumerate(filtered_logs[:visible_logs]):
                render_log_entry(entry, expanded=index == 0)

    with right_col:
        st.subheader("Telegram-style Decision Feed")
        st.caption(
            f"Read-only reconstructed cards from decision events. In-memory cap: latest {MAX_DECISIONS} entries."
        )
        if not filtered_decisions:
            st.warning("No matching decision cards were found for the current filters.")
        else:
            for entry in filtered_decisions:
                render_decision_entry(entry)

    maybe_autorefresh(refresh_label)


if __name__ == "__main__":
    main()
