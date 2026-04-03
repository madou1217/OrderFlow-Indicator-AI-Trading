#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config/config.yaml}"
RABBITMQ_MGMT_HOST="${RABBITMQ_MGMT_HOST:-127.0.0.1}"
RABBITMQ_MGMT_PORT="${RABBITMQ_MGMT_PORT:-15672}"

SKIP_INSTALL=0
VERIFY_ONLY=0
FORCE_RECREATE=0

usage() {
  cat <<'EOF'
Usage: scripts/init_rabbitmq.sh [options]

Initialize local RabbitMQ to match config/config.yaml, docs/mq_topology.md,
and code-derived dynamic queues.

Options:
  --config PATH        Override config path. Default: config/config.yaml
  --skip-install       Skip apt/systemd/plugin setup and only reconcile topology
  --verify-only        Only verify the current broker topology
  --force-recreate     Delete managed exchanges/queues before redeclaring them
  -h, --help           Show this help message

Environment:
  CONFIG_PATH                Same as --config
  RABBITMQ_MGMT_HOST         RabbitMQ management host. Default: 127.0.0.1
  RABBITMQ_MGMT_PORT         RabbitMQ management port. Default: 15672
  RABBITMQ_ADMIN_USER        Optional management API admin username
  RABBITMQ_ADMIN_PASSWORD    Optional management API admin password

Notes:
  - mq.password_env follows the same rule as the Rust services:
    if an environment variable with that name exists, its value is used;
    otherwise the raw string itself is treated as the password.
  - When mq.user is not guest, the script uses guest/guest for the management
    API by default. Override with RABBITMQ_ADMIN_USER/PASSWORD if your broker
    uses a different admin account.
  - --force-recreate is destructive for the managed RabbitMQ objects.
EOF
}

log() {
  printf '[init_rabbitmq] %s\n' "$*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      [[ $# -ge 2 ]] || { echo "--config requires a path" >&2; exit 1; }
      CONFIG_PATH="$2"
      shift 2
      ;;
    --skip-install)
      SKIP_INSTALL=1
      shift
      ;;
    --verify-only)
      VERIFY_ONLY=1
      shift
      ;;
    --force-recreate)
      FORCE_RECREATE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_cmd python3
require_cmd sudo

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config file not found: $CONFIG_PATH" >&2
  exit 1
fi

if (( FORCE_RECREATE )) && (( VERIFY_ONLY )); then
  echo "--force-recreate cannot be used together with --verify-only" >&2
  exit 1
fi

if (( ! VERIFY_ONLY )) && (( ! SKIP_INSTALL )); then
  log "Installing and starting RabbitMQ with management plugin"
  sudo apt-get update
  sudo apt-get install -y rabbitmq-server
  sudo systemctl enable --now rabbitmq-server
  sudo rabbitmq-plugins enable rabbitmq_management
  sudo rabbitmq-diagnostics await_startup >/dev/null
fi

require_cmd rabbitmqctl
require_cmd rabbitmqadmin

python3 - "$CONFIG_PATH" "$ROOT_DIR" "$RABBITMQ_MGMT_HOST" "$RABBITMQ_MGMT_PORT" "$VERIFY_ONLY" "$FORCE_RECREATE" <<'PY'
import ast
import base64
import json
import os
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import yaml


CONFIG_PATH = sys.argv[1]
ROOT_DIR = sys.argv[2]
MGMT_HOST = sys.argv[3]
MGMT_PORT = sys.argv[4]
VERIFY_ONLY = sys.argv[5] == "1"
FORCE_RECREATE = sys.argv[6] == "1"

LLM_BOOTSTRAP = os.path.join(ROOT_DIR, "systems/llm/src/app/bootstrap.rs")
INGESTOR_BOOTSTRAP = os.path.join(ROOT_DIR, "systems/market_data_ingestor/src/app/bootstrap.rs")


def log(message: str) -> None:
    print(f"[init_rabbitmq] {message}")


def fail(message: str, code: int = 1) -> None:
    print(f"[init_rabbitmq] ERROR: {message}", file=sys.stderr)
    raise SystemExit(code)


def run(cmd: list[str], capture: bool = False, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=capture,
    )
    if check and result.returncode != 0:
        rendered = shlex.join(cmd)
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        details = stderr or stdout or f"exit code {result.returncode}"
        fail(f"command failed: {rendered}\n{details}")
    return result


def http_request(method: str, path: str, username: str, password: str, body: dict | None = None) -> object:
    url = f"http://{MGMT_HOST}:{MGMT_PORT}/api{path}"
    payload = None
    headers = {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    headers["Authorization"] = f"Basic {token}"
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read()
            if not data:
                return None
            return json.loads(data.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        if exc.code in (401, 403):
            fail(
                f"management API auth failed for {method} {path}: {detail}. "
                "Set RABBITMQ_ADMIN_USER and RABBITMQ_ADMIN_PASSWORD to a RabbitMQ admin account."
            )
        fail(f"HTTP {exc.code} for {method} {path}: {detail}")
    except urllib.error.URLError as exc:
        fail(f"cannot reach RabbitMQ management API at {url}: {exc}")


def wait_for_management_api(username: str, password: str) -> None:
    for _ in range(30):
        try:
            url = f"http://{MGMT_HOST}:{MGMT_PORT}/api/overview"
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            request = urllib.request.Request(
                url,
                headers={"Authorization": f"Basic {token}"},
                method="GET",
            )
            with urllib.request.urlopen(request, timeout=5):
                return
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                fail(
                    "management API authentication failed while waiting for readiness. "
                    "Set RABBITMQ_ADMIN_USER and RABBITMQ_ADMIN_PASSWORD to a RabbitMQ admin account."
                )
        except urllib.error.URLError:
            pass
        time.sleep(1)
    fail(
        f"RabbitMQ management API did not become ready at http://{MGMT_HOST}:{MGMT_PORT}. "
        "Check rabbitmq-server and rabbitmq_management."
    )


def parse_literal_constant(path: str, pattern: str) -> str:
    text = open(path, "r", encoding="utf-8").read()
    match = re.search(pattern, text)
    if not match:
        fail(f"could not find constant in {path} matching {pattern!r}")
    return match.group(1)


def eval_int_expr(expr: str) -> int:
    node = ast.parse(expr, mode="eval")

    def visit(current: ast.AST) -> int:
        if isinstance(current, ast.Expression):
            return visit(current.body)
        if isinstance(current, ast.Constant) and isinstance(current.value, int):
            return current.value
        if isinstance(current, ast.UnaryOp) and isinstance(current.op, ast.USub):
            return -visit(current.operand)
        if isinstance(current, ast.BinOp):
            left = visit(current.left)
            right = visit(current.right)
            if isinstance(current.op, ast.Add):
                return left + right
            if isinstance(current.op, ast.Sub):
                return left - right
            if isinstance(current.op, ast.Mult):
                return left * right
            if isinstance(current.op, ast.FloorDiv):
                return left // right
            if isinstance(current.op, ast.Div):
                if right == 0 or left % right != 0:
                    fail(f"unsupported non-integer expression: {expr}")
                return left // right
        fail(f"unsupported expression: {expr}")

    return visit(node)


def get_nested(data: dict, *path: str):
    current = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def resolve_secret(raw: object) -> str:
    value = str(raw)
    return os.environ.get(value, value)


def apply_placeholders(value, symbol: str, symbol_lower: str):
    if isinstance(value, str):
        return value.replace("{symbol}", symbol).replace("{symbol_lower}", symbol_lower)
    if isinstance(value, list):
        return [apply_placeholders(item, symbol, symbol_lower) for item in value]
    if isinstance(value, dict):
        return {key: apply_placeholders(item, symbol, symbol_lower) for key, item in value.items()}
    return value


def is_legacy_indicator_queue_key(queue_key: str) -> bool:
    return queue_key.startswith("indicator_") and not queue_key.startswith("indicator_group_")


def queue_arguments(queue_cfg: dict) -> dict:
    arguments: dict[str, object] = {}
    ttl = queue_cfg.get("message_ttl_ms")
    if ttl is not None:
        arguments["x-message-ttl"] = int(ttl)
    max_length = queue_cfg.get("max_length")
    if max_length is not None:
        arguments["x-max-length"] = int(max_length)
    max_length_bytes = queue_cfg.get("max_length_bytes")
    if max_length_bytes is not None:
        arguments["x-max-length-bytes"] = int(max_length_bytes)
    if max_length is not None or max_length_bytes is not None:
        arguments["x-overflow"] = "drop-head"
    return arguments


def load_plan() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    symbol = None
    for path in (("instrument", "symbol"), ("indicator", "symbol"), ("llm", "symbol")):
        candidate = get_nested(config, *path)
        if candidate and candidate != "{symbol}":
            symbol = str(candidate)
            break
    if not symbol:
        fail("indicator.symbol or instrument.symbol is required in config")
    symbol_lower = symbol.lower()

    mq = apply_placeholders(config["mq"], symbol, symbol_lower)
    exchanges = []
    for key in ("md_live", "md_replay", "ind", "dlx"):
        exchange = mq["exchanges"][key]
        exchanges.append(
            {
                "name": exchange["name"],
                "type": exchange["type"],
                "durable": bool(exchange.get("durable", True)),
                "auto_delete": False,
                "internal": False,
                "arguments": {},
            }
        )

    queues = []
    for queue_key, queue_cfg in mq["queues"].items():
        if is_legacy_indicator_queue_key(queue_key):
            continue
        queues.append(
            {
                "name": queue_cfg["name"],
                "durable": True,
                "auto_delete": False,
                "arguments": queue_arguments(queue_cfg),
                "bindings": [
                    {
                        "source": binding["exchange"],
                        "routing_key": binding["routing_key"],
                        "arguments": {},
                    }
                    for binding in queue_cfg.get("bind", [])
                ],
            }
        )

    llm_queue_key = get_nested(config, "llm", "queue_key")
    if not llm_queue_key:
        fail("llm.queue_key is required in config")
    llm_base_queue_cfg = mq["queues"].get(llm_queue_key)
    if not llm_base_queue_cfg:
        fail(f"llm.queue_key={llm_queue_key!r} does not exist under mq.queues")

    watcher_ttl_expr = parse_literal_constant(
        LLM_BOOTSTRAP,
        r"const\s+WATCHER_FAST_QUEUE_TTL_MS:\s*u\d+\s*=\s*([^;]+);",
    )
    watcher_ttl_ms = eval_int_expr(watcher_ttl_expr)

    selfcheck_queue_name = parse_literal_constant(
        INGESTOR_BOOTSTRAP,
        r'const\s+INGESTOR_SELFCHECK_QUEUE:\s*&str\s*=\s*"([^"]+)";',
    )

    queues.append(
        {
            "name": f"{llm_base_queue_cfg['name']}.watcher_fast.{symbol_lower}",
            "durable": True,
            "auto_delete": False,
            "arguments": {
                "x-message-ttl": watcher_ttl_ms,
                "x-max-length": 4096,
                "x-overflow": "drop-head",
            },
            "bindings": [
                {
                    "source": mq["exchanges"]["md_live"]["name"],
                    "routing_key": f"md.futures.mark_price.{symbol_lower}",
                    "arguments": {},
                },
                {
                    "source": mq["exchanges"]["md_live"]["name"],
                    "routing_key": f"md.agg.futures.trade.1s.{symbol_lower}",
                    "arguments": {},
                },
                {
                    "source": mq["exchanges"]["md_live"]["name"],
                    "routing_key": f"md.futures.kline.1m.{symbol_lower}",
                    "arguments": {},
                },
            ],
        }
    )

    queues.append(
        {
            "name": selfcheck_queue_name,
            "durable": True,
            "auto_delete": False,
            "arguments": {},
            "bindings": [
                {
                    "source": mq["exchanges"]["md_live"]["name"],
                    "routing_key": "md.#",
                    "arguments": {},
                }
            ],
        }
    )

    app_user = str(mq["user"])
    app_password = resolve_secret(mq["password_env"])
    admin_user_env = os.environ.get("RABBITMQ_ADMIN_USER")
    admin_password_env = os.environ.get("RABBITMQ_ADMIN_PASSWORD")
    if bool(admin_user_env) != bool(admin_password_env):
        fail("RABBITMQ_ADMIN_USER and RABBITMQ_ADMIN_PASSWORD must be set together")

    if admin_user_env and admin_password_env:
        admin_user = admin_user_env
        admin_password = admin_password_env
    elif app_user == "guest":
        admin_user = app_user
        admin_password = app_password
    else:
        admin_user = "guest"
        admin_password = "guest"

    return {
        "symbol": symbol,
        "symbol_lower": symbol_lower,
        "mq_host": str(mq["host"]),
        "mq_port": int(mq["port"]),
        "vhost": str(mq["vhost"]),
        "app_user": app_user,
        "app_password": app_password,
        "admin_user": admin_user,
        "admin_password": admin_password,
        "exchanges": exchanges,
        "queues": queues,
    }


def ensure_app_user_and_vhost(plan: dict) -> None:
    vhost = plan["vhost"]
    app_user = plan["app_user"]
    app_password = plan["app_password"]

    vhosts_output = run(["sudo", "rabbitmqctl", "list_vhosts"], capture=True).stdout.splitlines()
    existing_vhosts = {line.strip() for line in vhosts_output[1:] if line.strip()}
    if vhost not in existing_vhosts:
        log(f"Creating vhost {vhost}")
        run(["sudo", "rabbitmqctl", "add_vhost", vhost])
    else:
        log(f"Vhost {vhost} already exists")

    users_output = run(["sudo", "rabbitmqctl", "list_users"], capture=True).stdout.splitlines()
    existing_users = set()
    for line in users_output[1:]:
        if not line.strip():
            continue
        existing_users.add(line.split("\t", 1)[0].strip())

    if app_user in existing_users:
        log(f"Updating password for RabbitMQ user {app_user}")
        run(["sudo", "rabbitmqctl", "change_password", app_user, app_password])
    else:
        log(f"Creating RabbitMQ user {app_user}")
        run(["sudo", "rabbitmqctl", "add_user", app_user, app_password])

    log(f"Granting permissions on {vhost} to {app_user}")
    run(["sudo", "rabbitmqctl", "set_permissions", "-p", vhost, app_user, ".*", ".*", ".*"])


def rabbitmqadmin(plan: dict, *args: str, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess[str]:
    cmd = [
        "rabbitmqadmin",
        "-q",
        "-H",
        MGMT_HOST,
        "-P",
        MGMT_PORT,
        "-u",
        plan["admin_user"],
        "-p",
        plan["admin_password"],
        "-V",
        plan["vhost"],
        *args,
    ]
    return run(cmd, capture=capture, check=check)


def existing_managed_objects(plan: dict) -> tuple[set[str], set[str]]:
    exchanges = http_request(
        "GET",
        f"/exchanges/{urllib.parse.quote(plan['vhost'], safe='')}",
        plan["admin_user"],
        plan["admin_password"],
    )
    queues = http_request(
        "GET",
        f"/queues/{urllib.parse.quote(plan['vhost'], safe='')}",
        plan["admin_user"],
        plan["admin_password"],
    )
    exchange_names = {item["name"] for item in exchanges if item.get("name")}
    queue_names = {item["name"] for item in queues if item.get("name")}
    return exchange_names, queue_names


def force_recreate_managed_topology(plan: dict) -> None:
    exchange_names, queue_names = existing_managed_objects(plan)

    for queue in plan["queues"]:
        if queue["name"] in queue_names:
            log(f"Deleting existing queue {queue['name']}")
            rabbitmqadmin(plan, "delete", "queue", f"name={queue['name']}")

    for exchange in reversed(plan["exchanges"]):
        if exchange["name"] in exchange_names:
            log(f"Deleting existing exchange {exchange['name']}")
            rabbitmqadmin(plan, "delete", "exchange", f"name={exchange['name']}")


def declare_topology(plan: dict) -> None:
    for exchange in plan["exchanges"]:
        log(f"Declaring exchange {exchange['name']}")
        rabbitmqadmin(
            plan,
            "declare",
            "exchange",
            f"name={exchange['name']}",
            f"type={exchange['type']}",
            f"durable={'true' if exchange['durable'] else 'false'}",
        )

    for queue in plan["queues"]:
        log(f"Declaring queue {queue['name']}")
        args = [
            "declare",
            "queue",
            f"name={queue['name']}",
            f"durable={'true' if queue['durable'] else 'false'}",
        ]
        if queue["arguments"]:
            args.append(f"arguments={json.dumps(queue['arguments'], separators=(',', ':'))}")
        try:
            rabbitmqadmin(plan, *args)
        except SystemExit as exc:
            fail(
                f"failed to declare queue {queue['name']}. "
                "If this broker already has the same queue with different arguments, "
                "re-run with --force-recreate after confirming it is safe to drop those objects."
            )

        for binding in queue["bindings"]:
            log(
                f"Binding {queue['name']} <- {binding['source']} ({binding['routing_key']})"
            )
            rabbitmqadmin(
                plan,
                "declare",
                "binding",
                f"source={binding['source']}",
                "destination_type=queue",
                f"destination={queue['name']}",
                f"routing_key={binding['routing_key']}",
            )


def normalize_arguments(arguments: dict) -> dict:
    normalized = dict(arguments or {})
    return normalized


def verify_topology(plan: dict) -> None:
    vhost_enc = urllib.parse.quote(plan["vhost"], safe="")
    exchanges = http_request("GET", f"/exchanges/{vhost_enc}", plan["admin_user"], plan["admin_password"])
    queues = http_request("GET", f"/queues/{vhost_enc}", plan["admin_user"], plan["admin_password"])
    bindings = http_request("GET", f"/bindings/{vhost_enc}", plan["admin_user"], plan["admin_password"])

    exchange_map = {item["name"]: item for item in exchanges if item.get("name")}
    queue_map = {item["name"]: item for item in queues if item.get("name")}
    binding_set = {
        (
            item.get("source", ""),
            item.get("destination", ""),
            item.get("destination_type", ""),
            item.get("routing_key", ""),
            json.dumps(item.get("arguments", {}), sort_keys=True, separators=(",", ":")),
        )
        for item in bindings
    }

    for exchange in plan["exchanges"]:
        actual = exchange_map.get(exchange["name"])
        if not actual:
            fail(f"missing exchange {exchange['name']}")
        if actual.get("type") != exchange["type"]:
            fail(f"exchange {exchange['name']} type mismatch: {actual.get('type')} != {exchange['type']}")
        if bool(actual.get("durable")) != exchange["durable"]:
            fail(f"exchange {exchange['name']} durable mismatch")
        if bool(actual.get("auto_delete")) != exchange["auto_delete"]:
            fail(f"exchange {exchange['name']} auto_delete mismatch")
        if bool(actual.get("internal")) != exchange["internal"]:
            fail(f"exchange {exchange['name']} internal mismatch")

    for queue in plan["queues"]:
        actual = queue_map.get(queue["name"])
        if not actual:
            fail(f"missing queue {queue['name']}")
        if bool(actual.get("durable")) != queue["durable"]:
            fail(f"queue {queue['name']} durable mismatch")
        if bool(actual.get("auto_delete")) != queue["auto_delete"]:
            fail(f"queue {queue['name']} auto_delete mismatch")
        expected_arguments = normalize_arguments(queue["arguments"])
        actual_arguments = normalize_arguments(actual.get("arguments", {}))
        if actual_arguments != expected_arguments:
            fail(
                f"queue {queue['name']} arguments mismatch: "
                f"{json.dumps(actual_arguments, sort_keys=True)} != {json.dumps(expected_arguments, sort_keys=True)}"
            )
        for binding in queue["bindings"]:
            key = (
                binding["source"],
                queue["name"],
                "queue",
                binding["routing_key"],
                json.dumps(binding["arguments"], sort_keys=True, separators=(",", ":")),
            )
            if key not in binding_set:
                fail(
                    f"missing binding for queue {queue['name']}: "
                    f"{binding['source']} -> {binding['routing_key']}"
                )

    log(
        "Verified topology: "
        f"{len(plan['exchanges'])} exchanges, {len(plan['queues'])} queues, "
        f"symbol={plan['symbol']} ({plan['symbol_lower']})"
    )


plan = load_plan()
log(
    "Loaded plan from "
    f"{CONFIG_PATH}: vhost={plan['vhost']}, user={plan['app_user']}, "
    f"mq={plan['mq_host']}:{plan['mq_port']}, symbol={plan['symbol']}"
)

if not VERIFY_ONLY:
    ensure_app_user_and_vhost(plan)

wait_for_management_api(plan["admin_user"], plan["admin_password"])

if FORCE_RECREATE:
    log("Force recreate requested; deleting managed RabbitMQ objects first")
    force_recreate_managed_topology(plan)

if not VERIFY_ONLY:
    declare_topology(plan)

verify_topology(plan)
PY
