#!/usr/bin/env bash
set -Eeuo pipefail 2>/dev/null || set -Eeuo

MVA_HOME="${MVA_HOME:-$HOME/mva}"
cd "$MVA_HOME"

LOG_DIR="$MVA_HOME/logs"
TMP_DIR="$MVA_HOME/tmp"
VENV_DIR="$MVA_HOME/.venv"

mkdir -p "$LOG_DIR" "$TMP_DIR"

command -v dos2unix >/dev/null 2>&1 && [[ -f "$MVA_HOME/.env" ]] && dos2unix -q "$MVA_HOME/.env" || true

if [[ -f "$MVA_HOME/.env" ]]; then
  set -a
  . "$MVA_HOME/.env"
  set +a
fi

ensure_venv() {
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    echo "❌ Virtual environment not found at $VENV_DIR"
    echo "Run: ~/mva/setup.sh"
    exit 1
  fi
}

declare -A ENTRY=(
  [ingestion]="$MVA_HOME/pi/services/ingestion/app.py"
  [storage]="$MVA_HOME/pi/services/storage/app.py"
  [inference]="$MVA_HOME/pi/services/inference/app.py"
  [event_processing]="$MVA_HOME/pi/services/event_processing/app.py"
  [notification]="$MVA_HOME/pi/services/notification/app.py"
)

SERVICES=(ingestion storage inference event_processing notification)

check_service_file() {
  local service="$1"
  local entry="${ENTRY[$service]}"
  if [[ ! -f "$entry" ]]; then
    echo "❌ Missing file for $service: $entry"
    return 1
  fi
}

start_one() {
  local service="$1"
  local entry="${ENTRY[$service]}"
  local log="$LOG_DIR/${service}.log"
  local pid="$TMP_DIR/${service}.pid"

  check_service_file "$service" || return 1

  if [[ -f "$pid" ]] && kill -0 "$(cat "$pid")" 2>/dev/null; then
    echo "ℹ️  $service already running (pid $(cat "$pid"))"
    return 0
  fi

  echo "▶️  starting $service ..."
  nohup "$VENV_DIR/bin/python" "$entry" >/dev/null 2>&1 &
  echo $! > "$pid"
  sleep 0.5

  if [[ -f "$pid" ]] && kill -0 "$(cat "$pid")" 2>/dev/null; then
    echo "✅ $service up (pid $(cat "$pid"))"
  else
    echo "❌ $service failed to start. Check: $log"
    rm -f "$pid"
    return 1
  fi
}

stop_one() {
  local service="$1"
  local pid="$TMP_DIR/${service}.pid"

  if [[ ! -f "$pid" ]]; then
    echo "ℹ️  $service not running"
    return 0
  fi

  local p
  p="$(cat "$pid")"

  if kill -0 "$p" 2>/dev/null; then
    echo "⏹  stopping $service (pid $p) ..."
    kill "$p" 2>/dev/null || true
    sleep 1
    if kill -0 "$p" 2>/dev/null; then
      kill -9 "$p" 2>/dev/null || true
    fi
  fi

  rm -f "$pid"
  echo "✅ $service stopped"
}

status_one() {
  local service="$1"
  local pid="$TMP_DIR/${service}.pid"
  if [[ -f "$pid" ]] && kill -0 "$(cat "$pid")" 2>/dev/null; then
    echo "✅ $service (pid $(cat "$pid"))"
  else
    echo "❌ $service (stopped)"
  fi
}

tail_one() {
  local service="$1"
  local log="$LOG_DIR/${service}.log"
  touch "$log"
  tail -n 80 -f "$log"
}

check_broker() {
  local host="${MQTT_HOST:-127.0.0.1}"
  local port="${MQTT_PORT:-1883}"

  if command -v timeout >/dev/null 2>&1; then
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/$host/$port" 2>/dev/null; then
      echo "✅ MQTT broker reachable at $host:$port"
      return 0
    else
      echo "❌ MQTT broker not reachable at $host:$port"
      return 1
    fi
  else
    echo "ℹ️  timeout not installed; skipping broker reachability test"
    return 0
  fi
}

check_db() {
  "$VENV_DIR/bin/python" - <<'PY'
import os, sqlite3, sys
db = os.path.expanduser("~/mva/data/mva.db")
os.makedirs(os.path.dirname(db), exist_ok=True)
con = sqlite3.connect(db)
mode = con.execute("PRAGMA journal_mode;").fetchone()[0]
qc = con.execute("PRAGMA quick_check;").fetchone()[0]
con.close()
print(f"DB: {db}  journal_mode={mode}  quick_check={qc}")
sys.exit(0 if qc == "ok" else 1)
PY
}

start_all() {
  ensure_venv
  check_broker || true
  start_one storage
  start_one ingestion
  start_one inference
  start_one event_processing
  start_one notification
  echo "🎯 All services started."
}

stop_all() {
  stop_one notification
  stop_one event_processing
  stop_one inference
  stop_one ingestion
  stop_one storage
  echo "🛑 All services stopped."
}

restart_all() {
  stop_all
  start_all
}

status_all() {
  for s in "${SERVICES[@]}"; do
    status_one "$s"
  done
}

health_all() {
  echo "Broker:"
  check_broker || true
  echo
  echo "Database:"
  check_db || true
  echo
  echo "Services:"
  status_all
}

logs_list() {
  ls -lah "$LOG_DIR"
}

clean_logs() {
  rm -f "$LOG_DIR"/*.log
  echo "✅ logs cleared"
}

usage() {
  cat <<USAGE
Usage:
  $0 start
  $0 stop
  $0 restart
  $0 status
  $0 health
  $0 start-one <service>
  $0 stop-one <service>
  $0 tail <service>
  $0 logs
  $0 logs --clean

Services:
  ingestion
  storage
  inference
  event_processing
  notification
USAGE
}

cmd="${1:-}"
case "$cmd" in
  start) start_all ;;
  stop) stop_all ;;
  restart) restart_all ;;
  status) status_all ;;
  health) health_all ;;
  start-one)
    shift
    ensure_venv
    start_one "${1:?service required}"
    ;;
  stop-one)
    shift
    stop_one "${1:?service required}"
    ;;
  tail)
    shift
    tail_one "${1:?service required}"
    ;;
  logs)
    shift || true
    if [[ "${1:-}" == "--clean" ]]; then
      clean_logs
    else
      logs_list
    fi
    ;;
  *)
    usage
    exit 1
    ;;
esac
