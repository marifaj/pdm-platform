cat > ~/mva/run_all_bg.sh <<'SH'
#!/usr/bin/env bash
# Safe mode with a fallback if pipefail isn't supported
set -Eeuo pipefail 2>/dev/null || set -Eeuo

MVA_HOME="${MVA_HOME:-$HOME/mva}"
cd "$MVA_HOME"

LOG_DIR="$MVA_HOME/logs"
TMP_DIR="$MVA_HOME/tmp"
VENV_DIR="$MVA_HOME/.venv"
REQ="$MVA_HOME/requirements.txt"
mkdir -p "$LOG_DIR" "$TMP_DIR" storage/schema storage/config models data

# Normalize .env if present
command -v dos2unix >/dev/null 2>&1 && dos2unix -q .env || true

# Auto-load .env
if [[ -f "$MVA_HOME/.env" ]]; then
  set -a
  . "$MVA_HOME/.env"
  set +a
fi

ensure_venv() {
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    echo "🔧 Creating venv at $VENV_DIR ..."
    python3 -m venv "$VENV_DIR"
  fi
  . "$VENV_DIR/bin/activate"
  python -m pip install --upgrade pip >/dev/null
  [[ -f "$REQ" ]] && python -m pip install -r "$REQ" >/dev/null
}

start_one() { # name entrypoint
  local name="$1"; shift
  local entry="$1"; shift || true
  local log="$LOG_DIR/${name}.log"
  local pid="$TMP_DIR/${name}.pid"

  if [[ -f "$pid" ]] && kill -0 "$(cat "$pid")" 2>/dev/null; then
    echo "ℹ️  $name already running (pid $(cat "$pid"))."
    return
  fi

  echo "▶️  starting $name ..."
  nohup "$VENV_DIR/bin/python" "$entry" "$@" >>"$log" 2>&1 &
  echo $! > "$pid"
  sleep 0.3
  if kill -0 "$(cat "$pid")" 2>/dev/null; then
    echo "✅ $name up (log: $log)"
  else
    echo "❌ $name failed (see: $log)"
    rm -f "$pid" || true
  fi
}

stop_one() { # name
  local name="$1"
  local pid="$TMP_DIR/${name}.pid"
  if [[ -f "$pid" ]]; then
    local p
    p="$(cat "$pid")"
    if kill -0 "$p" 2>/dev/null; then
      echo "⏹  stopping $name (pid $p)..."
      kill "$p" 2>/dev/null || true
      sleep 0.7
      kill -9 "$p" 2>/dev/null || true
    fi
    rm -f "$pid"
  else
    echo "ℹ️  $name not running."
  fi
}

status_one() { # name
  local name="$1"
  local pid="$TMP_DIR/${name}.pid"
  if [[ -f "$pid" ]] && kill -0 "$(cat "$pid")" 2>/dev/null; then
    echo "✅ $name (pid $(cat "$pid"))"
  else
    echo "❌ $name (stopped)"
  fi
}

tail_one() { local name="$1"; tail -n 80 -f "$LOG_DIR/${name}.log"; }

check_broker() {
  local host="${MQTT_HOST:-127.0.0.1}"
  local port="${MQTT_PORT:-1883}"
  if command -v timeout >/dev/null 2>&1; then
    if ! timeout 1 bash -c "cat < /dev/null > /dev/tcp/$host/$port" 2>/dev/null; then
      echo "⚠️  MQTT broker $host:$port not reachable."
      return 1
    fi
  else
    echo "ℹ️  Skipping broker reachability check (no timeout)."
  fi
}

check_db() {
  ensure_venv
  "$VENV_DIR/bin/python" - "$MVA_HOME" <<'PY'
import os, sqlite3, sys
home = sys.argv[1]
db = os.path.join(home, "data", "mva.db")
os.makedirs(os.path.dirname(db), exist_ok=True)
con = sqlite3.connect(db)
cur = con.execute("PRAGMA journal_mode;"); mode = cur.fetchone()[0]
cur = con.execute("PRAGMA quick_check;"); qc = cur.fetchone()[0]
con.close()
print(f"DB: {db}  journal_mode={mode}  quick_check={qc}")
sys.exit(0 if qc=="ok" else 1)
PY
}

# Consistent names = file names
declare -A ENTRY=(
  [data_storage]="storage/data_storage.py"
  [data_entry]="data_entry.py"
  [inference]="inference.py"
  [event_processing]="event_processing.py"
  [notification]="notification.py"
)

start_all() {
  ensure_venv
  check_broker || true
  start_one data_storage      "${ENTRY[data_storage]}"
  start_one data_entry        "${ENTRY[data_entry]}"
  start_one inference         "${ENTRY[inference]}"
  start_one event_processing  "${ENTRY[event_processing]}"
  start_one notification      "${ENTRY[notification]}"
  echo "🎯 All services started."
}

stop_all() {
  stop_one notification
  stop_one event_processing
  stop_one inference
  stop_one data_entry
  stop_one data_storage
  echo "🛑 All services stopped."
}

status_all() {
  status_one data_storage
  status_one data_entry
  status_one inference
  status_one event_processing
  status_one notification
}

restart_all() { stop_all; start_all; }

health_all() {
  echo "• Broker:"; if check_broker; then echo "  ✅ reachable"; else echo "  ❌ NOT reachable"; fi
  echo "• DB:";     if check_db; then echo "  ✅ ok";        else echo "  ❌ issues (see above)"; fi
  echo "• Services:"; status_all
}

clean_logs() { echo "🧹 cleaning logs..."; rm -f "$LOG_DIR"/*.log; echo "✅ logs cleared."; }

usage() {
  cat <<USAGE
Usage: $0 {start|stop|status|restart|health|tail <service>|logs [--clean]}
Services: data_storage | data_entry | inference | event_processing | notification
USAGE
}

cmd="${1:-}"
case "$cmd" in
  start)    start_all ;;
  stop)     stop_all ;;
  status)   status_all ;;
  restart)  restart_all ;;
  health)   health_all ;;
  tail)     shift; tail_one "${1:-data_entry}" ;;
  logs)     shift; [[ "${1:-}" == "--clean" ]] && clean_logs || ls -la "$LOG_DIR" ;;
  *)        usage; exit 1 ;;
esac
SH
