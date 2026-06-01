#!/usr/bin/env bash
set -Eeuo pipefail 2>/dev/null || set -Eeuo

MVA_HOME="${MVA_HOME:-$HOME/mva}"
cd "$MVA_HOME"

echo "==> Preparing folders..."
mkdir -p   "$MVA_HOME/logs"   "$MVA_HOME/tmp"   "$MVA_HOME/data"   "$MVA_HOME/models"   "$MVA_HOME/storage/config"   "$MVA_HOME/pi/services/ingestion"   "$MVA_HOME/pi/services/storage"   "$MVA_HOME/pi/services/inference"   "$MVA_HOME/pi/services/event_processing"   "$MVA_HOME/pi/services/notification"

REQ_FILE="$MVA_HOME/pi/requirements.txt"
if [[ ! -f "$REQ_FILE" ]]; then
  REQ_FILE="$MVA_HOME/requirements.txt"
fi
echo "==> Using requirements file: $REQ_FILE"

if [[ ! -f "$REQ_FILE" ]]; then
  cat > "$REQ_FILE" <<'REQ'
paho-mqtt==2.1.0
python-dotenv==1.0.1
numpy==1.26.4
pandas==2.2.2
onnxruntime==1.18.1
scikit-learn==1.3.2
skl2onnx==1.16.0
onnx==1.16.1
REQ
fi

echo "==> Creating virtual environment..."
PYTHON_BIN=""
if [[ -n "${MVA_PYTHON_BIN:-}" && -x "$MVA_PYTHON_BIN" ]]; then
  PYTHON_BIN="$MVA_PYTHON_BIN"
elif [[ -x "$HOME/.pyenv/versions/3.12.13/bin/python" ]]; then
  PYTHON_BIN="$HOME/.pyenv/versions/3.12.13/bin/python"
elif command -v python3.12 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3.12)"
elif command -v python3.11 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3.11)"
else
  PYTHON_BIN="$(command -v python3)"
  echo "⚠️  WARNING: Falling back to python3. Python 3.13 may be incompatible with numpy==1.26.4 and onnxruntime==1.18.1."
fi
echo "==> Selected Python: $("$PYTHON_BIN" --version) ($PYTHON_BIN)"
if [[ ! -x "$MVA_HOME/.venv/bin/python" ]]; then
  "$PYTHON_BIN" -m venv "$MVA_HOME/.venv"
fi

. "$MVA_HOME/.venv/bin/activate"
python -m pip install --upgrade pip
python -m pip install -r "$REQ_FILE"

if [[ ! -f "$MVA_HOME/.env" ]]; then
  cat > "$MVA_HOME/.env" <<'ENV'
# ---- core runtime ----
GATEWAY_ID=gw-01
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
MQTT_USER=
MQTT_PASS=
MVA_HOME=$HOME/mva

ENV
fi

cat > "$MVA_HOME/storage/config/thresholds.json" <<'JSON'
{
  "thresholds_by_type": {
    "motor": { "warn": 0.70, "high": 0.80, "crit": 0.90 }
  },
  "hysteresis": { "open_N": 3, "resolve_M": 10, "cooldown_s": 600 }
}
JSON

cat > "$MVA_HOME/storage/config/retention.json" <<'JSON'
{
  "retention": {
    "cap_percent_free": 70,
    "raw_min_hours": 24,
    "events_min_days": 30,
    "pred_keep": false
  },
  "trim_interval_s": 600,
  "vacuum_interval_s": 86400
}
JSON

SCHEMA_FILE="$MVA_HOME/storage/schema.sql"
SERVICE_SCHEMA="$MVA_HOME/pi/services/storage/schema.sql"
if [[ -f "$SERVICE_SCHEMA" ]]; then
  cp "$SERVICE_SCHEMA" "$SCHEMA_FILE"
else
  cat > "$SCHEMA_FILE" <<'SQL'
CREATE TABLE IF NOT EXISTS telemetry_normalized (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_gateway TEXT NOT NULL,
    factory_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    reading_index INTEGER NOT NULL,
    temperature_c REAL,
    raw_x INTEGER,
    raw_y INTEGER,
    raw_z INTEGER,
    x_g REAL,
    y_g REAL,
    z_g REAL,
    vibration_mag_g REAL,
    ts_storage TEXT,
    payload_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_ts ON telemetry_normalized(ts_gateway);
CREATE INDEX IF NOT EXISTS idx_machine ON telemetry_normalized(machine_id);

CREATE TABLE IF NOT EXISTS latency_trace (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT,
  trace_id TEXT,
  deployment_mode TEXT,
  factory_id TEXT,
  machine_id TEXT,
  device_id TEXT,
  reading_index INTEGER,
  window_start_index INTEGER,
  window_end_index INTEGER,
  esp_millis INTEGER,
  ts_ingestion TEXT,
  ts_storage TEXT,
  ts_inference TEXT,
  ts_event TEXT,
  ts_notification TEXT,
  ingestion_to_inference_ms INTEGER,
  inference_to_event_ms INTEGER,
  event_to_notification_ms INTEGER,
  end_to_alert_ms INTEGER,
  ts_publish_client INTEGER,
  ts_ingestion_received INTEGER,
  ts_ingestion_published INTEGER,
  ts_storage_received INTEGER,
  ts_storage_inserted INTEGER,
  ts_inference_received INTEGER,
  ts_inference_start INTEGER,
  ts_inference_end INTEGER,
  ts_prediction_published INTEGER,
  ts_event_received INTEGER,
  ts_event_created INTEGER,
  ts_event_published INTEGER,
  ts_notification_received INTEGER,
  ts_notification_created INTEGER,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_latency_trace_run_device_reading
ON latency_trace(run_id, device_id, reading_index);

CREATE INDEX IF NOT EXISTS idx_latency_trace_trace_id
ON latency_trace(trace_id);
SQL
fi

echo "==> Initializing SQLite schema..."
"$MVA_HOME/.venv/bin/python" - <<'PY'
import os
import sqlite3
from pathlib import Path

home = Path(os.path.expanduser("~/mva"))
db = home / "data" / "mva.db"
schema = home / "storage" / "schema.sql"
db.parent.mkdir(parents=True, exist_ok=True)

con = sqlite3.connect(db)
with open(schema, "r", encoding="utf-8") as f:
    con.executescript(f.read())
con.commit()
con.close()
print(f"SQLite initialized: {db}")
PY

echo "✅ setup complete."
echo "Next:"
echo "  1) put each service app.py in ~/mva/pi/services/<service>/app.py"
echo "  2) start them with ./services.sh start"
