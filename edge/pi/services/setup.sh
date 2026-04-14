#!/usr/bin/env bash
set -Eeuo pipefail 2>/dev/null || set -Eeuo

MVA_HOME="${MVA_HOME:-$HOME/mva}"
cd "$MVA_HOME"

echo "==> Preparing folders..."
mkdir -p   "$MVA_HOME/logs"   "$MVA_HOME/tmp"   "$MVA_HOME/data"   "$MVA_HOME/models"   "$MVA_HOME/storage/config"   "$MVA_HOME/pi/services/ingestion"   "$MVA_HOME/pi/services/storage"   "$MVA_HOME/pi/services/inference"   "$MVA_HOME/pi/services/event_processing"   "$MVA_HOME/pi/services/notification"

REQ_FILE="$MVA_HOME/requirements.txt"

if [[ ! -f "$REQ_FILE" ]]; then
  cat > "$REQ_FILE" <<'REQ'
paho-mqtt==2.1.0
python-dotenv==1.0.1
numpy==1.26.4
onnxruntime==1.18.1
scikit-learn==1.3.2
skl2onnx==1.16.0
onnx==1.16.1
REQ
fi

echo "==> Creating virtual environment..."
if [[ ! -x "$MVA_HOME/.venv/bin/python" ]]; then
  python3 -m venv "$MVA_HOME/.venv"
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

# ---- topic overrides (optional) ----
INFERENCE_INPUT_TOPIC=mva/gateway/gw-01/ingestion/normalized
INFERENCE_OUTPUT_TOPIC=mva/gateway/gw-01/pred/{device_id}
EVENT_PROC_INPUT_TOPIC=mva/gateway/gw-01/pred/+
EVENT_PROC_OUTPUT_TOPIC=mva/gateway/gw-01/event/{device_id}
NOTIFY_INPUT_TOPIC=mva/gateway/gw-01/event/+
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
cat > "$SCHEMA_FILE" <<'SQL'
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS telemetry_normalized (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  factory_id TEXT,
  machine_id TEXT,
  device_id TEXT,
  reading_index INTEGER,
  ts_gateway TEXT,
  temperature_c REAL,
  raw_x REAL,
  raw_y REAL,
  raw_z REAL,
  x_g REAL,
  y_g REAL,
  z_g REAL,
  vibration_mag_g REAL,
  payload_json TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_tel_norm_device_ts
ON telemetry_normalized(device_id, ts_gateway);

CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  msg_id TEXT,
  device_id TEXT,
  ts_utc TEXT,
  severity TEXT,
  rule_id TEXT,
  thresholds_json TEXT,
  meta_json TEXT,
  bucket_date TEXT,
  trace_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_device_ts
ON events(device_id, ts_utc);

CREATE TABLE IF NOT EXISTS incidents (
  incident_id TEXT PRIMARY KEY,
  device_id TEXT,
  status TEXT,
  severity_current TEXT,
  severity_peak TEXT,
  opened_at TEXT,
  last_seen_at TEXT,
  occurrences INTEGER,
  rule_id TEXT,
  cleared_by TEXT,
  cleared_at TEXT,
  trace_id TEXT,
  meta_json TEXT,
  score_min REAL,
  score_max REAL
);

CREATE INDEX IF NOT EXISTS idx_incidents_device
ON incidents(device_id);

CREATE TABLE IF NOT EXISTS notifications (
  notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
  incident_id TEXT,
  channel TEXT,
  status TEXT,
  attempt_count INTEGER,
  delivered_at TEXT,
  provider_msg_id TEXT,
  trace_id TEXT,
  meta_json TEXT
);

CREATE TABLE IF NOT EXISTS storage_migrations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT UNIQUE,
  applied_at TEXT
);
SQL

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
