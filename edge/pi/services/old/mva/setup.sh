cat > ~/mva/setup.sh <<'SH'
#!/bin/bash
set -euo pipefail
MVA="$HOME/mva"
cd "$MVA"

# Folders per Appendix H
mkdir -p logs tmp data storage/schema storage/config models

# Seed requirements.txt if missing
if [[ ! -f requirements.txt ]]; then
  cat > requirements.txt <<'REQ'
paho-mqtt==2.1.0
numpy==1.26.4
scikit-learn==1.3.2
joblib==1.3.2
sqlite-utils==3.36
python-dateutil==2.9.0
python-dotenv==1.0.1
REQ
fi

# Create venv + install
if [[ ! -x .venv/bin/python ]]; then
  python3 -m venv .venv
fi
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Seed .env if missing
if [[ ! -f .env ]]; then
  cat > .env <<'ENV'
# ---- MVA runtime env ----
GATEWAY_ID=gw-01
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
MQTT_USER=
MQTT_PASS=
MVA_HOME=$HOME/mva
ENV
fi

# Storage configs
cat > storage/config/retention.json <<'JSON'
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

cat > storage/config/thresholds.json <<'JSON'
{
  "thresholds_by_type": {
    "motor": { "warn": 0.60, "high": 0.80, "crit": 0.90 }
  },
  "hysteresis": { "open_N": 3, "resolve_M": 10, "cooldown_s": 600 }
}
JSON

# Schema file
cat > storage/schema.sql <<'SQL'
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS raw_messages(
  msg_id TEXT PRIMARY KEY,
  device_id TEXT, gateway_id TEXT, ts_utc TEXT,
  metrics_json TEXT, feature_ver TEXT, trace_id TEXT, bucket_date TEXT
);
CREATE INDEX IF NOT EXISTS idx_raw_device_ts ON raw_messages(device_id, ts_utc);
CREATE TABLE IF NOT EXISTS events(
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  msg_id TEXT, device_id TEXT, ts_utc TEXT, severity TEXT,
  rule_id TEXT, thresholds_json TEXT, meta_json TEXT, bucket_date TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_device_ts ON events(device_id, ts_utc);
CREATE TABLE IF NOT EXISTS incidents(
  incident_id TEXT PRIMARY KEY,
  device_id TEXT, status TEXT, severity_current TEXT, severity_peak TEXT,
  opened_at TEXT, last_seen_at TEXT, occurrences INTEGER,
  rule_id TEXT, cleared_by TEXT, cleared_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_incidents_device ON incidents(device_id);
CREATE TABLE IF NOT EXISTS notifications(
  notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
  incident_id TEXT, channel TEXT, status TEXT, attempt_count INTEGER,
  delivered_at TEXT, provider_msg_id TEXT
);
CREATE TABLE IF NOT EXISTS config(
  key TEXT PRIMARY KEY, value_json TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS metrics(
  name TEXT, value REAL, ts_utc TEXT
);
CREATE TABLE IF NOT EXISTS storage_migrations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT UNIQUE, applied_at TEXT
);
SQL

chmod +x run_all_bg.sh 2>/dev/null || true
echo "✅ setup.sh complete."
SH
chmod +x ~/mva/setup.sh
