PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS raw_messages(
  msg_id TEXT PRIMARY KEY,
  device_id TEXT,
  gateway_id TEXT,
  ts_utc TEXT,
  metrics_json TEXT,
  feature_ver TEXT,
  trace_id TEXT,
  bucket_date TEXT
);
CREATE INDEX IF NOT EXISTS idx_raw_device_ts ON raw_messages(device_id, ts_utc);

CREATE TABLE IF NOT EXISTS events(
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  msg_id TEXT,
  device_id TEXT,
  ts_utc TEXT,
  severity TEXT,
  rule_id TEXT,
  thresholds_json TEXT,
  meta_json TEXT,
  bucket_date TEXT,
  trace_id  TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_device_ts ON events(device_id, ts_utc);

-- ============================================================
-- Incidents (richer: trace_id, meta_json, score_min/max)
-- ============================================================
CREATE TABLE IF NOT EXISTS incidents(
  incident_id       TEXT PRIMARY KEY,
  device_id         TEXT,
  status            TEXT,
  severity_current  TEXT,
  severity_peak     TEXT,
  opened_at         TEXT,
  last_seen_at      TEXT,
  occurrences       INTEGER,
  rule_id           TEXT,
  cleared_by        TEXT,
  cleared_at        TEXT,
  -- New fields:
  trace_id          TEXT,
  meta_json         TEXT,
  score_min         REAL,
  score_max         REAL
);

CREATE INDEX IF NOT EXISTS idx_incidents_device     ON incidents(device_id);
CREATE INDEX IF NOT EXISTS idx_incidents_opened_at  ON incidents(opened_at);
CREATE INDEX IF NOT EXISTS idx_incidents_last_seen  ON incidents(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_incidents_status     ON incidents(status);
CREATE INDEX IF NOT EXISTS idx_incidents_trace      ON incidents(trace_id);


CREATE TABLE IF NOT EXISTS notifications(
  notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
  incident_id TEXT,
  channel TEXT,
  status TEXT,
  attempt_count INTEGER,
  delivered_at TEXT,
  provider_msg_id TEXT
);

CREATE TABLE IF NOT EXISTS config(
  key TEXT PRIMARY KEY,
  value_json TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS metrics(
  name TEXT,
  value REAL,
  ts_utc TEXT
);

CREATE TABLE IF NOT EXISTS storage_migrations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT UNIQUE,
  applied_at TEXT
);


-- Ground truth captured from RAW topic
CREATE TABLE IF NOT EXISTS ground_truth(
  msg_id     TEXT PRIMARY KEY,
  device_id  TEXT,
  gateway_id TEXT,
  trace_id   TEXT,
  ts_utc     TEXT,
  label      INTEGER,              -- 0/1 from the raw payload
  bucket_date TEXT
);

-- Per-message predictions captured from PRED topic
CREATE TABLE IF NOT EXISTS pred_messages(
  msg_id        TEXT PRIMARY KEY,
  device_id     TEXT,
  gateway_id    TEXT,
  trace_id      TEXT,
  ts_utc        TEXT,
  anomaly_score REAL,
  model_id      TEXT,
  model_ver     TEXT,
  bucket_date   TEXT
);
