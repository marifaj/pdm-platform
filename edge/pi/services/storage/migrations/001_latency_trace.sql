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
