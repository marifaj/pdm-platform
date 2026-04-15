CREATE TABLE IF NOT EXISTS inference_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_inference TEXT NOT NULL,
  ts_gateway TEXT NOT NULL,
  factory_id TEXT NOT NULL,
  machine_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  reading_index INTEGER NOT NULL,

  temperature_c REAL,
  x_g REAL,
  y_g REAL,
  z_g REAL,
  vibration_mag_g REAL,

  if_score REAL,
  if_pred INTEGER,
  is_anomaly_ml INTEGER,

  temp_ewma REAL,
  temp_residual REAL,
  temp_zscore REAL,
  is_anomaly_ewma INTEGER,

  is_anomaly_final INTEGER,
  anomaly_reason TEXT,

  payload_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_inference_results_device_ts
ON inference_results(device_id, ts_inference);

CREATE INDEX IF NOT EXISTS idx_inference_results_reading
ON inference_results(reading_index);