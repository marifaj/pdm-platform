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
    payload_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_ts ON telemetry_normalized(ts_gateway);
CREATE INDEX IF NOT EXISTS idx_machine ON telemetry_normalized(machine_id);