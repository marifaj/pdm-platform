#!/usr/bin/env python3
import json
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

# ---- optional .env auto-load ----
try:
    from dotenv import load_dotenv  # type: ignore
    env_path = Path.home() / "mva" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except Exception:
    pass

# =========================
# CONFIG
# =========================
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("STORAGE_INPUT_TOPIC", "mva/normalized/telemetry")

DB_PATH = os.path.expanduser("~/mva/data/mva.db")
SCHEMA_PATH = os.path.expanduser("~/mva/pi/services/storage/schema.sql")
LOG_PATH = os.path.expanduser("~/mva/logs/data_storage.log")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# =========================
# HELPERS
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def log(msg):
    line = f"{now_iso()} data-storage {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

def connect_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def apply_schema(con):
    if not os.path.exists(SCHEMA_PATH):
        log(f"[WARN] schema file not found: {SCHEMA_PATH}")
        return
    with open(SCHEMA_PATH, "r") as f:
        con.executescript(f.read())
    con.commit()
    log("[BOOT] schema ensured")

def validate_normalized_payload(d):
    required = [
        "factory_id",
        "machine_id",
        "device_id",
        "reading_index",
        "ts_gateway",
        "temperature_c",
        "raw_x",
        "raw_y",
        "raw_z",
        "x_g",
        "y_g",
        "z_g",
        "vibration_mag_g",
    ]
    for k in required:
        if k not in d:
            return False, f"missing field: {k}"
    return True, "ok"

# =========================
# DB
# =========================
con = connect_db()
apply_schema(con)

def insert_telemetry(d):
    con.execute(
        """
        INSERT INTO telemetry_normalized(
            ts_gateway,
            factory_id,
            machine_id,
            device_id,
            reading_index,
            temperature_c,
            raw_x,
            raw_y,
            raw_z,
            x_g,
            y_g,
            z_g,
            vibration_mag_g,
            payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            d["ts_gateway"],
            d["factory_id"],
            d["machine_id"],
            d["device_id"],
            int(d["reading_index"]),
            float(d["temperature_c"]),
            int(d["raw_x"]),
            int(d["raw_y"]),
            int(d["raw_z"]),
            float(d["x_g"]),
            float(d["y_g"]),
            float(d["z_g"]),
            float(d["vibration_mag_g"]),
            json.dumps(d, separators=(",", ":")),
        ),
    )
    con.commit()

# =========================
# MQTT CALLBACKS
# =========================
def on_connect(client, userdata, flags, rc, properties=None):
    log(f"[MQTT] connected rc={rc}")
    client.subscribe(MQTT_TOPIC, qos=1)
    log(f"[MQTT] subscribed to {MQTT_TOPIC}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
        d = json.loads(payload)
    except Exception as e:
        log(f"[WARN] invalid json: {e}")
        return

    ok, why = validate_normalized_payload(d)
    if not ok:
        log(f"[WARN] invalid normalized payload: {why}")
        return

    try:
        insert_telemetry(d)
        log(
            f"[DB] stored "
            f"machine={d['machine_id']} "
            f"reading_index={d['reading_index']} "
            f"temp={d['temperature_c']} "
            f"vibration_mag_g={d['vibration_mag_g']}"
        )
    except Exception as e:
        log(f"[ERROR] db insert failed: {e}")

# =========================
# MAIN
# =========================
def main():
    client = mqtt.Client(
        client_id="data-storage-service",
        protocol=mqtt.MQTTv311,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    def shutdown(signum, frame):
        log("[SYS] shutting down")
        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            con.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log("[BOOT] Data Storage service up")
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()