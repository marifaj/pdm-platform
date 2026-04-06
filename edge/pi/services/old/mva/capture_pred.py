#!/usr/bin/env python3
"""
Prediction Capture — Edge-only MVA
- Sub:  mva/gateway/{gateway_id}/pred/+
- DB:   insert into pred_messages (idempotent by msg_id)
"""

import os, json, sqlite3, signal, sys, time
from datetime import datetime, timezone
from pathlib import Path
import paho.mqtt.client as mqtt

# ----- .env auto-load -----
try:
    from dotenv import load_dotenv  # optional
    env_path = Path.home() / "mva" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except Exception:
    pass

# ----- Config -----
GATEWAY_ID = os.getenv("GATEWAY_ID", "gw-01")
MQTT_HOST  = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT  = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER  = os.getenv("MQTT_USER", "")
MQTT_PASS  = os.getenv("MQTT_PASS", "")
SUB_TOPIC  = f"mva/gateway/{GATEWAY_ID}/pred/+"

DB_PATH    = os.path.expanduser("~/mva/data/mva.db")
LOG_PATH   = os.path.expanduser("~/mva/logs/capture_pred.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def log(s):
    line = f"{iso_now()} capture-pred {s}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ----- DB -----
con = sqlite3.connect(DB_PATH, check_same_thread=False)
con.execute("PRAGMA journal_mode=WAL;")
con.execute("PRAGMA synchronous=NORMAL;")
cur = con.cursor()

def bucket_date(ts_iso):
    # expect "YYYY-MM-DD..." string
    return (ts_iso or iso_now())[:10]

def insert_pred(d: dict):
    # required-ish fields with safe defaults
    msg_id   = d.get("msg_id")
    device   = d.get("device_id")
    gateway  = d.get("gateway_id") or GATEWAY_ID
    trace_id = d.get("trace_id")
    ts_utc   = d.get("ts_utc") or iso_now()
    score    = float(d.get("anomaly_score", 0.0))
    model_id = d.get("model_id") or "unknown"
    model_ver= d.get("model_ver") or "unknown"
    bdate    = bucket_date(ts_utc)

    if not msg_id:
        # don’t insert without msg_id (DB PK)
        return

    try:
        cur.execute("""
            INSERT OR IGNORE INTO pred_messages
            (msg_id, device_id, gateway_id, trace_id, ts_utc, anomaly_score, model_id, model_ver, bucket_date)
            VALUES (?,      ?,         ?,          ?,        ?,      ?,             ?,        ?,         ?)
        """, (msg_id, device, gateway, trace_id, ts_utc, score, model_id, model_ver, bdate))
        con.commit()
    except Exception as e:
        log(f"[ERR] insert pred: {e}")

# ----- MQTT -----
def on_connect(client, u, flags, rc, props=None):
    log(f"[MQTT] Connected rc={rc}; sub {SUB_TOPIC}")
    client.subscribe(SUB_TOPIC, qos=0)

def on_message(client, u, msg):
    try:
        d = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception as e:
        log(f"[WARN] bad json: {e}")
        return
    insert_pred(d)

def on_disconnect(client, u, rc, p=None):
    log(f"[MQTT] Disconnected rc={rc}")

def main():
    client = mqtt.Client(
        client_id=f"capture-pred-{GATEWAY_ID}",
        protocol=mqtt.MQTTv311,
        transport="tcp",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    def shutdown(signum, frame):
        log("[SYS] shutting down")
        try: client.disconnect()
        except Exception: pass
        try: con.close()
        except Exception: pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log(f"[BOOT] gateway={GATEWAY_ID} host={MQTT_HOST}:{MQTT_PORT}")
    client.loop_forever()

if __name__ == "__main__":
    main()
