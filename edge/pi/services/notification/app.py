#!/usr/bin/env python3
"""
Notification Service — MVP (Edge-only MVA)

- Subscribes: mva/gateway/{gateway_id}/event/+
- Writes:     notifications table (audit)
- Prints:     human-readable alert line to console/log
- QoS:        0 (internal)
"""

import os, json, sqlite3, time, signal, sys
from datetime import datetime, timezone
from pathlib import Path
import paho.mqtt.client as mqtt

# ======== ENV AUTO-LOAD (.env) ========
try:
    from dotenv import load_dotenv  # type: ignore
    env_path = Path.home() / "mva" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except Exception:
    pass

# ======== CONFIG ========
GATEWAY_ID = os.getenv("GATEWAY_ID", "gw-01")
MQTT_HOST  = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT  = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER  = os.getenv("MQTT_USER", "")
MQTT_PASS  = os.getenv("MQTT_PASS", "")

EVENT_TPL  = f"mva/gateway/{GATEWAY_ID}/event/+"
DB_PATH    = os.path.expanduser("~/mva/data/mva.db")
LOG_PATH   = os.path.expanduser("~/mva/logs/notification.log")

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ======== DB ========
con = sqlite3.connect(DB_PATH, check_same_thread=False)
con.execute("PRAGMA journal_mode=WAL;")
con.execute("PRAGMA synchronous=NORMAL;")
cur = con.cursor()

def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def log(line):
    s = f"{iso_now()} notify {line}"
    print(s, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(s + "\n")
    except Exception:
        pass

def insert_notification_with_retry(values, retries=3, delay=0.05):
    for i in range(retries):
        try:
            cur.execute(
                "INSERT INTO notifications(incident_id, channel, status, attempt_count, delivered_at, provider_msg_id) "
                "VALUES(?,?,?,?,?,?)",
                values
            )
            con.commit()
            return True
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and i < retries - 1:
                time.sleep(delay)
                continue
            log(f"[ERR] insert notification: {e}")
            return False
        except Exception as e:
            log(f"[ERR] insert notification: {e}")
            return False
    return False

# ======== MQTT ========
def on_connect(client, u, flags, rc, p=None):
    log(f"[MQTT] Connected rc={rc}; sub {EVENT_TPL}")
    client.subscribe(EVENT_TPL, qos=0)  # internal QoS0

def on_message(client, u, msg):
    try:
        d = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception as e:
        log(f"[WARN] bad json: {e}")
        return

    dev   = d.get("device_id", "unknown")
    sev   = d.get("severity", "info")
    rule  = d.get("rule_id", "default-001")
    ts    = d.get("ts_utc", iso_now())
    inc   = f"INC-{dev}"   # matches event_processing incident id

    # Audit row
    insert_notification_with_retry(
        (inc, "console", "DELIVERED", 1, ts, None)
    )

    # Human-friendly alert line (carry trace/msg ID for correlation if present)
    tid = d.get("trace_id")
    mid = d.get("msg_id")
    extra = []
    if tid: extra.append(f"trace={tid}")
    if mid: extra.append(f"msg={mid}")
    extra_s = (" " + " ".join(extra)) if extra else ""
    log(f"[ALERT] {sev.upper()} device={dev} rule={rule} at {ts}{extra_s}")

def on_disconnect(client, u, rc, p=None):
    log(f"[MQTT] Disconnected rc={rc}")

def main():
    client = mqtt.Client(
        client_id=f"notify-{GATEWAY_ID}",
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

    client.loop_forever()

if __name__ == "__main__":
    main()
