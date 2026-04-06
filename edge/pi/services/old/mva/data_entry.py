#!/usr/bin/env python3
"""
Data Entry Service — Edge-only MVA (per spec)

- Subscribes:  mva/gateway/{gateway_id}/ingress/+/raw
- Validates:   schema_ver, msg_id, trace_id, device_id, gateway_id, ts_device, feature_ver; numeric sanity & clamps
- Adds:        ts_utc (gateway time); optional meta.ts_device_valid / meta.ts_drift_ms
- Writes:      raw_messages (idempotent on msg_id)
- Emits:       same topic with stamped ts_utc (internal QoS0, retain off)
- Quarantine:  unknown versions / bad payloads → .../ingress/{device_id}/quarantine
- Storage:     SQLite (WAL), idempotent inserts
"""

import json, os, sqlite3, time, signal, sys, math
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
GATEWAY_ID   = os.getenv("GATEWAY_ID", "gw-01")
MQTT_HOST    = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT    = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER    = os.getenv("MQTT_USER", "")
MQTT_PASS    = os.getenv("MQTT_PASS", "")
TOPIC        = f"mva/gateway/{GATEWAY_ID}/ingress/+/raw"

DB_PATH      = os.path.expanduser("~/mva/data/mva.db")
LOG_PATH     = os.path.expanduser("~/mva/logs/data_entry.log")

# Logging verbosity (set DEBUG_DATA_ENTRY=0 in .env to quiet info logs)
DEBUG = os.getenv("DEBUG_DATA_ENTRY", "1") == "1"

# Version allow-lists
ALLOWED_SCHEMA_VERS  = {"1"}
ALLOWED_FEATURE_VERS = {"v1"}

# Quarantine / safety
MAX_PAYLOAD_BYTES = 16 * 1024  # 16 KB hard cap
CLAMPS = {
    "temp_c":    (-40.0, 125.0),
    "rpm":       (0, 100000),
    "vib_rms":   (0.0, 10.0),
    "power_w":   (0.0, 20000.0),
    "current_a": (0.0, 1000.0),
}

# ======== FS / DB SETUP ========
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

con = sqlite3.connect(DB_PATH, check_same_thread=False)
con.execute("PRAGMA journal_mode=WAL;")
con.execute("PRAGMA synchronous=NORMAL;")
cur = con.cursor()

# ======== HELPERS ========
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def bucket_date(ts_iso: str) -> str:
    # YYYY-MM-DD from ts_utc
    return ts_iso[:10]

def is_finite_number(x) -> bool:
    try:
        v = float(x)
        return math.isfinite(v)
    except Exception:
        return False

def validate_payload(d: dict) -> tuple[bool, str]:
    req_keys = ["schema_ver", "msg_id", "trace_id", "device_id", "gateway_id", "ts_device", "feature_ver", "metrics"]
    for k in req_keys:
        if k not in d:
            return False, f"missing {k}"
    if d["gateway_id"] != GATEWAY_ID:
        return False, f"gateway_mismatch {d['gateway_id']} != {GATEWAY_ID}"
    if d["schema_ver"] not in ALLOWED_SCHEMA_VERS:
        return False, f"unknown_schema_ver {d['schema_ver']}"
    if d["feature_ver"] not in ALLOWED_FEATURE_VERS:
        return False, f"unknown_feature_ver {d['feature_ver']}"

    m = d.get("metrics", {})
    for k, (lo, hi) in CLAMPS.items():
        if k in m:
            if not is_finite_number(m[k]):
                return False, f"metric {k} not finite"
            v = float(m[k])
            if v < lo or v > hi:
                return False, f"metric {k} out_of_range {v} not in [{lo},{hi}]"
    return True, "ok"

def parse_any_iso(ts: str | None):
    if not ts:
        return None
    try:
        # strict Z (seconds)
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        # flexible (handles millis)
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def db_insert_with_retry(params, retries: int = 3, delay: float = 0.05) -> bool:
    for i in range(retries):
        try:
            cur.execute(
                "INSERT OR IGNORE INTO raw_messages("
                "  msg_id, device_id, gateway_id, ts_utc, metrics_json, feature_ver, trace_id, bucket_date"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params,
            )
            con.commit()
            return True
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and i < retries - 1:
                time.sleep(delay)
                continue
            log(f"[ERROR] db insert failed: {e}")
            return False
        except Exception as e:
            log(f"[ERROR] db insert failed: {e}")
            return False
    return False

# ======== LOGGING ========
def log(line: str):
    if not DEBUG and not line.startswith("[ERROR]") and not line.startswith("[WARN]"):
        return
    s = f"{iso_now()} data-entry {line}"
    print(s, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(s + "\n")
    except Exception:
        pass

# ======== MQTT HANDLERS ========
CLIENT_ID = f"data-entry-{GATEWAY_ID}"

def on_connect(client, userdata, flags, rc, properties=None):
    log(f"[MQTT] Connected rc={rc}; sub {TOPIC}")
    client.subscribe(TOPIC, qos=1)  # ESP32 publishes QoS1; we accept 1

def on_message(client, userdata, msg):
    # Size guard
    if len(msg.payload) > MAX_PAYLOAD_BYTES:
        log(f"[WARN] payload_too_large bytes={len(msg.payload)} on {msg.topic}")
        client.publish(
            msg.topic.replace("/raw", "/quarantine"),
            json.dumps({"why": "payload_too_large", "bytes": len(msg.payload)}),
            qos=0, retain=False,
        )
        return

    # Decode JSON
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
        d = json.loads(payload)
    except Exception as e:
        log(f"[WARN] invalid json on {msg.topic}: {e}")
        client.publish(
            msg.topic.replace("/raw", "/quarantine"),
            json.dumps({"why": "invalid_json"}),
            qos=0, retain=False,
        )
        return

    # Loop guard
    if "ts_utc" in d:
        return

    # Validate contract
    ok, why = validate_payload(d)
    if not ok:
        log(f"[WARN] validation failed: {why}")
        client.publish(
            msg.topic.replace("/raw", "/quarantine"),
            json.dumps({"why": why, "device_id": d.get("device_id")}),
            qos=0, retain=False,
        )
        return

    # Stamp authoritative gateway time
    d["ts_utc"] = iso_now()

    # Optional: annotate device time drift
    try:
        tdev = parse_any_iso(d.get("ts_device"))
        tgwy = parse_any_iso(d["ts_utc"])
        valid = tdev is not None and tdev.year >= 2010
        d.setdefault("meta", {})
        d["meta"]["ts_device_valid"] = bool(valid)
        d["meta"]["ts_drift_ms"] = int((tdev - tgwy).total_seconds() * 1000) if (valid and tgwy) else None
    except Exception:
        pass

    # Persist (idempotent on msg_id) with retry-on-lock
    db_insert_with_retry((
        d["msg_id"], d["device_id"], d["gateway_id"], d["ts_utc"],
        json.dumps(d.get("metrics", {}), separators=(",", ":")),
        d.get("feature_ver", ""), d.get("trace_id", ""),
        bucket_date(d["ts_utc"]),
    ))

    # Re-publish to same ingress topic (internal QoS0; retain off)
    out_payload = json.dumps(d, separators=(",", ":"))
    client.publish(msg.topic, out_payload, qos=0, retain=False)

def on_disconnect(client, userdata, rc, properties=None):
    log(f"[MQTT] Disconnected rc={rc}")

# ======== MAIN ========
def main():
    client = mqtt.Client(
        client_id=CLIENT_ID,
        protocol=mqtt.MQTTv311,
        transport="tcp",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        clean_session=True,                 # <<< ensure no persistent session
    )
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

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

    log(f"[BOOT] gateway={GATEWAY_ID} host={MQTT_HOST}:{MQTT_PORT} sub={TOPIC}")
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
