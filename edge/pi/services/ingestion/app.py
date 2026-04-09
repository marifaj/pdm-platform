#!/usr/bin/env python3
import json
import math
import os
import signal
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

IN_TOPIC = "factory/+/machine/+/telemetry"
OUT_TOPIC = "mva/normalized/telemetry"
QUARANTINE_TOPIC = "mva/quarantine/telemetry"

def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def is_number(x):
    try:
        float(x)
        return True
    except Exception:
        return False

def validate_payload(d):
    required = [
        "factoryId", "machineId", "deviceId", "readingIndex",
        "temperatureC", "rawX", "rawY", "rawZ",
        "x_g", "y_g", "z_g"
    ]

    for k in required:
        if k not in d:
            return False, f"missing field: {k}"

    numeric_fields = [
        "readingIndex", "temperatureC",
        "rawX", "rawY", "rawZ",
        "x_g", "y_g", "z_g"
    ]

    for k in numeric_fields:
        if not is_number(d[k]):
            return False, f"non-numeric field: {k}"

    temp = float(d["temperatureC"])
    if temp < -40 or temp > 150:
        return False, f"temperature out of range: {temp}"

    return True, "ok"

def normalize_payload(d):
    xg = float(d["x_g"])
    yg = float(d["y_g"])
    zg = float(d["z_g"])

    vibration_mag_g = math.sqrt(xg*xg + yg*yg + zg*zg)

    return {
        "factory_id": d["factoryId"],
        "machine_id": d["machineId"],
        "device_id": d["deviceId"],
        "reading_index": int(d["readingIndex"]),
        "ts_gateway": iso_now(),
        "temperature_c": float(d["temperatureC"]),
        "raw_x": int(d["rawX"]),
        "raw_y": int(d["rawY"]),
        "raw_z": int(d["rawZ"]),
        "x_g": xg,
        "y_g": yg,
        "z_g": zg,
        "vibration_mag_g": vibration_mag_g,
        "payload_json": d
    }

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[MQTT] connected rc={rc}")
    client.subscribe(IN_TOPIC, qos=1)
    print(f"[MQTT] subscribed to {IN_TOPIC}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
        d = json.loads(payload)
    except Exception as e:
        print(f"[WARN] invalid json: {e}")
        client.publish(QUARANTINE_TOPIC, json.dumps({"why": "invalid_json"}), qos=0, retain=False)
        return

    ok, why = validate_payload(d)
    if not ok:
        print(f"[WARN] bad payload: {why}")
        client.publish(QUARANTINE_TOPIC, json.dumps({"why": why, "payload": d}), qos=0, retain=False)
        return

    normalized = normalize_payload(d)

    print("[INGESTION] normalized telemetry:")
    print(json.dumps(normalized, indent=2))

    client.publish(OUT_TOPIC, json.dumps(normalized), qos=0, retain=False)

def main():
    client = mqtt.Client(
        client_id="ingestion-service",
        protocol=mqtt.MQTTv311,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    def shutdown(signum, frame):
        print("[SYS] shutting down")
        client.loop_stop()
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()