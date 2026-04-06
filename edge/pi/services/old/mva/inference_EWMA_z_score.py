#!/usr/bin/env python3
"""
Inference Service — Edge-only MVA (spec-aligned)

- Subscribes: mva/gateway/{gateway_id}/ingress/+/raw   (expects ts_utc present)
- Computes:   anomaly_score (0..1) via simple EWMA + z-score heuristic
- Emits:      mva/gateway/{gateway_id}/pred/{device_id}
- Adds:       model_id, model_ver, inference_ms, is_anomaly_model_hint

Notes:
- Swap this heuristic with ONNX Isolation Forest later without changing I/O contracts.
"""
import os, json, time, signal, sys, math
from collections import defaultdict
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

INGRESS_TPL  = f"mva/gateway/{GATEWAY_ID}/ingress/+/raw"
PRED_TPL     = f"mva/gateway/{GATEWAY_ID}/pred/{{device_id}}"

MODEL_ID     = "iforest-v1"      # placeholder id to match spec shape
MODEL_VER    = "1.0.0"
LOG_PATH     = os.path.expanduser("~/mva/logs/inference.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# Heuristic model params
ALPHA        = 0.1               # EWMA smoothing
MIN_SAMPLES  = 20                # warm-up before confident z-scores
HINT_THRESH  = 0.80              # score above which we hint anomaly

# Per-device state
class EWMAState:
    def __init__(self):
        self.n = 0
        self.mean = {"vib_rms": None, "power_w": None, "temp_c": None, "rpm": None}
        self.var  = {"vib_rms": 0.0,   "power_w": 0.0,   "temp_c": 0.0,   "rpm": 0.0}

state = defaultdict(EWMAState)

def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")

def log(line: str):
    s = f"{iso_now()} inference {line}"
    print(s, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(s + "\n")
    except Exception:
        pass

def update_ewma(s: EWMAState, m: dict):
    # features used (extendable)
    for k in ("vib_rms", "power_w", "temp_c", "rpm"):
        x = m.get(k)
        if x is None:
            continue
        x = float(x)
        if s.mean[k] is None:
            s.mean[k] = x
            s.var[k]  = 0.0
        else:
            prev_mean = s.mean[k]
            s.mean[k] = ALPHA * x + (1 - ALPHA) * s.mean[k]
            # EWMA variance around updated mean
            s.var[k]  = ALPHA * (x - s.mean[k])**2 + (1 - ALPHA) * s.var[k]
    s.n += 1

def zscore(x: float, mu: float | None, var: float) -> float:
    if mu is None:
        return 0.0
    sd = math.sqrt(max(var, 1e-9))
    return abs((x - mu) / sd)

def score_sample(s: EWMAState, m: dict) -> float:
    # Combine z-scores with weights; squash to 0..1
    parts = []
    for k, w in (("vib_rms", 0.5), ("power_w", 0.3), ("temp_c", 0.1), ("rpm", 0.1)):
        v = m.get(k)
        if v is None or s.mean[k] is None:
            continue
        z = zscore(float(v), s.mean[k], s.var[k])
        parts.append(min(10.0, z) * w)  # cap extreme z for stability
    if not parts:
        return 0.0

    raw = sum(parts)  # typical 0..~?
    # Logistic squashing; center around ~2.0
    score = 1.0 / (1.0 + math.exp(-(raw - 2.0)))

    # Warm-up damping based on number of samples for this device
    warm = min(1.0, max(0.0, s.n / float(MIN_SAMPLES)))
    score *= warm

    return max(0.0, min(1.0, score))

# ======== MQTT HANDLERS ========
def on_connect(client, u, flags, rc, props=None):
    log(f"[MQTT] Connected rc={rc}; sub {INGRESS_TPL}")
    # Internal topics QoS0 per spec
    client.subscribe(INGRESS_TPL, qos=0)

def on_message(client, u, msg):
    t0 = time.perf_counter()
    try:
        d = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception as e:
        log(f"[WARN] invalid JSON: {e}")
        return

    # Require gateway stamp from Data Entry (avoid loop on raw device payloads)
    if "ts_utc" not in d:
        return

    dev = d.get("device_id", "unknown")
    metrics = d.get("metrics", {})
    s = state[dev]

    # update state then score
    update_ewma(s, metrics)
    score = score_sample(s, metrics)

    out = {
        "schema_ver":  d.get("schema_ver", "1"),
        "trace_id":    d.get("trace_id"),
        "msg_id":      d.get("msg_id"),
        "device_id":   dev,
        "gateway_id":  d.get("gateway_id"),
        "ts_utc":      iso_now(),  # inference time
        "feature_ver": d.get("feature_ver", "v1"),
        "model_id":    MODEL_ID,
        "model_ver":   MODEL_VER,
        "anomaly_score": round(score, 4),
        "is_anomaly_model_hint": bool(score >= HINT_THRESH),
        "inference_ms": int((time.perf_counter() - t0) * 1000),
    }

    topic = PRED_TPL.format(device_id=dev)
    client.publish(topic, json.dumps(out, separators=(",", ":")), qos=0, retain=False)

def on_disconnect(client, u, rc, p=None):
    log(f"[MQTT] Disconnected rc={rc}")

def main():
    client = mqtt.Client(
        client_id=f"inference-{GATEWAY_ID}",
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
        try:
            client.disconnect()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    client.loop_forever()

if __name__ == "__main__":
    main()
