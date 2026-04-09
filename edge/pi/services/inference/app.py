#!/usr/bin/env python3
"""
Inference Service — temp + vibration only (test version)

Input payload expected from ingestion (flat normalized JSON):
{
  "factory_id": "factory-001",
  "machine_id": "machine-001",
  "device_id": "esp32-001",
  "reading_index": 2507,
  "ts_gateway": "2026-04-09T09:20:00.000Z",
  "temperature_c": 23.56,
  "vibration_mag_g": 0.907
}

Behavior:
- If ~/mva/models/iforest-temp-vibration-v1.onnx exists and onnxruntime is installed,
  run ONNX inference first.
- Else fallback to EWMA + z-score heuristic.
- Publishes prediction to: mva/gateway/{gateway_id}/pred/{device_id}

This is a TEST service. Replace the ONNX file later with the real trained one.
"""
import os, json, time, signal, sys, math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import paho.mqtt.client as mqtt

# ===== env auto-load (.env) =====
try:
    from dotenv import load_dotenv
    env_path = Path.home() / "mva" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except Exception:
    pass

# ===== config =====
GATEWAY_ID   = os.getenv("GATEWAY_ID", "gw-01")
MQTT_HOST    = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT    = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER    = os.getenv("MQTT_USER", "")
MQTT_PASS    = os.getenv("MQTT_PASS", "")

# Change this if your ingestion publishes somewhere else
INGRESS_TPL  = os.getenv("INFERENCE_INPUT_TOPIC", f"mva/gateway/{GATEWAY_ID}/ingestion/normalized")
PRED_TPL     = os.getenv("INFERENCE_OUTPUT_TOPIC", f"mva/gateway/{GATEWAY_ID}/pred/{{device_id}}")

MODEL_ID     = "iforest-temp-vibration-v1"
MODEL_VER    = "0.1.0-test"
LOG_PATH     = os.path.expanduser("~/mva/logs/inference.log")
MODEL_PATH   = Path.home() / "mva" / "models" / "iforest-temp-vibration-v1.onnx"

# Fallback EWMA params
ALPHA        = float(os.getenv("EWMA_ALPHA", "0.10"))
MIN_SAMPLES  = int(os.getenv("EWMA_MIN_SAMPLES", "20"))
HINT_THRESH  = float(os.getenv("ANOMALY_HINT_THRESHOLD", "0.80"))

FEATS = ("temperature_c", "vibration_mag_g")

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def log(line):
    s = f"{iso_now()} inference {line}"
    print(s, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(s + "\n")
    except Exception:
        pass

# ===== optional ONNX runtime =====
use_onnx = False
sess = None
input_name = None
output_names = []

if MODEL_PATH.exists():
    try:
        import onnxruntime as ort
        sess = ort.InferenceSession(str(MODEL_PATH), providers=["CPUExecutionProvider"])
        input_name = sess.get_inputs()[0].name
        output_names = [o.name for o in sess.get_outputs()]
        use_onnx = True
        log(f"[BOOT] ONNX model loaded: {MODEL_PATH}")
        log(f"[BOOT] ONNX input={input_name} outputs={output_names}")
    except Exception as e:
        log(f"[WARN] ONNX init failed, using EWMA fallback only: {e}")
else:
    log(f"[BOOT] No ONNX file at {MODEL_PATH}; using EWMA fallback only")

# ===== EWMA fallback state =====
class EWMAState:
    def __init__(self):
        self.n = 0
        self.mean = {k: None for k in FEATS}
        self.var = {k: 0.0 for k in FEATS}

state = defaultdict(EWMAState)

def update_ewma(s: EWMAState, sample: dict):
    for k in FEATS:
        x = sample.get(k)
        if x is None:
            continue
        x = float(x)
        if s.mean[k] is None:
            s.mean[k] = x
            s.var[k] = 0.0
        else:
            s.mean[k] = ALPHA * x + (1 - ALPHA) * s.mean[k]
            s.var[k] = ALPHA * (x - s.mean[k]) ** 2 + (1 - ALPHA) * s.var[k]
    s.n += 1

def zscore(x: float, mu, var: float) -> float:
    if mu is None:
        return 0.0
    sd = math.sqrt(max(var, 1e-9))
    return abs((x - mu) / sd)

def score_ewma(s: EWMAState, sample: dict) -> float:
    # More weight on vibration, less on temperature
    parts = []
    weights = {
        "temperature_c": 0.35,
        "vibration_mag_g": 0.65,
    }
    for k in FEATS:
        v = sample.get(k)
        if v is None or s.mean[k] is None:
            continue
        z = zscore(float(v), s.mean[k], s.var[k])
        parts.append(min(10.0, z) * weights[k])

    if not parts:
        return 0.0

    raw = sum(parts)
    score = 1.0 / (1.0 + math.exp(-(raw - 2.0)))

    # warm-up damping
    warm = min(1.0, max(0.0, s.n / float(MIN_SAMPLES)))
    score *= warm
    return max(0.0, min(1.0, score))

def build_feature_vector(d: dict) -> np.ndarray:
    vals = []
    for k in FEATS:
        v = d.get(k, np.nan)
        vals.append(float(v) if v is not None else np.nan)
    return np.array([vals], dtype=np.float32)

def run_onnx_score(x: np.ndarray) -> float:
    """
    Expected exported ONNX behavior:
    - Input shape: [1, 2] float32
    - Output[0] contains decision_function or anomaly score-like value

    For test-time robustness:
    - If output is already in [0,1], use it directly.
    - Otherwise assume it's a decision_function where lower = more anomalous,
      and map to anomaly_score via sigmoid(-value).
    """
    y = sess.run(output_names, {input_name: x})

    # Prefer first output
    first = y[0]
    val = float(first[0]) if np.ndim(first) == 1 else float(first[0][0])

    if 0.0 <= val <= 1.0:
        score = val
    else:
        score = 1.0 / (1.0 + math.exp(val))

    return max(0.0, min(1.0, score))

# ===== MQTT handlers =====
def on_connect(client, userdata, flags, rc, props=None):
    log(f"[MQTT] connected rc={rc}; sub {INGRESS_TPL}")
    client.subscribe(INGRESS_TPL, qos=0)

def on_disconnect(client, userdata, rc, props=None):
    log(f"[MQTT] disconnected rc={rc}")

def on_message(client, userdata, msg):
    t0 = time.perf_counter()

    try:
        d = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception as e:
        log(f"[WARN] invalid JSON: {e}")
        return

    device_id = d.get("device_id", "unknown")
    ewma_state = state[device_id]

    sample = {
        "temperature_c": d.get("temperature_c"),
        "vibration_mag_g": d.get("vibration_mag_g"),
    }

    x = build_feature_vector(sample)

    model_mode = "ewma"
    score = None

    if use_onnx and not np.isnan(x).any():
        try:
            score = run_onnx_score(x)
            model_mode = "onnx"
        except Exception as e:
            log(f"[WARN] ONNX inference failed for device={device_id}; fallback to EWMA: {e}")

    if score is None:
        update_ewma(ewma_state, sample)
        score = score_ewma(ewma_state, sample)

    out = {
        "factory_id": d.get("factory_id"),
        "machine_id": d.get("machine_id"),
        "device_id": device_id,
        "reading_index": d.get("reading_index"),
        "ts_gateway": d.get("ts_gateway"),
        "ts_inference": iso_now(),
        "model_id": MODEL_ID if model_mode == "onnx" else "ewma-temp-vibration-v1",
        "model_ver": MODEL_VER if model_mode == "onnx" else "0.1.0-test",
        "mode": model_mode,
        "features_used": {
            "temperature_c": sample["temperature_c"],
            "vibration_mag_g": sample["vibration_mag_g"],
        },
        "anomaly_score": round(float(score), 4),
        "is_anomaly": bool(score >= HINT_THRESH),
        "inference_ms": int((time.perf_counter() - t0) * 1000),
    }

    topic = PRED_TPL.format(device_id=device_id)
    client.publish(topic, json.dumps(out, separators=(",", ":")), qos=0, retain=False)
    log(f"[PUB] mode={model_mode} dev={device_id} score={out['anomaly_score']} topic={topic}")

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
    client.on_disconnect = on_disconnect
    client.on_message = on_message

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
