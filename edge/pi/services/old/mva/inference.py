#!/usr/bin/env python3
"""
Inference Service — Edge-only MVA
- If ~/mva/models/iforest-v1.onnx exists -> use ONNX (scaler+IF pipeline)
- Else fallback to EWMA+z heuristic
- Sub:  mva/gateway/{gateway_id}/ingress/+/raw
- Pub:  mva/gateway/{gateway_id}/pred/{device_id}
"""
import os, json, time, signal, sys, math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import paho.mqtt.client as mqtt

# ===== env auto-load (.env) =====
try:
    from dotenv import load_dotenv  # optional
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
INGRESS_TPL  = f"mva/gateway/{GATEWAY_ID}/ingress/+/raw"
PRED_TPL     = f"mva/gateway/{GATEWAY_ID}/pred/{{device_id}}"

MODEL_ID     = "iforest-v1"
MODEL_VER    = "1.0.0"
LOG_PATH     = os.path.expanduser("~/mva/logs/inference.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# heuristic params (fallback)
ALPHA        = 0.1
MIN_SAMPLES  = 20
HINT_THRESH  = 0.80
FEATS        = ("vib_rms", "power_w", "temp_c", "rpm")

def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")

def log(line):
    s = f"{iso_now()} inference {line}"
    print(s, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(s + "\n")
    except Exception:
        pass

# ===== ONNX (if available) =====
ONNX_PATH = Path.home() / "mva" / "models" / "iforest-v1.onnx"
use_onnx = False
sess = None
input_name = None
output_name = None

if ONNX_PATH.exists():
    try:
        import onnxruntime as ort  # only needed at runtime
        sess = ort.InferenceSession(str(ONNX_PATH), providers=["CPUExecutionProvider"])
        input_name  = sess.get_inputs()[0].name
        output_name = sess.get_outputs()[0].name
        use_onnx = True
        log(f"[BOOT] ONNX model loaded: {ONNX_PATH.name}")
    except Exception as e:
        log(f"[WARN] failed to init ONNX; fallback to heuristic: {e}")

# ===== heuristic state =====
class EWMAState:
    def __init__(self):
        self.n = 0
        self.mean = {k: None for k in FEATS}
        self.var  = {k: 0.0  for k in FEATS}

state = defaultdict(EWMAState)

def update_ewma(s: EWMAState, m: dict):
    for k in FEATS:
        x = m.get(k)
        if x is None:
            continue
        x = float(x)
        if s.mean[k] is None:
            s.mean[k] = x
            s.var[k]  = 0.0
        else:
            s.mean[k] = ALPHA * x + (1 - ALPHA) * s.mean[k]
            s.var[k]  = ALPHA * (x - s.mean[k])**2 + (1 - ALPHA) * s.var[k]
    s.n += 1

def zscore(x: float, mu: float|None, var: float) -> float:
    if mu is None:
        return 0.0
    sd = math.sqrt(max(var, 1e-9))
    return abs((x - mu)/sd)

def score_heuristic(s: EWMAState, m: dict) -> float:
    parts = []
    for k, w in (("vib_rms",0.5), ("power_w",0.3), ("temp_c",0.1), ("rpm",0.1)):
        v = m.get(k)
        if v is None or s.mean[k] is None:
            continue
        parts.append(min(10.0, zscore(float(v), s.mean[k], s.var[k])) * w)
    if not parts:
        return 0.0
    raw = sum(parts)
    score = 1.0 / (1.0 + math.exp(-(raw - 2.0)))
    # warm-up damping (FIX: use s.n, not state[...] on metrics)
    warm = min(1.0, max(0.0, s.n / float(MIN_SAMPLES)))
    score *= warm
    return max(0.0, min(1.0, score))

# ===== MQTT handlers =====
def on_connect(client, u, flags, rc, props=None):
    log(f"[MQTT] Connected rc={rc}; sub {INGRESS_TPL}")
    client.subscribe(INGRESS_TPL, qos=0)

def on_message(client, u, msg):
    t0 = time.perf_counter()
    try:
        d = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception as e:
        log(f"[WARN] invalid JSON: {e}")
        return

    # Stamp ts_utc if missing (common for ESP32-originated frames)
    if "ts_utc" not in d:
        d["ts_utc"] = iso_now()

    dev     = d.get("device_id", "unknown")
    metrics = d.get("metrics", {})
    s       = state[dev]

    # Feature vector
    x = np.array([[float(metrics.get(k, np.nan)) for k in FEATS]], dtype=np.float32)

    if np.isnan(x).any():
        update_ewma(s, metrics)
        score = score_heuristic(s, metrics)
    else:
        if use_onnx:
            try:
                y = sess.run([output_name], {input_name: x})[0]
                # sklearn IF decision_function: higher=normal, lower=anomaly
                # map to [0..1] anomaly_score: sigmoid(-dfunc) == 1/(1+exp(dfunc))
                dfunc = float(y[0]) if np.ndim(y) == 1 else float(y[0][0])
                score = 1.0 / (1.0 + math.exp(dfunc))
            except Exception as e:
                log(f"[WARN] ONNX inference failed, fallback: {e}")
                update_ewma(s, metrics)
                score = score_heuristic(s, metrics)
        else:
            update_ewma(s, metrics)
            score = score_heuristic(s, metrics)

    out = {
        "schema_ver":  d.get("schema_ver", "1"),
        "trace_id":    d.get("trace_id"),
        "msg_id":      d.get("msg_id"),
        "device_id":   dev,
        "gateway_id":  d.get("gateway_id"),
        "ts_utc":      iso_now(),
        "feature_ver": d.get("feature_ver", "v1"),
        "model_id":    MODEL_ID,
        "model_ver":   MODEL_VER,
        "anomaly_score": round(float(score), 4),
        "is_anomaly_model_hint": bool(score >= HINT_THRESH),
        "inference_ms": int((time.perf_counter() - t0) * 1000),
    }
    client.publish(PRED_TPL.format(device_id=dev),
                   json.dumps(out, separators=(",",":")),
                   qos=0, retain=False)
    log(f"[PUB] pred dev={dev} score={out['anomaly_score']} ms={out['inference_ms']}")

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
