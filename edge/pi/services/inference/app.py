#!/usr/bin/env python3

import json
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt

# ---- optional .env auto-load ----
try:
    from dotenv import load_dotenv  # type: ignore
    env_path = Path.home() / "mva" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except Exception:
    pass


# ============================================================
# CONFIG
# ============================================================
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

INPUT_TOPIC = os.getenv("INFERENCE_INPUT_TOPIC", "mva/normalized/telemetry")
OUTPUT_TOPIC = os.getenv("INFERENCE_OUTPUT_TOPIC", "mva/inference/telemetry")

MODEL_PATH = os.path.expanduser(
    os.getenv("INFERENCE_MODEL_PATH", "~/mva/models/if_model.joblib")
)

# Optional lightweight debug
ENABLE_CONSOLE_LOGGING = False

# ============================================================
# EXPERIMENT SETTINGS
# ============================================================
# Allowed:
#   "ewma_only"
#   "ml_only"
#   "hybrid"
DETECTION_MODE = "hybrid"

ML_RUN_EVERY_N = 1
DISABLE_ML_WHEN_UNUSED = True

FEATURE_COLUMNS = [
    "temperature_c",
    "x_g",
    "y_g",
    "z_g",
    "vibration_mag_g",
]

# ---- EWMA parameters ----
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.10"))
EWMA_WINDOW_SIZE = int(os.getenv("EWMA_WINDOW_SIZE", "200"))
EWMA_Z_THRESHOLD = float(os.getenv("EWMA_Z_THRESHOLD", "3.0"))
EWMA_MIN_STD = float(os.getenv("EWMA_MIN_STD", "0.05"))

# For hybrid mode:
# True  -> anomaly if either ML or EWMA says anomaly
# False -> anomaly only if both say anomaly
HYBRID_USE_OR = True


# ============================================================
# HELPERS
# ============================================================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def now_iso_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def log(msg: str) -> None:
    if ENABLE_CONSOLE_LOGGING:
        print(f"{now_iso()} inference {msg}", flush=True)


# ============================================================
# MODEL LOADING
# ============================================================
def should_load_model() -> bool:
    if DETECTION_MODE == "ewma_only" and DISABLE_ML_WHEN_UNUSED:
        return False
    return True


def load_model():
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Model file not found: {MODEL_PATH}")
    m = joblib.load(MODEL_PATH)
    log(f"[BOOT] model loaded from {MODEL_PATH}")
    return m


model = None
if should_load_model():
    model = load_model()
else:
    log("[BOOT] model loading skipped (EWMA-only mode)")


# ============================================================
# EWMA STATE
# ============================================================
class EWMATemperatureMonitor:
    def __init__(self, alpha: float, window_size: int, z_threshold: float, min_std: float):
        self.alpha = alpha
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.min_std = min_std

        self.ewma = None
        self.residuals = deque(maxlen=window_size)

    def update(self, temp_c: float):
        if self.ewma is None:
            self.ewma = temp_c
            residual = 0.0
            self.residuals.append(residual)
            return self.ewma, residual, 0.0, 0

        self.ewma = self.alpha * temp_c + (1.0 - self.alpha) * self.ewma
        residual = temp_c - self.ewma

        if len(self.residuals) >= 10:
            mean_res = float(np.mean(self.residuals))
            std_res = float(np.std(self.residuals))
            std_res = max(std_res, self.min_std)
            zscore = (residual - mean_res) / std_res
        else:
            zscore = 0.0

        self.residuals.append(residual)
        is_anomaly = 1 if abs(zscore) >= self.z_threshold else 0

        return self.ewma, residual, zscore, is_anomaly


temp_monitor = EWMATemperatureMonitor(
    alpha=EWMA_ALPHA,
    window_size=EWMA_WINDOW_SIZE,
    z_threshold=EWMA_Z_THRESHOLD,
    min_std=EWMA_MIN_STD,
)


# ============================================================
# FEATURE EXTRACTION
# ============================================================
def validate_payload(d: dict):
    required = [
        "factory_id",
        "machine_id",
        "device_id",
        "reading_index",
        "ts_gateway",
        "temperature_c",
        "x_g",
        "y_g",
        "z_g",
        "vibration_mag_g",
    ]
    for k in required:
        if k not in d:
            return False, f"missing field: {k}"
    return True, "ok"


def extract_feature_row(d: dict):
    return {
        "temperature_c": float(d["temperature_c"]),
        "x_g": float(d["x_g"]),
        "y_g": float(d["y_g"]),
        "z_g": float(d["z_g"]),
        "vibration_mag_g": float(d["vibration_mag_g"]),
    }


def make_feature_frame(row: dict) -> pd.DataFrame:
    return pd.DataFrame([row], columns=FEATURE_COLUMNS)


# ============================================================
# INFERENCE LOGIC
# ============================================================
def run_ml_inference(X: pd.DataFrame):
    score = float(model.decision_function(X)[0])  # higher = more normal
    pred = int(model.predict(X)[0])               # 1 = normal, -1 = anomaly
    is_anomaly_ml = 1 if pred == -1 else 0
    return score, pred, is_anomaly_ml


def combine_decision(is_anomaly_ml: int, is_anomaly_ewma: int):
    if DETECTION_MODE == "ewma_only":
        is_final = is_anomaly_ewma
    elif DETECTION_MODE == "ml_only":
        is_final = is_anomaly_ml
    else:
        if HYBRID_USE_OR:
            is_final = 1 if (is_anomaly_ml or is_anomaly_ewma) else 0
        else:
            is_final = 1 if (is_anomaly_ml and is_anomaly_ewma) else 0

    if is_anomaly_ml and is_anomaly_ewma:
        reason = "ml_and_ewma"
    elif is_anomaly_ml:
        reason = "ml_only"
    elif is_anomaly_ewma:
        reason = "ewma_only"
    else:
        reason = "normal"

    return is_final, reason


last_ml_state_by_device = {}


def get_last_ml_state(device_id: str):
    return last_ml_state_by_device.get(device_id, {
        "if_score": 0.0,
        "if_pred": 1,
        "is_anomaly_ml": 0,
    })


def set_last_ml_state(device_id: str, if_score: float, if_pred: int, is_anomaly_ml: int):
    last_ml_state_by_device[device_id] = {
        "if_score": if_score,
        "if_pred": if_pred,
        "is_anomaly_ml": is_anomaly_ml,
    }


# ============================================================
# MQTT CALLBACKS
# ============================================================
def on_connect(client, userdata, flags, rc, properties=None):
    log(f"[MQTT] connected rc={rc}")
    client.subscribe(INPUT_TOPIC, qos=1)
    log(f"[MQTT] subscribed to {INPUT_TOPIC}")


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
        d = json.loads(payload)

        ok, why = validate_payload(d)
        if not ok:
            log(f"[WARN] invalid normalized payload: {why}")
            return

        row = extract_feature_row(d)

        device_id = d["device_id"]
        reading_index = int(d["reading_index"])

        # EWMA every message
        temp_c = row["temperature_c"]
        temp_ewma, temp_residual, temp_zscore, is_anomaly_ewma = temp_monitor.update(temp_c)

        # ML only if needed
        ml_needed_for_mode = not (DETECTION_MODE == "ewma_only" and DISABLE_ML_WHEN_UNUSED)

        if_score = 0.0
        if_pred = 1
        is_anomaly_ml = 0

        if ml_needed_for_mode:
            run_ml_now = (ML_RUN_EVERY_N <= 1) or (reading_index % ML_RUN_EVERY_N == 0)

            if run_ml_now:
                X = make_feature_frame(row)
                if_score, if_pred, is_anomaly_ml = run_ml_inference(X)
                set_last_ml_state(device_id, if_score, if_pred, is_anomaly_ml)
            else:
                ml_state = get_last_ml_state(device_id)
                if_score = float(ml_state["if_score"])
                if_pred = int(ml_state["if_pred"])
                is_anomaly_ml = int(ml_state["is_anomaly_ml"])

        is_anomaly_final, anomaly_reason = combine_decision(
            is_anomaly_ml=is_anomaly_ml,
            is_anomaly_ewma=is_anomaly_ewma,
        )

        result = {
            "ts_inference": now_iso_ms(),
            "factory_id": d["factory_id"],
            "machine_id": d["machine_id"],
            "device_id": device_id,
            "reading_index": reading_index,
            "ts_gateway": d["ts_gateway"],

            "temperature_c": row["temperature_c"],
            "x_g": row["x_g"],
            "y_g": row["y_g"],
            "z_g": row["z_g"],
            "vibration_mag_g": row["vibration_mag_g"],

            "if_score": if_score,
            "if_pred": if_pred,
            "is_anomaly_ml": is_anomaly_ml,

            "temp_ewma": round(float(temp_ewma), 6),
            "temp_residual": round(float(temp_residual), 6),
            "temp_zscore": round(float(temp_zscore), 6),
            "is_anomaly_ewma": is_anomaly_ewma,

            "is_anomaly_final": is_anomaly_final,
            "anomaly_reason": anomaly_reason,
        }

        client.publish(OUTPUT_TOPIC, json.dumps(result), qos=0, retain=False)

    except Exception as e:
        log(f"[ERROR] inference failed: {e}")


# ============================================================
# MAIN
# ============================================================
def main():
    client = mqtt.Client(
        client_id="inference-service",
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
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log("[BOOT] Inference-only service up")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()