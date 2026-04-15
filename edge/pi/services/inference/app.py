#!/usr/bin/env python3

import json
import math
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
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

MODEL_PATH = os.path.expanduser(os.getenv("INFERENCE_MODEL_PATH", "~/mva/models/if_model.joblib"))
LOG_PATH = os.path.expanduser("~/mva/logs/inference.log")

FEATURE_COLUMNS = [
    "temperature_c",
    "x_g",
    "y_g",
    "z_g",
    "vibration_mag_g",
]

# ---- EWMA parameters ----
# Lower alpha = smoother EWMA; 0.05 to 0.2 is a reasonable starting range
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.10"))

# How many recent residuals to keep for z-score calculation
EWMA_WINDOW_SIZE = int(os.getenv("EWMA_WINDOW_SIZE", "200"))

# Z-score threshold for temperature anomaly
EWMA_Z_THRESHOLD = float(os.getenv("EWMA_Z_THRESHOLD", "3.0"))

# Minimum residual std to avoid division by zero / unstable tiny std
EWMA_MIN_STD = float(os.getenv("EWMA_MIN_STD", "0.05"))

# If true, final anomaly if either ML or EWMA says anomaly
HYBRID_USE_OR = os.getenv("HYBRID_USE_OR", "true").lower() == "true"


# ============================================================
# LOGGING
# ============================================================
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def log(msg: str) -> None:
    line = f"{now_iso()} inference {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ============================================================
# MODEL LOADING
# ============================================================
def load_model():
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Model file not found: {MODEL_PATH}")
    model = joblib.load(MODEL_PATH)
    log(f"[BOOT] model loaded from {MODEL_PATH}")
    return model


try:
    model = load_model()
except Exception as e:
    log(f"[FATAL] failed to load model: {e}")
    raise


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
        """
        Returns:
            temp_ewma, residual, zscore, is_anomaly_ewma
        """
        if self.ewma is None:
            self.ewma = temp_c
            residual = 0.0
            self.residuals.append(residual)
            return self.ewma, residual, 0.0, 0

        # Update EWMA
        self.ewma = self.alpha * temp_c + (1.0 - self.alpha) * self.ewma
        residual = temp_c - self.ewma

        # Compute z-score based on residual history
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


def extract_features(d: dict):
    try:
        vals = np.array(
            [
                float(d["temperature_c"]),
                float(d["x_g"]),
                float(d["y_g"]),
                float(d["z_g"]),
                float(d["vibration_mag_g"]),
            ],
            dtype=float,
        ).reshape(1, -1)
        return vals
    except Exception as e:
        log(f"[WARN] feature extraction failed: {e}")
        return None


# ============================================================
# INFERENCE LOGIC
# ============================================================
def run_ml_inference(X: np.ndarray):
    """
    Isolation Forest:
    - decision_function: higher = more normal, lower = more anomalous
    - predict: 1 = normal, -1 = anomaly
    """
    score = float(model.decision_function(X)[0])
    pred = int(model.predict(X)[0])
    is_anomaly_ml = 1 if pred == -1 else 0
    return score, pred, is_anomaly_ml


def combine_decision(is_anomaly_ml: int, is_anomaly_ewma: int):
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
    except Exception as e:
        log(f"[WARN] invalid json: {e}")
        return

    ok, why = validate_payload(d)
    if not ok:
        log(f"[WARN] invalid normalized payload: {why}")
        return

    X = extract_features(d)
    if X is None:
        return

    try:
        # ---- ML ----
        if_score, if_pred, is_anomaly_ml = run_ml_inference(X)

        # ---- EWMA on temperature ----
        temp_c = float(d["temperature_c"])
        temp_ewma, temp_residual, temp_zscore, is_anomaly_ewma = temp_monitor.update(temp_c)

        # ---- Hybrid final decision ----
        is_anomaly_final, anomaly_reason = combine_decision(
            is_anomaly_ml=is_anomaly_ml,
            is_anomaly_ewma=is_anomaly_ewma,
        )

        result = {
            "ts_inference": now_iso(),
            "factory_id": d["factory_id"],
            "machine_id": d["machine_id"],
            "device_id": d["device_id"],
            "reading_index": int(d["reading_index"]),
            "ts_gateway": d["ts_gateway"],

            # raw normalized telemetry
            "temperature_c": temp_c,
            "x_g": float(d["x_g"]),
            "y_g": float(d["y_g"]),
            "z_g": float(d["z_g"]),
            "vibration_mag_g": float(d["vibration_mag_g"]),

            # ML output
            "if_score": if_score,
            "if_pred": if_pred,
            "is_anomaly_ml": is_anomaly_ml,

            # EWMA output
            "temp_ewma": round(float(temp_ewma), 6),
            "temp_residual": round(float(temp_residual), 6),
            "temp_zscore": round(float(temp_zscore), 6),
            "is_anomaly_ewma": is_anomaly_ewma,

            # final decision
            "is_anomaly_final": is_anomaly_final,
            "anomaly_reason": anomaly_reason,
        }

        client.publish(OUTPUT_TOPIC, json.dumps(result), qos=0, retain=False)

        if int(d["reading_index"]) % 100 == 0:
            log(
                f"[INF] idx={d['reading_index']} "
                f"if_score={if_score:.4f} "
                f"ml={is_anomaly_ml} "
                f"ewma={is_anomaly_ewma} "
                f"final={is_anomaly_final} "
                f"reason={anomaly_reason}"
            )

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

    log("[BOOT] Inference service up")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()