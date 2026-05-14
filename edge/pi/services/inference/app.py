#!/usr/bin/env python3

import json
import os
import signal
import sys
import time
from collections import defaultdict, deque
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
    os.getenv("INFERENCE_MODEL_PATH", "~/mva/models/training/if_window_model_100hz.joblib")
)

# Turn on only when debugging
ENABLE_CONSOLE_LOGGING = os.getenv("INFERENCE_ENABLE_CONSOLE_LOGGING", "false").lower() == "true"

# ============================================================
# DETECTION SETTINGS
# ============================================================
# Allowed:
#   "ewma_only"
#   "ml_only"
#   "hybrid"
DETECTION_MODE = os.getenv("DETECTION_MODE", "hybrid").strip().lower()

# For hybrid mode:
# True  -> anomaly if either ML or EWMA says anomaly
# False -> anomaly only if both say anomaly
HYBRID_USE_OR = os.getenv("HYBRID_USE_OR", "true").lower() == "true"

# If True and DETECTION_MODE == "ewma_only", model is not loaded at all.
DISABLE_ML_WHEN_UNUSED = os.getenv("DISABLE_ML_WHEN_UNUSED", "true").lower() == "true"

# ============================================================
# WINDOW SETTINGS
# ============================================================
# At 50 Hz:
#   WINDOW_SIZE = 50  -> 1.0 second
#   WINDOW_STEP = 25  -> new ML result every 0.5 seconds
WINDOW_SIZE = int(os.getenv("INFERENCE_WINDOW_SIZE", "100"))
WINDOW_STEP = int(os.getenv("INFERENCE_WINDOW_STEP", "50"))

# Keep enough recent samples per device
MAX_BUFFER_SIZE = max(WINDOW_SIZE * 3, WINDOW_SIZE + WINDOW_STEP + 10)

# ============================================================
# WINDOW FEATURE COLUMNS
# IMPORTANT:
# The trained ML artifact must use these exact columns.
# ============================================================
FEATURE_COLUMNS = [
    "temp_mean",
    "temp_std",
    "temp_min",
    "temp_max",
    "x_mean",
    "x_std",
    "y_mean",
    "y_std",
    "z_mean",
    "z_std",
    "vib_mean",
    "vib_std",
    "vib_min",
    "vib_max",
    "vib_rms",
]

# ============================================================
# EWMA PARAMETERS
# ============================================================
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.10"))
EWMA_WINDOW_SIZE = int(os.getenv("EWMA_WINDOW_SIZE", "200"))
EWMA_Z_THRESHOLD = float(os.getenv("EWMA_Z_THRESHOLD", "3.0"))
EWMA_MIN_STD = float(os.getenv("EWMA_MIN_STD", "0.05"))


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
        raise FileNotFoundError(f"Window model file not found: {MODEL_PATH}")

    artifact = joblib.load(MODEL_PATH)

    # Preferred artifact format from train_window_model.py
    if isinstance(artifact, dict) and "model" in artifact:
        model_obj = artifact["model"]

        trained_feature_columns = artifact.get("feature_columns")
        if trained_feature_columns is not None:
            if list(trained_feature_columns) != FEATURE_COLUMNS:
                raise ValueError(
                    "Feature column mismatch between app.py and trained model.\n"
                    f"App expects: {FEATURE_COLUMNS}\n"
                    f"Model has:   {trained_feature_columns}"
                )

        trained_window_size = artifact.get("window_size")
        trained_window_step = artifact.get("window_step")

        if trained_window_size is not None and int(trained_window_size) != WINDOW_SIZE:
            raise ValueError(
                "Window size mismatch between app.py and trained model.\n"
                f"App uses WINDOW_SIZE={WINDOW_SIZE}\n"
                f"Model was trained with window_size={trained_window_size}"
            )

        if trained_window_step is not None and int(trained_window_step) != WINDOW_STEP:
            raise ValueError(
                "Window step mismatch between app.py and trained model.\n"
                f"App uses WINDOW_STEP={WINDOW_STEP}\n"
                f"Model was trained with window_step={trained_window_step}"
            )

        log(
            f"[BOOT] window model artifact loaded from {MODEL_PATH} "
            f"(window_size={trained_window_size}, window_step={trained_window_step})"
        )
        return model_obj

    # Fallback: raw sklearn model
    log(f"[BOOT] raw model loaded from {MODEL_PATH}")
    return artifact


model = None
if should_load_model():
    model = load_model()
else:
    log("[BOOT] model loading skipped (EWMA-only mode)")


# ============================================================
# EWMA STATE (PER DEVICE)
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


temp_monitors_by_device = {}


def get_temp_monitor(device_id: str) -> EWMATemperatureMonitor:
    if device_id not in temp_monitors_by_device:
        temp_monitors_by_device[device_id] = EWMATemperatureMonitor(
            alpha=EWMA_ALPHA,
            window_size=EWMA_WINDOW_SIZE,
            z_threshold=EWMA_Z_THRESHOLD,
            min_std=EWMA_MIN_STD,
        )
    return temp_monitors_by_device[device_id]


# ============================================================
# WINDOW BUFFERS (PER DEVICE)
# ============================================================
buffers_by_device = defaultdict(lambda: deque(maxlen=MAX_BUFFER_SIZE))


# ============================================================
# PAYLOAD VALIDATION
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


def normalize_input_row(d: dict):
    return {
        "factory_id": str(d["factory_id"]),
        "machine_id": str(d["machine_id"]),
        "device_id": str(d["device_id"]),
        "reading_index": int(d["reading_index"]),
        "ts_gateway": str(d["ts_gateway"]),
        "temperature_c": float(d["temperature_c"]),
        "x_g": float(d["x_g"]),
        "y_g": float(d["y_g"]),
        "z_g": float(d["z_g"]),
        "vibration_mag_g": float(d["vibration_mag_g"]),
    }


# ============================================================
# WINDOW FEATURE ENGINEERING
# ============================================================
def compute_window_features(window_rows):
    temps = np.array([r["temperature_c"] for r in window_rows], dtype=float)
    x_vals = np.array([r["x_g"] for r in window_rows], dtype=float)
    y_vals = np.array([r["y_g"] for r in window_rows], dtype=float)
    z_vals = np.array([r["z_g"] for r in window_rows], dtype=float)
    vib_vals = np.array([r["vibration_mag_g"] for r in window_rows], dtype=float)

    features = {
        "temp_mean": float(np.mean(temps)),
        "temp_std": float(np.std(temps)),
        "temp_min": float(np.min(temps)),
        "temp_max": float(np.max(temps)),
        "x_mean": float(np.mean(x_vals)),
        "x_std": float(np.std(x_vals)),
        "y_mean": float(np.mean(y_vals)),
        "y_std": float(np.std(y_vals)),
        "z_mean": float(np.mean(z_vals)),
        "z_std": float(np.std(z_vals)),
        "vib_mean": float(np.mean(vib_vals)),
        "vib_std": float(np.std(vib_vals)),
        "vib_min": float(np.min(vib_vals)),
        "vib_max": float(np.max(vib_vals)),
        "vib_rms": float(np.sqrt(np.mean(np.square(vib_vals)))),
    }
    return features


def make_feature_frame(feature_row: dict) -> pd.DataFrame:
    return pd.DataFrame([feature_row], columns=FEATURE_COLUMNS)


# ============================================================
# ML INFERENCE
# ============================================================
def run_ml_inference(X: pd.DataFrame):
    score = float(model.decision_function(X)[0])  # higher = more normal
    pred = int(model.predict(X)[0])               # 1 = normal, -1 = anomaly
    ML_SCORE_THRESHOLD = float(os.getenv("ML_SCORE_THRESHOLD", "-0.01"))
    is_anomaly_ml = 1 if score < ML_SCORE_THRESHOLD else 0
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


# ============================================================
# WINDOW / EWMA DECISION LOGIC
# ============================================================
def get_window_ewma_flag(window_rows):
    # Current policy:
    # Use the latest EWMA decision in the window.
    latest = window_rows[-1]
    return int(latest.get("is_anomaly_ewma", 0))


def build_window_result(window_rows, window_features, if_score, if_pred, is_anomaly_ml):
    first_row = window_rows[0]
    last_row = window_rows[-1]

    is_anomaly_ewma = get_window_ewma_flag(window_rows)
    is_anomaly_final, anomaly_reason = combine_decision(
        is_anomaly_ml=is_anomaly_ml,
        is_anomaly_ewma=is_anomaly_ewma,
    )

    result = {
        "ts_inference": now_iso_ms(),
        "factory_id": last_row["factory_id"],
        "machine_id": last_row["machine_id"],
        "device_id": last_row["device_id"],

        "window_start_index": int(first_row["reading_index"]),
        "window_end_index": int(last_row["reading_index"]),
        "window_size": int(len(window_rows)),
        "window_step": int(WINDOW_STEP),

        "window_start_ts": first_row["ts_gateway"],
        "window_end_ts": last_row["ts_gateway"],

        "temp_mean": round(float(window_features["temp_mean"]), 6),
        "temp_std": round(float(window_features["temp_std"]), 6),
        "temp_min": round(float(window_features["temp_min"]), 6),
        "temp_max": round(float(window_features["temp_max"]), 6),

        "x_mean": round(float(window_features["x_mean"]), 6),
        "x_std": round(float(window_features["x_std"]), 6),
        "y_mean": round(float(window_features["y_mean"]), 6),
        "y_std": round(float(window_features["y_std"]), 6),
        "z_mean": round(float(window_features["z_mean"]), 6),
        "z_std": round(float(window_features["z_std"]), 6),

        "vib_mean": round(float(window_features["vib_mean"]), 6),
        "vib_std": round(float(window_features["vib_std"]), 6),
        "vib_min": round(float(window_features["vib_min"]), 6),
        "vib_max": round(float(window_features["vib_max"]), 6),
        "vib_rms": round(float(window_features["vib_rms"]), 6),

        "if_score": float(if_score),
        "if_pred": int(if_pred),
        "is_anomaly_ml": int(is_anomaly_ml),

        # latest EWMA state within the window
        "temp_ewma": round(float(last_row.get("temp_ewma", 0.0)), 6),
        "temp_residual": round(float(last_row.get("temp_residual", 0.0)), 6),
        "temp_zscore": round(float(last_row.get("temp_zscore", 0.0)), 6),
        "is_anomaly_ewma": int(is_anomaly_ewma),

        "is_anomaly_final": int(is_anomaly_final),
        "anomaly_reason": anomaly_reason,
    }
    return result


def process_window_if_ready(client, device_id: str):
    buf = buffers_by_device[device_id]

    while len(buf) >= WINDOW_SIZE:
        window_rows = list(buf)[:WINDOW_SIZE]
        window_features = compute_window_features(window_rows)

        if DETECTION_MODE == "ewma_only":
            if_score = 0.0
            if_pred = 1
            is_anomaly_ml = 0
        else:
            X = make_feature_frame(window_features)
            if_score, if_pred, is_anomaly_ml = run_ml_inference(X)

        result = build_window_result(
            window_rows=window_rows,
            window_features=window_features,
            if_score=if_score,
            if_pred=if_pred,
            is_anomaly_ml=is_anomaly_ml,
        )

        client.publish(OUTPUT_TOPIC, json.dumps(result), qos=0, retain=False)

        log(
            f"[WIN] device={device_id} "
            f"idx={result['window_start_index']}-{result['window_end_index']} "
            f"ml={result['is_anomaly_ml']} "
            f"ewma={result['is_anomaly_ewma']} "
            f"final={result['is_anomaly_final']} "
            f"reason={result['anomaly_reason']}"
        )

        # Slide forward by WINDOW_STEP
        drop_n = min(WINDOW_STEP, len(buf))
        for _ in range(drop_n):
            buf.popleft()


# ============================================================
# MQTT CALLBACKS
# ============================================================
def on_connect(client, userdata, flags, rc, properties=None):
    log(f"[MQTT] connected rc={rc}")
    client.subscribe(INPUT_TOPIC, qos=1)
    log(f"[MQTT] subscribed to {INPUT_TOPIC}")
    log(
        f"[BOOT] DETECTION_MODE={DETECTION_MODE} "
        f"WINDOW_SIZE={WINDOW_SIZE} "
        f"WINDOW_STEP={WINDOW_STEP}"
    )


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
        d = json.loads(payload)

        ok, why = validate_payload(d)
        if not ok:
            log(f"[WARN] invalid normalized payload: {why}")
            return

        row = normalize_input_row(d)
        device_id = row["device_id"]

        # EWMA still runs per message
        temp_monitor = get_temp_monitor(device_id)
        temp_ewma, temp_residual, temp_zscore, is_anomaly_ewma = temp_monitor.update(
            row["temperature_c"]
        )

        row["temp_ewma"] = float(temp_ewma)
        row["temp_residual"] = float(temp_residual)
        row["temp_zscore"] = float(temp_zscore)
        row["is_anomaly_ewma"] = int(is_anomaly_ewma)

        buffers_by_device[device_id].append(row)

        process_window_if_ready(client, device_id)

    except Exception as e:
        log(f"[ERROR] inference failed: {e}")


# ============================================================
# MAIN
# ============================================================
def main():
    client = mqtt.Client(
        client_id="inference-service-windowed",
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

    log("[BOOT] Windowed inference service up")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()