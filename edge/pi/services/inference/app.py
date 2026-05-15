#!/usr/bin/env python3
import json
import os
import signal
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt


# ============================================================
# Configuration
# ============================================================

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mva-inference-service")

INPUT_TOPIC = os.getenv("INFERENCE_INPUT_TOPIC", "mva/normalized/telemetry")
OUTPUT_TOPIC = os.getenv("INFERENCE_OUTPUT_TOPIC", "mva/inference/telemetry")

LOG_PATH = os.path.expanduser(os.getenv("INFERENCE_LOG_PATH", "~/mva/logs/inference.log"))
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# Detection mode: ewma_only, ml_only, hybrid
DETECTION_MODE = os.getenv("DETECTION_MODE", "hybrid").lower().strip()

# Windowing
WINDOW_SIZE = int(os.getenv("INFERENCE_WINDOW_SIZE", "100"))
WINDOW_STEP = int(os.getenv("INFERENCE_WINDOW_STEP", "50"))

# Ignore early startup windows before this reading index.
# At 100 Hz, 5000 readings is approximately 50 seconds.
IGNORE_BEFORE_INDEX = int(os.getenv("INFERENCE_IGNORE_BEFORE_INDEX", "5000"))

# ML model paths
MODEL_DIR = Path(os.getenv("INFERENCE_MODEL_DIR", str(Path.home() / "mva/models")))

# Expected filenames:
#   ~/mva/models/if_model_esp32_001.joblib
#   ~/mva/models/if_model_esp32_002.joblib
#
# Fallback model:
#   ~/mva/models/if_model.joblib
MODEL_FILE_TEMPLATE = os.getenv("INFERENCE_MODEL_TEMPLATE", "if_model_{device_id}.joblib")
FALLBACK_MODEL_PATH = Path(os.getenv("INFERENCE_MODEL_PATH", str(MODEL_DIR / "if_model.joblib")))

# Fallback is disabled by default so esp32-002 cannot silently use the old/shared model.
# Enable only intentionally:
# export INFERENCE_ALLOW_FALLBACK_MODEL=1
ALLOW_FALLBACK_MODEL = os.getenv("INFERENCE_ALLOW_FALLBACK_MODEL", "0") == "1"

# Optional explicit device model map:
# export INFERENCE_DEVICE_MODELS='{"esp32-001":"/home/marifaj/mva/models/if_model_esp32_001.joblib","esp32-002":"/home/marifaj/mva/models/if_model_esp32_002.joblib"}'
DEVICE_MODELS_JSON = os.getenv("INFERENCE_DEVICE_MODELS", "").strip()

# EWMA settings
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.10"))
EWMA_RESIDUAL_WINDOW = int(os.getenv("EWMA_RESIDUAL_WINDOW", "200"))
EWMA_MIN_STD = float(os.getenv("EWMA_MIN_STD", "0.05"))
EWMA_THRESHOLD = float(os.getenv("EWMA_THRESHOLD", "3.0"))

# Logging
# Debug defaults are intentionally verbose. After debugging, use:
# export INFERENCE_LOG_NORMAL_WINDOWS=0
# export INFERENCE_LOG_EVERY_N=100
LOG_EVERY_N_WINDOWS = int(os.getenv("INFERENCE_LOG_EVERY_N", "1"))
LOG_NORMAL_WINDOWS = os.getenv("INFERENCE_LOG_NORMAL_WINDOWS", "1") == "1"

# MQTT QoS
MQTT_QOS = int(os.getenv("MQTT_QOS", "0"))

# Required feature order. This must match training.
FEATURE_COLUMNS = [
    "temp_mean",
    "temp_std",
    "temp_min",
    "temp_max",
    "vib_mean",
    "vib_std",
    "vib_min",
    "vib_max",
]


# ============================================================
# Runtime state
# ============================================================

running = True

# Per-device telemetry windows
buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

# Per-device count of samples since last inference window
steps_since_window: Dict[str, int] = defaultdict(int)

# Per-device window counter
window_counter: Dict[str, int] = defaultdict(int)

# Per-device loaded ML models
models: Dict[str, Any] = {}

# Optional fallback model
fallback_model: Optional[Any] = None

# EWMA state per device and signal
ewma_state: Dict[str, Dict[str, Any]] = defaultdict(dict)

# Avoid repeated missing-model spam.
missing_model_warned = set()


# ============================================================
# Utility functions
# ============================================================

def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(message: str) -> None:
    line = f"{now_iso_utc()} inference {message}"
    print(line, flush=True)

    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # Logging should never crash inference.
        pass


def normalize_device_id_for_filename(device_id: str) -> str:
    """
    Converts esp32-001 -> esp32_001 for filenames.
    Expected model files:
      if_model_esp32_001.joblib
      if_model_esp32_002.joblib
    """
    return device_id.replace("-", "_")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def parse_payload(raw_payload: bytes) -> Optional[Dict[str, Any]]:
    try:
        text = raw_payload.decode("utf-8")
        data = json.loads(text)
        if not isinstance(data, dict):
            return None
        return data
    except Exception as exc:
        log(f"[WARN] Failed to parse MQTT payload: {exc}")
        return None


def get_factory_id(data: Dict[str, Any]) -> str:
    return (
        data.get("factory_id")
        or data.get("factoryId")
        or data.get("factory")
        or "factory-001"
    )


def get_device_id(data: Dict[str, Any]) -> str:
    return (
        data.get("device_id")
        or data.get("deviceId")
        or data.get("device")
        or "unknown-device"
    )


def get_machine_id(data: Dict[str, Any]) -> str:
    return (
        data.get("machine_id")
        or data.get("machineId")
        or data.get("machine")
        or "machine-001"
    )


def extract_sample(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts normalized telemetry into the fields required for inference.
    Expected input fields:
      factory_id, device_id, machine_id, reading_index,
      temperature_c, x_g, y_g, z_g, vibration_mag_g
    """

    factory_id = get_factory_id(data)
    device_id = get_device_id(data)
    machine_id = get_machine_id(data)

    temperature_c = safe_float(
        data.get("temperature_c", data.get("temperature", data.get("temp_c"))),
        default=np.nan,
    )

    x_g = safe_float(data.get("x_g", data.get("raw_x")), default=np.nan)
    y_g = safe_float(data.get("y_g", data.get("raw_y")), default=np.nan)
    z_g = safe_float(data.get("z_g", data.get("raw_z")), default=np.nan)

    vibration_mag_g = data.get("vibration_mag_g", data.get("vibration_magnitude_g"))

    if vibration_mag_g is None:
        if not np.isnan(x_g) and not np.isnan(y_g) and not np.isnan(z_g):
            vibration_mag_g = float(np.sqrt(x_g * x_g + y_g * y_g + z_g * z_g))
        else:
            vibration_mag_g = np.nan
    else:
        vibration_mag_g = safe_float(vibration_mag_g, default=np.nan)

    # Prefer gateway timestamp, but accept other aliases.
    ts_gateway = (
        data.get("ts_gateway")
        or data.get("gateway_ts")
        or data.get("timestamp")
        or data.get("ts")
        or now_iso_utc()
    )

    sample = {
        "factory_id": factory_id,
        "device_id": device_id,
        "machine_id": machine_id,
        "reading_index": safe_int(data.get("reading_index", data.get("idx")), default=0),
        "ts_gateway": ts_gateway,
        "temperature_c": temperature_c,
        "x_g": x_g,
        "y_g": y_g,
        "z_g": z_g,
        "vibration_mag_g": vibration_mag_g,
        "raw": data,
    }

    return sample


def build_features(window_samples: list) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Builds one feature row from one window.

    Feature order must match the training script:
      temp_mean, temp_std, temp_min, temp_max,
      vib_mean, vib_std, vib_min, vib_max
    """

    temp = np.array([s["temperature_c"] for s in window_samples], dtype=float)
    vib = np.array([s["vibration_mag_g"] for s in window_samples], dtype=float)

    # Remove NaNs if any.
    temp = temp[~np.isnan(temp)]
    vib = vib[~np.isnan(vib)]

    if len(temp) == 0:
        temp = np.array([0.0])
    if len(vib) == 0:
        vib = np.array([0.0])

    feature_dict = {
        "temp_mean": float(np.mean(temp)),
        "temp_std": float(np.std(temp)),
        "temp_min": float(np.min(temp)),
        "temp_max": float(np.max(temp)),
        "vib_mean": float(np.mean(vib)),
        "vib_std": float(np.std(vib)),
        "vib_min": float(np.min(vib)),
        "vib_max": float(np.max(vib)),
    }

    df = pd.DataFrame([feature_dict], columns=FEATURE_COLUMNS)
    return df, feature_dict


# ============================================================
# Model loading
# ============================================================

def load_model_file(path: Path) -> Optional[Any]:
    try:
        if path.exists():
            model = joblib.load(path)
            log(f"[MODEL] Loaded: {path}")
            return model

        log(f"[MODEL] Missing: {path}")
        return None
    except Exception as exc:
        log(f"[ERROR] Failed to load model {path}: {exc}")
        return None


def load_models() -> None:
    global models, fallback_model

    models = {}

    log(f"[MODEL] MODEL_DIR={MODEL_DIR}")
    log(f"[MODEL] MODEL_FILE_TEMPLATE={MODEL_FILE_TEMPLATE}")
    log(f"[MODEL] FALLBACK_MODEL_PATH={FALLBACK_MODEL_PATH}")
    log(f"[MODEL] ALLOW_FALLBACK_MODEL={ALLOW_FALLBACK_MODEL}")

    if DEVICE_MODELS_JSON:
        try:
            explicit_map = json.loads(DEVICE_MODELS_JSON)
            log(f"[MODEL] Using explicit INFERENCE_DEVICE_MODELS map for {len(explicit_map)} device(s)")

            for device_id, model_path in explicit_map.items():
                loaded = load_model_file(Path(model_path))
                if loaded is not None:
                    models[device_id] = loaded
        except Exception as exc:
            log(f"[WARN] Could not parse INFERENCE_DEVICE_MODELS: {exc}")

    # Try common model names for current devices.
    known_devices = ["esp32-001", "esp32-002"]

    for device_id in known_devices:
        if device_id in models:
            continue

        filename_device = normalize_device_id_for_filename(device_id)
        filename = MODEL_FILE_TEMPLATE.format(
            device_id=filename_device,
            raw_device_id=device_id,
        )
        path = MODEL_DIR / filename

        log(f"[MODEL] Looking for device={device_id} model at {path}")

        loaded = load_model_file(path)
        if loaded is not None:
            models[device_id] = loaded

    if ALLOW_FALLBACK_MODEL:
        fallback_model = load_model_file(FALLBACK_MODEL_PATH)
    else:
        fallback_model = None
        log("[MODEL] Fallback disabled. Missing device-specific models will produce no-model decisions.")

    log("[MODEL] Device models loaded:")
    if models:
        for device_id in models.keys():
            log(f"  - {device_id}")
    else:
        log("  - none")

    if fallback_model is not None:
        log(f"[MODEL] Fallback model available: {FALLBACK_MODEL_PATH}")
    else:
        log("[MODEL] No fallback model active")


def get_model_for_device(device_id: str) -> Tuple[Optional[Any], str]:
    if device_id in models:
        return models[device_id], device_id

    normalized = device_id.replace("_", "-")
    if normalized in models:
        return models[normalized], normalized

    if ALLOW_FALLBACK_MODEL and fallback_model is not None:
        return fallback_model, "fallback"

    if device_id not in missing_model_warned:
        missing_model_warned.add(device_id)
        log(f"[WARN] No device-specific model found for device={device_id}; fallback disabled")

    return None, "none"


# ============================================================
# EWMA detector
# ============================================================

def ewma_update_signal(device_id: str, signal_name: str, value: float) -> Tuple[bool, float]:
    """
    Returns:
      is_anomaly, normalized_residual
    """

    key = signal_name

    if key not in ewma_state[device_id]:
        ewma_state[device_id][key] = {
            "mean": value,
            "residuals": deque(maxlen=EWMA_RESIDUAL_WINDOW),
            "initialized": True,
        }
        return False, 0.0

    state = ewma_state[device_id][key]

    prev_mean = state["mean"]
    residual = value - prev_mean

    state["residuals"].append(residual)

    # EWMA update.
    state["mean"] = EWMA_ALPHA * value + (1.0 - EWMA_ALPHA) * prev_mean

    residuals = np.array(state["residuals"], dtype=float)

    if len(residuals) < max(20, min(EWMA_RESIDUAL_WINDOW, 50)):
        return False, 0.0

    std = float(np.std(residuals))
    std = max(std, EWMA_MIN_STD)

    normalized_residual = float(residual / std)
    is_anomaly = abs(normalized_residual) >= EWMA_THRESHOLD

    return bool(is_anomaly), normalized_residual


def ewma_detect(device_id: str, features: Dict[str, float]) -> Tuple[bool, Dict[str, Any]]:
    temp_anom, temp_r = ewma_update_signal(device_id, "temp_mean", features["temp_mean"])
    vib_anom, vib_r = ewma_update_signal(device_id, "vib_mean", features["vib_mean"])

    is_anomaly = temp_anom or vib_anom

    details = {
        "ewma_anomaly": bool(is_anomaly),
        "ewma_temp_residual": float(temp_r),
        "ewma_vib_residual": float(vib_r),
        "ewma_threshold": EWMA_THRESHOLD,
    }

    return bool(is_anomaly), details


# ============================================================
# ML detector
# ============================================================

def ml_detect(device_id: str, feature_df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    model, model_used = get_model_for_device(device_id)

    if model is None:
        return False, {
            "ml_anomaly": False,
            "ml_available": False,
            "ml_reason": "no_model_for_device",
            "ml_prediction": 1,
            "ml_score": 0.0,
            "ml_model_device": model_used,
        }

    try:
        pred = int(model.predict(feature_df)[0])
        score = 0.0

        if hasattr(model, "decision_function"):
            score = float(model.decision_function(feature_df)[0])

        is_anomaly = pred == -1

        return bool(is_anomaly), {
            "ml_anomaly": bool(is_anomaly),
            "ml_available": True,
            "ml_prediction": pred,
            "ml_score": score,
            "ml_model_device": model_used,
        }

    except Exception as exc:
        log(f"[ERROR] ML prediction failed for device={device_id}, model_used={model_used}: {exc}")
        return False, {
            "ml_anomaly": False,
            "ml_available": False,
            "ml_reason": f"prediction_error: {exc}",
            "ml_prediction": 1,
            "ml_score": 0.0,
            "ml_model_device": "error",
        }


# ============================================================
# Final decision logic
# ============================================================

def make_final_decision(
    ewma_anomaly: bool,
    ml_anomaly: bool,
    ewma_details: Dict[str, Any],
    ml_details: Dict[str, Any],
) -> Tuple[bool, str, str]:
    """
    Returns:
      is_final_anomaly, reason, severity
    """

    if DETECTION_MODE == "ewma_only":
        if ewma_anomaly:
            return True, "ewma_only", "medium"
        return False, "normal", "info"

    if DETECTION_MODE == "ml_only":
        if ml_anomaly:
            return True, "ml_only", "high"
        return False, "normal", "info"

    # Default: hybrid.
    if DETECTION_MODE == "hybrid":
        if ewma_anomaly and ml_anomaly:
            return True, "ml_and_ewma", "critical"
        if ml_anomaly:
            return True, "ml_only", "high"
        if ewma_anomaly:
            return True, "ewma_only", "medium"
        return False, "normal", "info"

    # Unknown mode fallback.
    log(f"[WARN] Unknown DETECTION_MODE={DETECTION_MODE}, using hybrid behavior")

    if ewma_anomaly and ml_anomaly:
        return True, "ml_and_ewma", "critical"
    if ml_anomaly:
        return True, "ml_only", "high"
    if ewma_anomaly:
        return True, "ewma_only", "medium"

    return False, "normal", "info"


def publish_inference_result(
    client: mqtt.Client,
    factory_id: str,
    device_id: str,
    machine_id: str,
    window_samples: list,
    feature_values: Dict[str, float],
    ewma_details: Dict[str, Any],
    ml_details: Dict[str, Any],
    is_final_anomaly: bool,
    reason: str,
    severity: str,
) -> None:
    first = window_samples[0]
    last = window_samples[-1]

    ts_ms = now_ms()
    ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000))

    window_start_ts = first.get("ts_gateway") or ts_iso
    window_end_ts = last.get("ts_gateway") or ts_iso

    window_start_index = int(first.get("reading_index") or 0)
    window_end_index = int(last.get("reading_index") or 0)

    ml_score = ml_details.get("ml_score")
    if ml_score is None:
        ml_score = 0.0

    if_pred = ml_details.get("ml_prediction")
    if if_pred is None:
        if_pred = 1

    is_anomaly_ml = 1 if bool(ml_details.get("ml_anomaly", False)) else 0
    is_anomaly_ewma = 1 if bool(ewma_details.get("ewma_anomaly", False)) else 0
    is_anomaly_final = 1 if bool(is_final_anomaly) else 0

    temp_zscore = float(ewma_details.get("ewma_temp_residual", 0.0) or 0.0)

    # Keep event_processing naming convention.
    anomaly_reason = reason
    if anomaly_reason == "ewma_and_ml":
        anomaly_reason = "ml_and_ewma"

    # Event-processing service expects a flat legacy-compatible schema.
    result = {
        "schema": "mva.inference.v1",

        # Required by event_processing/app.py.
        "ts_inference": ts_iso,
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,

        "factory_id": factory_id,
        "machine_id": machine_id,
        "device_id": device_id,

        "window_start_index": window_start_index,
        "window_end_index": window_end_index,
        "window_size": WINDOW_SIZE,
        "window_step": WINDOW_STEP,

        "if_score": float(ml_score),
        "if_pred": int(if_pred),
        "is_anomaly_ml": int(is_anomaly_ml),
        "temp_zscore": float(temp_zscore),
        "is_anomaly_ewma": int(is_anomaly_ewma),
        "is_anomaly_final": int(is_anomaly_final),
        "anomaly_reason": anomaly_reason,

        # Extra compatibility / debugging fields.
        "ts_inference_ms": ts_ms,
        "reading_index_start": window_start_index,
        "reading_index_end": window_end_index,
        "detection_mode": DETECTION_MODE,
        "is_anomaly": bool(is_final_anomaly),
        "is_final": is_anomaly_final,
        "reason": reason,
        "severity": severity,

        # Model debug fields.
        "ml_model_device": ml_details.get("ml_model_device", "unknown"),
        "ml_available": bool(ml_details.get("ml_available", False)),
        "ml_reason": ml_details.get("ml_reason", ""),

        # Flattened features for event DB columns.
        "temp_mean": float(feature_values.get("temp_mean", 0.0)),
        "temp_std": float(feature_values.get("temp_std", 0.0)),
        "vib_mean": float(feature_values.get("vib_mean", 0.0)),
        "vib_std": float(feature_values.get("vib_std", 0.0)),
        "vib_rms": float(feature_values.get("vib_mean", 0.0)),

        # Nested details retained for debugging.
        "features": feature_values,
        "ewma": ewma_details,
        "ml": ml_details,
    }

    try:
        payload = json.dumps(result, separators=(",", ":"))
        client.publish(OUTPUT_TOPIC, payload, qos=MQTT_QOS, retain=False)
    except Exception as exc:
        log(f"[ERROR] Failed to publish inference result: {exc}")


# ============================================================
# MQTT callbacks
# ============================================================

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log(f"[MQTT] Connected to {MQTT_HOST}:{MQTT_PORT}")
        log(f"[MQTT] Subscribing to: {INPUT_TOPIC}")
        client.subscribe(INPUT_TOPIC, qos=MQTT_QOS)
    else:
        log(f"[MQTT] Connection failed rc={rc}")


def on_disconnect(client, userdata, rc, properties=None):
    log(f"[MQTT] Disconnected rc={rc}")


def on_message(client, userdata, msg):
    data = parse_payload(msg.payload)
    if data is None:
        return

    sample = extract_sample(data)

    factory_id = sample["factory_id"]
    device_id = sample["device_id"]
    machine_id = sample["machine_id"]

    if np.isnan(sample["temperature_c"]) or np.isnan(sample["vibration_mag_g"]):
        log(f"[WARN] Skipping invalid sample device={device_id}, missing temp/vib")
        return

    buffers[device_id].append(sample)
    steps_since_window[device_id] += 1

    if len(buffers[device_id]) < WINDOW_SIZE:
        return

    if steps_since_window[device_id] < WINDOW_STEP:
        return

    steps_since_window[device_id] = 0
    window_counter[device_id] += 1

    window_samples = list(buffers[device_id])

    feature_df, feature_values = build_features(window_samples)

    ewma_anomaly, ewma_details = ewma_detect(device_id, feature_values)
    ml_anomaly, ml_details = ml_detect(device_id, feature_df)

    is_final_anomaly, reason, severity = make_final_decision(
        ewma_anomaly,
        ml_anomaly,
        ewma_details,
        ml_details,
    )

    # Startup warm-up suppression:
    # Do not allow early windows to become final anomalies.
    window_end_index = int(window_samples[-1].get("reading_index") or 0)

    suppressed_by_warmup = False
    original_reason = reason
    original_ml_anomaly = bool(ml_details.get("ml_anomaly", False))
    original_ewma_anomaly = bool(ewma_details.get("ewma_anomaly", False))

    if window_end_index < IGNORE_BEFORE_INDEX:
        if is_final_anomaly:
            suppressed_by_warmup = True
            log(
                "[WARMUP] Suppressed early anomaly "
                f"device={device_id} "
                f"win={window_samples[0]['reading_index']}-{window_samples[-1]['reading_index']} "
                f"reason={reason} "
                f"ml={original_ml_anomaly} "
                f"ewma={original_ewma_anomaly} "
                f"model={ml_details.get('ml_model_device')} "
                f"score={ml_details.get('ml_score')} "
                f"threshold={IGNORE_BEFORE_INDEX}"
            )

        is_final_anomaly = False
        reason = "normal"
        severity = "info"

        # Also make the flat fields consistent for event_processing.
        ml_details["ml_anomaly"] = False
        ml_details["ml_prediction"] = 1
        ewma_details["ewma_anomaly"] = False

    publish_inference_result(
        client=client,
        factory_id=factory_id,
        device_id=device_id,
        machine_id=machine_id,
        window_samples=window_samples,
        feature_values=feature_values,
        ewma_details=ewma_details,
        ml_details=ml_details,
        is_final_anomaly=is_final_anomaly,
        reason=reason,
        severity=severity,
    )

    should_log = False

    if is_final_anomaly:
        should_log = True
    elif suppressed_by_warmup:
        should_log = True
    elif LOG_NORMAL_WINDOWS:
        should_log = True
    elif LOG_EVERY_N_WINDOWS > 0 and window_counter[device_id] % LOG_EVERY_N_WINDOWS == 0:
        should_log = True

    if should_log:
        log(
            "[INFERENCE] "
            f"device={device_id} "
            f"machine={machine_id} "
            f"win={window_samples[0]['reading_index']}-{window_samples[-1]['reading_index']} "
            f"is_final={1 if is_final_anomaly else 0} "
            f"reason={reason} "
            f"original_reason={original_reason} "
            f"severity={severity} "
            f"ml={ml_details.get('ml_anomaly')} "
            f"ml_original={original_ml_anomaly} "
            f"ml_model={ml_details.get('ml_model_device')} "
            f"ml_score={ml_details.get('ml_score')} "
            f"ml_pred={ml_details.get('ml_prediction')} "
            f"ml_available={ml_details.get('ml_available')} "
            f"ewma={ewma_details.get('ewma_anomaly')} "
            f"ewma_original={original_ewma_anomaly} "
            f"temp_z={ewma_details.get('ewma_temp_residual')} "
            f"vib_z={ewma_details.get('ewma_vib_residual')} "
            f"temp_mean={feature_values['temp_mean']:.3f} "
            f"temp_std={feature_values['temp_std']:.6f} "
            f"vib_mean={feature_values['vib_mean']:.6f} "
            f"vib_std={feature_values['vib_std']:.6f} "
            f"warmup={suppressed_by_warmup}"
        )


# ============================================================
# Shutdown handling
# ============================================================

def handle_shutdown(signum, frame):
    global running
    running = False
    log("[SERVICE] Shutdown requested")


# ============================================================
# Main
# ============================================================

def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    log("[SERVICE] Starting MVA inference service")
    log(f"[CONFIG] MQTT_HOST={MQTT_HOST}")
    log(f"[CONFIG] MQTT_PORT={MQTT_PORT}")
    log(f"[CONFIG] INPUT_TOPIC={INPUT_TOPIC}")
    log(f"[CONFIG] OUTPUT_TOPIC={OUTPUT_TOPIC}")
    log(f"[CONFIG] LOG_PATH={LOG_PATH}")
    log(f"[CONFIG] DETECTION_MODE={DETECTION_MODE}")
    log(f"[CONFIG] WINDOW_SIZE={WINDOW_SIZE}")
    log(f"[CONFIG] WINDOW_STEP={WINDOW_STEP}")
    log(f"[CONFIG] IGNORE_BEFORE_INDEX={IGNORE_BEFORE_INDEX}")
    log(f"[CONFIG] MODEL_DIR={MODEL_DIR}")
    log(f"[CONFIG] LOG_NORMAL_WINDOWS={LOG_NORMAL_WINDOWS}")
    log(f"[CONFIG] LOG_EVERY_N_WINDOWS={LOG_EVERY_N_WINDOWS}")

    load_models()

    try:
        client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    except TypeError:
        client = mqtt.Client(MQTT_CLIENT_ID)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    while running:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_start()

            while running:
                time.sleep(1)

            client.loop_stop()
            client.disconnect()

        except Exception as exc:
            if running:
                log(f"[ERROR] MQTT loop error: {exc}")
                log("[SERVICE] Reconnecting in 3 seconds...")
                time.sleep(3)

    log("[SERVICE] Inference service stopped")


if __name__ == "__main__":
    main()
