#!/usr/bin/env python3
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest


INPUT_CSV = Path.home() / "mva/data/training/train_esp32_001_normal_v2.csv"
MODEL_OUT = Path.home() / "mva/models/if_model_esp32_001.joblib"
META_OUT = Path.home() / "mva/models/if_model_esp32_001.metadata.json"

WINDOW_SIZE = 100
WINDOW_STEP = 50

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


def build_window_features(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    values = df[["temperature_c", "vibration_mag_g"]].astype(float).to_numpy()

    for start in range(0, len(values) - WINDOW_SIZE + 1, WINDOW_STEP):
        window = values[start:start + WINDOW_SIZE]

        temp = window[:, 0]
        vib = window[:, 1]

        rows.append({
            "temp_mean": float(np.mean(temp)),
            "temp_std": float(np.std(temp)),
            "temp_min": float(np.min(temp)),
            "temp_max": float(np.max(temp)),
            "vib_mean": float(np.mean(vib)),
            "vib_std": float(np.std(vib)),
            "vib_min": float(np.min(vib)),
            "vib_max": float(np.max(vib)),
        })

    return pd.DataFrame(rows, columns=FEATURE_COLUMNS)


def max_consecutive_anomalies(preds):
    max_run = 0
    current = 0

    for p in preds:
        if p == -1:
            current += 1
            max_run = max(max_run, current)
        else:
            current = 0

    return max_run


def main():
    df = pd.read_csv(INPUT_CSV)

    df = df.dropna(subset=["temperature_c", "vibration_mag_g"])
    df = df.sort_values("reading_index").reset_index(drop=True)

    print(f"Loaded telemetry rows: {len(df)}")

    if len(df) < 10000:
        raise ValueError("Too few rows. Collect at least 10 minutes of normal Device 002 data.")

    print("\nRaw telemetry summary:")
    print(df[["temperature_c", "vibration_mag_g"]].describe())

    features = build_window_features(df)

    print(f"\nCreated windows: {len(features)}")
    print("\nFeature summary:")
    print(features.describe())

    if len(features) < 200:
        raise ValueError("Too few windows for training.")

    model = IsolationForest(
        n_estimators=100,
        contamination=0.005,
        random_state=42,
        n_jobs=1
    )

    model.fit(features)

    preds = model.predict(features)
    scores = model.decision_function(features)

    anomaly_rate = float((preds == -1).mean())
    max_run = int(max_consecutive_anomalies(preds))

    print("\nTraining validation:")
    print(f"Anomaly rate on training windows: {anomaly_rate:.4f}")
    print(f"Max consecutive anomaly windows: {max_run}")
    print(f"Score min/mean/max: {scores.min():.6f} / {scores.mean():.6f} / {scores.max():.6f}")

    MODEL_OUT.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_OUT)

    metadata = {
        "device_id": "esp32-001",
        "input_csv": str(INPUT_CSV),
        "model_path": str(MODEL_OUT),
        "window_size": WINDOW_SIZE,
        "window_step": WINDOW_STEP,
        "feature_columns": FEATURE_COLUMNS,
        "training_rows": int(len(df)),
        "training_windows": int(len(features)),
        "training_anomaly_rate": anomaly_rate,
        "max_consecutive_training_anomaly_windows": max_run,
        "model_type": "IsolationForest",
        "n_estimators": 100,
        "contamination": 0.005,
        "random_state": 42,
    }

    META_OUT.write_text(json.dumps(metadata, indent=2))

    print(f"\nSaved model to: {MODEL_OUT}")
    print(f"Saved metadata to: {META_OUT}")


if __name__ == "__main__":
    main()