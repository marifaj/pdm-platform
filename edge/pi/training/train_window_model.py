#!/usr/bin/env python3

import json
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

INPUT_CSV = os.path.expanduser(
    os.getenv("TRAINING_INPUT_CSV", "~/mva/data/telemetry_training_combined_windowed.csv")
)
OUTPUT_MODEL = os.path.expanduser(
    os.getenv("TRAINING_OUTPUT_MODEL", "~/mva/models/if_window_model.joblib")
)
OUTPUT_FEATURES_CSV = os.path.expanduser(
    os.getenv("TRAINING_OUTPUT_FEATURES_CSV", "~/mva/data/window_features.csv")
)

WINDOW_SIZE = int(os.getenv("TRAINING_WINDOW_SIZE", "50"))
WINDOW_STEP = int(os.getenv("TRAINING_WINDOW_STEP", "25"))

N_ESTIMATORS = int(os.getenv("IF_N_ESTIMATORS", "200"))
CONTAMINATION = float(os.getenv("IF_CONTAMINATION", "0.02"))
RANDOM_STATE = int(os.getenv("IF_RANDOM_STATE", "42"))

USE_LABEL_FILTER = os.getenv("USE_LABEL_FILTER", "false").lower() == "true"
LABEL_COLUMN = os.getenv("LABEL_COLUMN", "label")
NORMAL_LABEL_VALUE = os.getenv("NORMAL_LABEL_VALUE", "normal")
GROUP_BY_DEVICE = os.getenv("GROUP_BY_DEVICE", "true").lower() == "true"

RAW_REQUIRED_COLUMNS = [
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


def ensure_parent_dir(path_str: str) -> None:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)


def validate_input_columns(df: pd.DataFrame) -> None:
    missing = [c for c in RAW_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input CSV: {missing}")


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["factory_id"] = out["factory_id"].astype(str)
    out["machine_id"] = out["machine_id"].astype(str)
    out["device_id"] = out["device_id"].astype(str)
    out["reading_index"] = pd.to_numeric(out["reading_index"], errors="coerce").astype("Int64")
    for col in ["temperature_c", "x_g", "y_g", "z_g", "vibration_mag_g"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["ts_gateway"] = pd.to_datetime(out["ts_gateway"], errors="coerce", utc=True)
    return out


def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    needed = RAW_REQUIRED_COLUMNS.copy()
    if USE_LABEL_FILTER and LABEL_COLUMN in df.columns:
        needed.append(LABEL_COLUMN)
    out = df.dropna(subset=[c for c in needed if c in df.columns]).copy()
    for col in ["temperature_c", "x_g", "y_g", "z_g", "vibration_mag_g"]:
        out = out[np.isfinite(out[col])]
    return out


def filter_normal_only(df: pd.DataFrame) -> pd.DataFrame:
    if not USE_LABEL_FILTER:
        return df
    if LABEL_COLUMN not in df.columns:
        raise ValueError(f"USE_LABEL_FILTER=True but label column '{LABEL_COLUMN}' not found in input CSV.")
    return df[df[LABEL_COLUMN].astype(str) == str(NORMAL_LABEL_VALUE)].copy()


def compute_window_features(window_df: pd.DataFrame) -> dict:
    temps = window_df["temperature_c"].to_numpy(dtype=float)
    x_vals = window_df["x_g"].to_numpy(dtype=float)
    y_vals = window_df["y_g"].to_numpy(dtype=float)
    z_vals = window_df["z_g"].to_numpy(dtype=float)
    vib_vals = window_df["vibration_mag_g"].to_numpy(dtype=float)
    return {
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


def build_windows_for_group(group_df: pd.DataFrame) -> list[dict]:
    rows = []
    group_df = group_df.sort_values(["reading_index", "ts_gateway"]).reset_index(drop=True)
    n = len(group_df)
    if n < WINDOW_SIZE:
        return rows
    for start in range(0, n - WINDOW_SIZE + 1, WINDOW_STEP):
        end = start + WINDOW_SIZE
        window_df = group_df.iloc[start:end]
        feature_row = compute_window_features(window_df)
        first_row = window_df.iloc[0]
        last_row = window_df.iloc[-1]
        row = {
            "factory_id": str(last_row["factory_id"]),
            "machine_id": str(last_row["machine_id"]),
            "device_id": str(last_row["device_id"]),
            "window_start_index": int(first_row["reading_index"]),
            "window_end_index": int(last_row["reading_index"]),
            "window_size": int(WINDOW_SIZE),
            "window_step": int(WINDOW_STEP),
            "window_start_ts": first_row["ts_gateway"].isoformat().replace("+00:00", "Z"),
            "window_end_ts": last_row["ts_gateway"].isoformat().replace("+00:00", "Z"),
        }
        row.update(feature_row)
        if "operating_state" in window_df.columns:
            row["operating_state_majority"] = window_df["operating_state"].astype(str).mode().iloc[0]
        if USE_LABEL_FILTER and LABEL_COLUMN in window_df.columns:
            row["window_label_majority"] = window_df[LABEL_COLUMN].astype(str).mode().iloc[0]
        rows.append(row)
    return rows


def build_window_feature_dataset(df: pd.DataFrame) -> pd.DataFrame:
    all_rows = []
    if GROUP_BY_DEVICE:
        grouped = df.groupby(["factory_id", "machine_id", "device_id"], dropna=False)
        for _, group_df in grouped:
            all_rows.extend(build_windows_for_group(group_df))
    else:
        df_sorted = df.sort_values(["reading_index", "ts_gateway"]).reset_index(drop=True)
        all_rows.extend(build_windows_for_group(df_sorted))
    if not all_rows:
        raise ValueError("No training windows were created. Check CSV size, grouping, and WINDOW_SIZE.")
    out = pd.DataFrame(all_rows)
    missing = [c for c in FEATURE_COLUMNS if c not in out.columns]
    if missing:
        raise ValueError(f"Missing expected feature columns after window extraction: {missing}")
    return out


def train_isolation_forest(X: pd.DataFrame) -> IsolationForest:
    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X)
    return model


def main():
    print(f"[INFO] Reading raw telemetry CSV: {INPUT_CSV}")
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    validate_input_columns(df)
    df = normalize_dtypes(df)
    df = basic_clean(df)
    df = filter_normal_only(df)

    if df.empty:
        raise ValueError("No usable rows left after cleaning/filtering.")

    print(f"[INFO] Raw usable rows: {len(df)}")
    feature_df = build_window_feature_dataset(df)
    print(f"[INFO] Windowed rows created: {len(feature_df)}")

    X = feature_df[FEATURE_COLUMNS].copy()
    ensure_parent_dir(OUTPUT_FEATURES_CSV)
    feature_df.to_csv(OUTPUT_FEATURES_CSV, index=False)
    print(f"[INFO] Saved window feature dataset to: {OUTPUT_FEATURES_CSV}")

    print("[INFO] Training IsolationForest...")
    model = train_isolation_forest(X)

    metadata = {
        "model_type": "IsolationForest",
        "input_type": "window_features",
        "feature_columns": FEATURE_COLUMNS,
        "window_size": WINDOW_SIZE,
        "window_step": WINDOW_STEP,
        "n_training_windows": int(len(feature_df)),
        "n_estimators": N_ESTIMATORS,
        "contamination": CONTAMINATION,
        "random_state": RANDOM_STATE,
        "group_by_device": GROUP_BY_DEVICE,
        "use_label_filter": USE_LABEL_FILTER,
        "label_column": LABEL_COLUMN if USE_LABEL_FILTER else None,
        "normal_label_value": NORMAL_LABEL_VALUE if USE_LABEL_FILTER else None,
    }

    artifact = {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
        "window_size": WINDOW_SIZE,
        "window_step": WINDOW_STEP,
        "metadata": metadata,
    }

    ensure_parent_dir(OUTPUT_MODEL)
    joblib.dump(artifact, OUTPUT_MODEL)
    print(f"[INFO] Saved model artifact to: {OUTPUT_MODEL}")
    print("[INFO] Training complete.")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
