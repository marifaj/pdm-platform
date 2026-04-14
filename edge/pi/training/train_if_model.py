#!/usr/bin/env python3
"""
Train first Isolation Forest model using healthy idle + load telemetry.

Input files:
- ~/mva/data/idle.csv
- ~/mva/data/load.csv

Outputs:
- ~/mva/models/if_model.joblib
- ~/mva/models/if_model_metadata.json
- ~/mva/data/healthy_combined_for_training.csv

This first version trains on row-level features:
- temperature_c
- x_g
- y_g
- z_g
- vibration_mag_g

Later, you can evolve this to window-based features.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler


# ============================================================
# CONFIG
# ============================================================
MVA_HOME = Path(os.path.expanduser("~/mva"))
DATA_DIR = MVA_HOME / "data"
MODELS_DIR = MVA_HOME / "models"

IDLE_CSV = DATA_DIR / "idle.csv"
LOAD_CSV = DATA_DIR / "load.csv"

COMBINED_CSV = DATA_DIR / "healthy_combined_for_training.csv"
MODEL_FILE = MODELS_DIR / "if_model.joblib"
METADATA_FILE = MODELS_DIR / "if_model_metadata.json"

FEATURE_COLUMNS: List[str] = [
    "temperature_c",
    "x_g",
    "y_g",
    "z_g",
    "vibration_mag_g",
]

# Isolation Forest settings
RANDOM_STATE = 42
N_ESTIMATORS = 200
CONTAMINATION = 0.01  # small value because training data is intended to be healthy
MAX_SAMPLES = "auto"


# ============================================================
# METADATA STRUCTURE
# ============================================================
@dataclass
class ModelMetadata:
    model_type: str
    created_utc: str
    feature_columns: List[str]
    idle_csv: str
    load_csv: str
    combined_rows: int
    idle_rows: int
    load_rows: int
    training_conditions: List[str]
    random_state: int
    n_estimators: int
    contamination: float
    max_samples: str
    notes: str


# ============================================================
# HELPERS
# ============================================================
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_columns(df: pd.DataFrame, file_label: str) -> None:
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{file_label} is missing required columns: {missing}")


def clean_dataframe(df: pd.DataFrame, source_label: str) -> pd.DataFrame:
    df = df.copy()

    # Keep original source label for traceability
    df["source_label"] = source_label

    # Parse timestamp if present
    if "ts_gateway" in df.columns:
        df["ts_gateway"] = pd.to_datetime(df["ts_gateway"], errors="coerce", utc=True)

    # Force feature columns numeric
    for col in FEATURE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where all features are missing
    df = df.dropna(subset=FEATURE_COLUMNS, how="all")

    # Optional sanity filtering
    # Keep temperature in plausible range
    df = df[(df["temperature_c"].isna()) | ((df["temperature_c"] >= -40) & (df["temperature_c"] <= 150))]

    # Keep vibration magnitude non-negative if present
    df = df[(df["vibration_mag_g"].isna()) | (df["vibration_mag_g"] >= 0)]

    return df


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    df = pd.read_csv(path)
    validate_columns(df, label)
    df = clean_dataframe(df, label)
    return df


def build_training_dataframe(idle_df: pd.DataFrame, load_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([idle_df, load_df], ignore_index=True)

    # Drop duplicate readings if exact duplicates exist
    subset_cols = [c for c in ["reading_index", "ts_gateway", *FEATURE_COLUMNS] if c in combined.columns]
    if subset_cols:
        combined = combined.drop_duplicates(subset=subset_cols)

    # Sort by time if timestamp exists
    if "ts_gateway" in combined.columns:
        combined = combined.sort_values("ts_gateway").reset_index(drop=True)

    return combined


def train_model(df: pd.DataFrame) -> Pipeline:
    X = df[FEATURE_COLUMNS].copy()

    pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "model",
                IsolationForest(
                    n_estimators=N_ESTIMATORS,
                    contamination=CONTAMINATION,
                    max_samples=MAX_SAMPLES,
                    random_state=RANDOM_STATE,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    pipeline.fit(X)
    return pipeline


def evaluate_training_distribution(model: Pipeline, df: pd.DataFrame) -> pd.DataFrame:
    X = df[FEATURE_COLUMNS].copy()

    # decision_function: higher = more normal, lower = more anomalous
    scores = model.decision_function(X)
    preds = model.predict(X)  # 1 = inlier, -1 = outlier

    result = df.copy()
    result["if_score"] = scores
    result["if_pred"] = preds
    result["is_outlier"] = (preds == -1).astype(int)
    return result


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading input files...")
    idle_df = load_csv(IDLE_CSV, "idle")
    load_df = load_csv(LOAD_CSV, "load")

    print(f"Idle rows loaded: {len(idle_df):,}")
    print(f"Load rows loaded: {len(load_df):,}")

    combined_df = build_training_dataframe(idle_df, load_df)
    print(f"Combined healthy rows: {len(combined_df):,}")

    # Save combined healthy dataset snapshot for traceability
    combined_df.to_csv(COMBINED_CSV, index=False)
    print(f"Saved combined healthy dataset: {COMBINED_CSV}")

    print("Training Isolation Forest...")
    model = train_model(combined_df)

    print("Evaluating training distribution...")
    scored_df = evaluate_training_distribution(model, combined_df)

    outlier_rate = scored_df["is_outlier"].mean() * 100.0
    print(f"Training outlier rate: {outlier_rate:.2f}%")

    score_summary = scored_df["if_score"].describe()
    print("\nIsolation Forest score summary:")
    print(score_summary.to_string())

    # Save model
    joblib.dump(model, MODEL_FILE)
    print(f"\nSaved model: {MODEL_FILE}")

    metadata = ModelMetadata(
        model_type="IsolationForest",
        created_utc=utc_now(),
        feature_columns=FEATURE_COLUMNS,
        idle_csv=str(IDLE_CSV),
        load_csv=str(LOAD_CSV),
        combined_rows=int(len(combined_df)),
        idle_rows=int(len(idle_df)),
        load_rows=int(len(load_df)),
        training_conditions=["idle", "load"],
        random_state=RANDOM_STATE,
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        max_samples=str(MAX_SAMPLES),
        notes=(
            "First row-level healthy-behavior model. "
            "Trained only on normal idle and normal load data. "
            "Later version should use window-based features."
        ),
    )

    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(asdict(metadata), f, indent=2)

    print(f"Saved metadata: {METADATA_FILE}")

    # Optional: save a scored sample for inspection
    scored_preview_path = DATA_DIR / "healthy_scored_preview.csv"
    scored_df.head(5000).to_csv(scored_preview_path, index=False)
    print(f"Saved scored preview: {scored_preview_path}")

    print("\nTraining completed successfully.")


if __name__ == "__main__":
    main()