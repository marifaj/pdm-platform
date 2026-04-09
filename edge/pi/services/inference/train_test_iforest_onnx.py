#!/usr/bin/env python3
"""
Create a TEST ONNX anomaly model for only:
- temperature_c
- vibration_mag_g

This does NOT use your real historical data.
It generates synthetic "mostly normal" data just so you can test the inference pipeline end to end.

Output:
    ~/mva/models/iforest-temp-vibration-v1.onnx

Requirements:
    pip install numpy scikit-learn skl2onnx onnx onnxruntime

Run:
    python3 train_test_iforest_onnx.py
"""
from pathlib import Path
import numpy as np
from sklearn.ensemble import IsolationForest
from skl2onnx import to_onnx
from skl2onnx.common.data_types import FloatTensorType

SEED = 42
OUT = Path.home() / "mva" / "models" / "iforest-temp-vibration-v1.onnx"
OUT.parent.mkdir(parents=True, exist_ok=True)

rng = np.random.default_rng(SEED)

# Synthetic "normal" data
n_normal = 1500
temp_normal = rng.normal(loc=24.0, scale=1.2, size=n_normal)
vib_normal = rng.normal(loc=0.85, scale=0.08, size=n_normal)

X_normal = np.column_stack([temp_normal, vib_normal]).astype(np.float32)

# Train a tiny test Isolation Forest
clf = IsolationForest(
    n_estimators=100,
    contamination=0.03,
    random_state=SEED,
)
clf.fit(X_normal)

# Convert to ONNX
initial_types = [("float_input", FloatTensorType([None, 2]))]
onx = to_onnx(clf, initial_types=initial_types, target_opset=15)

with open(OUT, "wb") as f:
    f.write(onx.SerializeToString())

print(f"Saved test ONNX model to: {OUT}")
