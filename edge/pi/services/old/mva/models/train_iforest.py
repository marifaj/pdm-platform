# train_iforest.py — run on laptop/PC (NOT on Pi)
# Suggested env:
# pip install "scikit-learn==1.3.2" "skl2onnx==1.16.0" "onnx==1.16.0" numpy pandas
# (Optional sanity check) pip install onnxruntime

import json
import uuid
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

# ---------- 1) Load data ----------
CSV_PATH = "telemetry.csv"     # <-- put your path here
FEATURES = ["vib_rms", "power_w", "temp_c", "rpm"]

df = pd.read_csv(CSV_PATH)
missing = [c for c in FEATURES if c not in df.columns]
if missing:
    raise ValueError(f"Missing required columns in CSV: {missing}")

X = df[FEATURES].astype("float32").values
if len(X) < 10:
    raise ValueError("Too few training rows. Provide at least ~100 'normal' samples.")

# ---------- 2) Build + fit pipeline ----------
pipe = Pipeline(steps=[
    ("scaler", StandardScaler(with_mean=True, with_std=True)),
    ("iforest", IsolationForest(
        n_estimators=100,
        max_samples=min(256, len(X)),
        contamination="auto",   # or a small fixed value like 0.02
        random_state=42,
        n_jobs=-1
    ))
])

pipe.fit(X)

# ---------- 3) Calibrate score -> anomaly_score (0..1) ----------
# IsolationForest.score_samples: higher = more normal, lower = more anomalous.
# We store training distribution to map at the edge:
#   s_norm = (score - s_min) / (s_max - s_min) clipped to [0,1]
#   anomaly_score = 1 - s_norm
train_scores = pipe.score_samples(X).astype("float32")
s_min = float(np.min(train_scores))
s_max = float(np.max(train_scores))
if np.isclose(s_max, s_min):
    # degenerate case; avoid div-by-zero on edge
    s_max = s_min + 1e-6

# Some extra stats if you want them later
s_mean = float(np.mean(train_scores))
s_std  = float(np.std(train_scores, ddof=0))

# ---------- 4) Export ONNX (fix opset mismatch) ----------
# Keep main ('') ONNX opset at 15, force 'ai.onnx.ml' to 3
initial_type = [("input", FloatTensorType([None, len(FEATURES)]))]
onnx_model = convert_sklearn(
    pipe,
    initial_types=initial_type,
    target_opset={'': 15, 'ai.onnx.ml': 3}
)

ONNX_PATH = "iforest-v1.onnx"
with open(ONNX_PATH, "wb") as f:
    f.write(onnx_model.SerializeToString())

# ---------- 5) Sidecar model metadata for your edge service ----------
model_info = {
    "model_id": "iforest-edge",
    "model_ver": "1.0.0",
    "exported_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "export_run_id": str(uuid.uuid4()),
    "feature_order": FEATURES,
    "input_dtype": "float32",
    "preprocessing": {
        "type": "StandardScaler",
        # NOTE: values shown are for inspection only; ONNX already contains the scaler.
        "with_mean": True,
        "with_std": True
    },
    "estimator": {
        "type": "IsolationForest",
        "n_estimators": 100,
        "max_samples": int(min(256, len(X))),
        "contamination": "auto",
        "random_state": 42
    },
    # Edge mapping to get anomaly_score in [0,1]
    "score_calibration": {
        "method": "minmax_then_invert",
        "train_score_min": s_min,
        "train_score_max": s_max,
        "train_score_mean": s_mean,
        "train_score_std": s_std,
        "formula": "anomaly_score = 1 - clip((score - s_min)/(s_max - s_min), 0, 1)"
    },
    "onnx": {
        "input_name": "input",
        "target_opset": {"": 15, "ai.onnx.ml": 3},
        "path": ONNX_PATH
    }
}

with open("iforest-v1.modelinfo.json", "w", encoding="utf-8") as f:
    json.dump(model_info, f, indent=2)

print("✅ Exported ONNX:", ONNX_PATH)
print("📝 Wrote metadata: iforest-v1.modelinfo.json")

# ---------- 6) Optional ONNXRuntime smoke test ----------
try:
    import onnx
    import onnxruntime as ort

    onnx_model_loaded = onnx.load(ONNX_PATH)
    onnx.checker.check_model(onnx_model_loaded)

    sess = ort.InferenceSession(ONNX_PATH, providers=["CPUExecutionProvider"])
    inp_name = sess.get_inputs()[0].name

    # Use a few training rows to verify
    dummy = X[:5].astype("float32")
    outs = sess.run(None, {inp_name: dummy})

    # skl2onnx IsolationForest typically exposes 'label' and 'scores'
    out_meta = [o.name for o in sess.get_outputs()]
    print("ORT outputs:", out_meta)
    for i, arr in enumerate(outs):
        arr = np.asarray(arr)
        print(f"  out[{i}] shape={arr.shape} dtype={arr.dtype}")

    print("✅ ONNXRuntime smoke test passed.")
except Exception as e:
    print("ℹ️ Skipping ORT smoke test (install onnxruntime to run). Reason:", e)
