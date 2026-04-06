# train_iforest.py — run on laptop/PC (NOT on Pi)
# pip install scikit-learn==1.3.2 skl2onnx==1.16.0 onnx==1.16.0 numpy pandas
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

# 1) Load your *normal* data (one row per sample)
# Expect columns: vib_rms, power_w, temp_c, rpm
df = pd.read_csv("normal_telemetry.csv")  # <-- put your path here
features = ["vib_rms", "power_w", "temp_c", "rpm"]
X = df[features].astype("float32").values

# 2) Build pipeline
pipe = Pipeline(steps=[
    ("scaler", StandardScaler(with_mean=True, with_std=True)),
    ("iforest", IsolationForest(
        n_estimators=100,
        max_samples=min(256, len(X)),
        contamination="auto",   # or set a small fixed value like 0.02 if you prefer
        random_state=42
    ))
])

pipe.fit(X)

# 3) Convert to ONNX (float input shape: [None, 4])
initial_type = [("input", FloatTensorType([None, len(features)]))]
onnx_model = convert_sklearn(pipe, initial_types=initial_type, target_opset=15)

with open("iforest-v1.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())

print("✅ Exported ONNX: iforest-v1.onnx")
