# train_iforest.py
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

# load
df = pd.read_csv("telemetry.csv")
features = ["vib_rms", "power_w", "temp_c", "rpm"]
X = df.loc[df.label == 0, features].astype("float32").values

# build pipeline
pipe = Pipeline([
    ("scaler", StandardScaler()),
    ("iforest", IsolationForest(n_estimators=100, contamination=0.05, random_state=42))
])
pipe.fit(X)

# export to ONNX
onnx_model = convert_sklearn(pipe, initial_types=[("float_input", FloatTensorType([None, len(features)]))])
with open("iforest-v1.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())

print("✅ Model trained and saved as iforest-v1.onnx")
