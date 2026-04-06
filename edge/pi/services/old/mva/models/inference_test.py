# inference_test.py
import pandas as pd
import onnxruntime as rt
import numpy as np

df = pd.read_csv("telemetry.csv")
features = ["vib_rms", "power_w", "temp_c", "rpm"]

sess = rt.InferenceSession("iforest-v1.onnx", providers=["CPUExecutionProvider"])
input_name = sess.get_inputs()[0].name
output_name = sess.get_outputs()[0].name

# Pick one row that might be anomalous (label=1)
sample = df.loc[df.label == 1, features].iloc[[0]].astype("float32")
pred = sess.run(None, {input_name: sample.values})[0]
score = float(pred[0])

print(f"Sample: {sample.to_dict('records')[0]}")
print(f"Anomaly score = {score:.4f}")
