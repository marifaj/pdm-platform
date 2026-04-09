FILES
1. inference_app_temp_vibration.py
   New inference service for your current temp + vibration payload.

2. train_test_iforest_onnx.py
   Generates a TEST ONNX model with synthetic normal data.

3. requirements_inference_test.txt
   Packages you likely need.

IMPORTANT
- The ONNX model created here is only for pipeline testing.
- Replace it later with your real trained model.
- The inference service already contains EWMA fallback, so you can start even before ONNX works.
