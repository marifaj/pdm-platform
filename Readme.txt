Deployed structure of folders and files on Pi 4 should be like following:

~/mva/
├── data/
├── logs/
├── models/
│   └── iforest-temp-vibration-v1.onnx   (later)
├── pi/services/
│   ├── ingestion/app.py
│   ├── storage/app.py
│   ├── inference/app.py   ✅ (this one)
│   ├── event_processing/app.py
│   └── notification/app.py