# generate_telemetry.py
import numpy as np
import pandas as pd

def gen_signals(n=1000, anom_frac=0.1, seed=42):
    np.random.seed(seed)
    base_temp = 32.0
    base_rpm = 1800.0
    base_vib = 0.12
    base_pow = 250.0

    rows = []
    drift_t = 0.0
    for i in range(n):
        drift_t += 0.01
        rpm = base_rpm + 15.0*np.sin(drift_t*0.5) + np.random.uniform(-8, 8)
        temp = base_temp + 0.08*(rpm - base_rpm) + np.random.uniform(-0.3, 0.3)
        vib = base_vib + 0.002*abs(rpm - base_rpm) + np.random.uniform(-0.01, 0.01)
        poww = base_pow + 0.18*(rpm - base_rpm) + np.random.uniform(-3, 3)
        # inject anomaly
        is_anom = np.random.rand() < anom_frac
        if is_anom:
            vib += 0.25
        rows.append([vib, poww, temp, rpm, int(is_anom)])
    df = pd.DataFrame(rows, columns=["vib_rms", "power_w", "temp_c", "rpm", "label"])
    return df

df = gen_signals(1000, anom_frac=0.1)
df.to_csv("telemetry.csv", index=False)
print(df.head())
