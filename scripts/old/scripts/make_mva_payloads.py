#!/usr/bin/env python3
"""
Generate MVA PdM JSONL scenarios (offline only).

Creates:
  ./mva_raw_payloads/
    - raw_normal.jsonl
    - raw_soft_fault.jsonl
    - raw_hard_fault.jsonl
    - raw_recovery.jsonl
    - raw_flapping.jsonl
    - raw_burst.jsonl
    - raw_all_scenarios.jsonl
    - README.txt
  ./mva_raw_payload_scenarios.zip  (archive of the above)

Replay example (Windows PowerShell):
  Get-Content raw_normal.jsonl | % { mosquitto_pub -h localhost -t "mva/gateway/gw-01/ingress/motor-3/raw" -m $_ -q 1; Start-Sleep -Seconds 1 }

Replay example (Linux/macOS):
  while IFS= read -r line; do mosquitto_pub -h localhost -t "mva/gateway/gw-01/ingress/motor-3/raw" -m "$line" -q 1; sleep 1; done < raw_normal.jsonl
"""

import os, json, uuid, random, zipfile, datetime

# ===========================
# Tunables
# ===========================
OUT_DIR = "mva_raw_payloads"
ZIP_NAME = "mva_raw_payload_scenarios.zip"

GATEWAY_ID = "gw-01"
DEVICE_ID  = "motor-3"
TENANT_ID  = "local"

SCHEMA_VER  = "1"
FEATURE_VER = "v1"

# Baselines (motor profile)
BASE_TEMP = 61.5
BASE_RPM  = 1475
BASE_VIB  = 0.17
BASE_POW  = 319.0
BASE_CUR  = 1.89

# Seeds / timeline
RANDOM_SEED = 42
START = datetime.datetime(2025, 1, 12, 10, 0, 0)

# ===========================
# Helpers
# ===========================
def iso(ts: datetime.datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def jitter(x: float, pct: float) -> float:
    """Return x with ±pct random jitter."""
    return x * (1 + random.uniform(-pct, pct))

def mk_msg(ts, seq, t, r, v, p, c, trace):
    """Build one raw.payload message."""
    return {
        "schema_ver": SCHEMA_VER,
        "feature_ver": FEATURE_VER,
        "msg_id": str(uuid.uuid4()),
        "trace_id": trace,
        "tenant_id": TENANT_ID,
        "device_id": DEVICE_ID,
        "gateway_id": GATEWAY_ID,
        "ts_device": iso(ts),
        "metrics": {
            "temp_c": round(t, 2),
            "rpm": int(r),
            "vib_rms": round(v, 3),
            "power_w": round(p, 1),
            "current_a": round(c, 2)
        },
        "meta": { "fw_ver": "esp32-pdm-v1.2.3", "rssi": -65, "seq": seq }
    }

# ===========================
# Scenario builders
# ===========================
def build_normal(start, n=20, hz=1.0):
    seq = 1
    dt = start
    step = datetime.timedelta(seconds=1.0/hz)
    t = BASE_TEMP
    trace = str(uuid.uuid4())
    out = []
    for _ in range(n):
        t += random.uniform(-0.05, 0.05)  # slow drift
        r = jitter(BASE_RPM, 0.005)
        v = jitter(BASE_VIB, 0.05)
        p = jitter(BASE_POW, 0.01)
        c = jitter(BASE_CUR, 0.02)
        out.append(mk_msg(dt, seq, t, r, v, p, c, trace))
        seq += 1; dt += step
    return out

def build_soft_fault(start, pre=8, anom=12, hz=1.0):
    seq = 1
    dt = start + datetime.timedelta(minutes=1)
    step = datetime.timedelta(seconds=1.0/hz)
    tr = str(uuid.uuid4())
    out = []
    # Pre-normal
    for _ in range(pre):
        t = jitter(BASE_TEMP, 0.003)
        r = jitter(BASE_RPM, 0.006)
        v = jitter(BASE_VIB, 0.05)
        p = jitter(BASE_POW, 0.01)
        c = jitter(BASE_CUR, 0.02)
        out.append(mk_msg(dt, seq, t, r, v, p, c, tr)); seq += 1; dt += step
    # Soft fault (vib +20%, slight rpm/power/current increases)
    for _ in range(anom):
        t = jitter(BASE_TEMP + 1.5, 0.005)
        r = jitter(BASE_RPM, 0.015)
        v = jitter(BASE_VIB * 1.2, 0.05)
        p = jitter(BASE_POW * 1.05, 0.01)
        c = jitter(BASE_CUR * 1.05, 0.02)
        out.append(mk_msg(dt, seq, t, r, v, p, c, tr)); seq += 1; dt += step
    return out

def build_hard_fault(start, pre=8, anom=30, hz=1.0):
    seq = 1
    dt = start + datetime.timedelta(minutes=2)
    step = datetime.timedelta(seconds=1.0/hz)
    tr = str(uuid.uuid4())
    out = []
    # Pre
    for _ in range(pre):
        t = BASE_TEMP; r = BASE_RPM; v = BASE_VIB; p = BASE_POW; c = BASE_CUR
        out.append(mk_msg(dt, seq, t, r, v, p, c, tr)); seq += 1; dt += step
    # Hard fault (vib +50%, temp +10C, strong rpm jitter, power/current up)
    for _ in range(anom):
        t = jitter(BASE_TEMP + 10.0, 0.01)
        r = jitter(BASE_RPM, 0.10)
        v = jitter(BASE_VIB * 1.5, 0.08)
        p = jitter(BASE_POW * 1.10, 0.02)
        c = jitter(BASE_CUR * 1.20, 0.03)
        out.append(mk_msg(dt, seq, t, r, v, p, c, tr)); seq += 1; dt += step
    return out

def build_recovery(start, n=12, hz=1.0):
    seq = 1
    dt = start + datetime.timedelta(minutes=4)
    step = datetime.timedelta(seconds=1.0/hz)
    tr = str(uuid.uuid4())
    out = []
    for _ in range(n):
        t = jitter(BASE_TEMP, 0.003)
        r = jitter(BASE_RPM, 0.006)
        v = jitter(BASE_VIB, 0.03)
        p = jitter(BASE_POW, 0.01)
        c = jitter(BASE_CUR, 0.02)
        out.append(mk_msg(dt, seq, t, r, v, p, c, tr)); seq += 1; dt += step
    return out

def build_flapping(start, n=30, hz=1.0):
    seq = 1
    dt = start + datetime.timedelta(minutes=5)
    step = datetime.timedelta(seconds=1.0/hz)
    tr = str(uuid.uuid4())
    out = []
    for i in range(n):
        factor = 1.2 + (0.02 * (1 if i % 2 == 0 else -1))  # around "high" threshold
        t = jitter(BASE_TEMP + 0.8, 0.004)
        r = jitter(BASE_RPM, 0.02)
        v = jitter(BASE_VIB * factor, 0.03)
        p = jitter(BASE_POW, 0.02)
        c = jitter(BASE_CUR, 0.02)
        out.append(mk_msg(dt, seq, t, r, v, p, c, tr)); seq += 1; dt += step
    return out

def build_burst(start, n=50, hz=5.0):
    seq = 1
    dt = start + datetime.timedelta(minutes=6)
    step = datetime.timedelta(seconds=1.0/hz)
    tr = str(uuid.uuid4())
    out = []
    for _ in range(n):
        t = jitter(BASE_TEMP, 0.005)
        r = jitter(BASE_RPM, 0.01)
        v = jitter(BASE_VIB, 0.04)
        p = jitter(BASE_POW, 0.01)
        c = jitter(BASE_CUR, 0.02)
        out.append(mk_msg(dt, seq, t, r, v, p, c, tr)); seq += 1; dt += step
    return out

# ===========================
# Main
# ===========================
def main():
    random.seed(RANDOM_SEED)
    os.makedirs(OUT_DIR, exist_ok=True)

    scenarios = {
        "raw_normal.jsonl":   build_normal(START),
        "raw_soft_fault.jsonl":  build_soft_fault(START),
        "raw_hard_fault.jsonl":  build_hard_fault(START),
        "raw_recovery.jsonl":    build_recovery(START),
        "raw_flapping.jsonl":    build_flapping(START),
        "raw_burst.jsonl":       build_burst(START),
    }

    # Write each scenario file
    all_msgs = []
    for fname, rows in scenarios.items():
        path = os.path.join(OUT_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            for m in rows:
                f.write(json.dumps(m) + "\n")
        all_msgs.extend(rows)

    # Combined file
    combined = os.path.join(OUT_DIR, "raw_all_scenarios.jsonl")
    with open(combined, "w", encoding="utf-8") as f:
        for m in all_msgs:
            f.write(json.dumps(m) + "\n")

    # README
    readme = f"""MVA Edge-Only PdM – Raw Payload Scenarios (JSONL)
-------------------------------------------------
Dataset generated: {datetime.datetime.utcnow():%b %Y}
Version: v1
Author: Mentor Arifaj

Files:
- raw_normal.jsonl        : Baseline 1 Hz
- raw_soft_fault.jsonl    : vib_rms +20% for 12s
- raw_hard_fault.jsonl    : vib_rms +50%, temp +10°C, rpm jitter for 30s
- raw_recovery.jsonl      : return to baseline (12 samples)
- raw_flapping.jsonl      : oscillation around high threshold (30s)
- raw_burst.jsonl         : 5 Hz burst for 10s
- raw_all_scenarios.jsonl : concatenation of all above

Usage (Windows PowerShell, local broker):
  Get-Content raw_normal.jsonl | % {{ mosquitto_pub -h localhost -t "mva/gateway/gw-01/ingress/motor-3/raw" -m $_ -q 1; Start-Sleep -Seconds 1 }}
  Get-Content raw_burst.jsonl  | % {{ mosquitto_pub -h localhost -t "mva/gateway/gw-01/ingress/motor-3/raw" -m $_ -q 1; Start-Sleep -Milliseconds 200 }}

Notes:
- Each line is a complete raw.payload JSON message
- msg_id/trace_id unique; meta.seq increments
- ts_device is ISO-8601 with milliseconds
"""
    readme_path = os.path.join(OUT_DIR, "README.txt")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(readme)

    # ZIP everything
    zip_path = os.path.abspath(ZIP_NAME)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for fname in os.listdir(OUT_DIR):
            z.write(os.path.join(OUT_DIR, fname), fname)

    # Print results
    print("\n[OK] Generated files in:", os.path.abspath(OUT_DIR))
    for fname in sorted(os.listdir(OUT_DIR)):
        print("  -", fname)
    print("\n[OK] ZIP created at:", zip_path)
    print("\nReplay (Linux/macOS):")
    print('  while IFS= read -r line; do mosquitto_pub -h localhost -t "mva/gateway/gw-01/ingress/motor-3/raw" -m "$line" -q 1; sleep 1; done < raw_normal.jsonl')

if __name__ == "__main__":
    main()
