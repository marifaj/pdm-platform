MVA Edge-Only PdM – Raw Payload Scenarios (JSONL)
-------------------------------------------------
Dataset generated: Oct 2025
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
  Get-Content raw_normal.jsonl | % { mosquitto_pub -h localhost -t "mva/gateway/gw-01/ingress/motor-3/raw" -m $_ -q 1; Start-Sleep -Seconds 1 }
  Get-Content raw_burst.jsonl  | % { mosquitto_pub -h localhost -t "mva/gateway/gw-01/ingress/motor-3/raw" -m $_ -q 1; Start-Sleep -Milliseconds 200 }

Notes:
- Each line is a complete raw.payload JSON message
- msg_id/trace_id unique; meta.seq increments
- ts_device is ISO-8601 with milliseconds
