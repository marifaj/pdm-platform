#!/usr/bin/env python3
"""
Event Processing — Edge-only MVA (spec-aligned)

- Subscribes: mva/gateway/{gateway_id}/pred/+
- Applies:    hysteresis + thresholds → OPEN/UPSERT incidents, RESOLVE when normal
- Emits:      mva/gateway/{gateway_id}/event/{device_id}
- Persists:   events, incidents in SQLite

Thresholds config (JSON): ~/mva/storage/config/thresholds.json
{
  "thresholds_by_type": {"motor": {"warn":0.70, "high":0.80, "crit":0.90}},
  "hysteresis": {"open_N":3, "resolve_M":10, "cooldown_s":600}
}
"""
import os, json, time, sqlite3, signal, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
import paho.mqtt.client as mqtt

# ======== ENV AUTO-LOAD (.env) ========
try:
    from dotenv import load_dotenv  # type: ignore
    env_path = Path.home() / "mva" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except Exception:
    pass

# ======== CONFIG ========
GATEWAY_ID   = os.getenv("GATEWAY_ID", "gw-01")
MQTT_HOST    = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT    = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER    = os.getenv("MQTT_USER", "")
MQTT_PASS    = os.getenv("MQTT_PASS", "")

PRED_SUB     = f"mva/gateway/{GATEWAY_ID}/pred/+"
EVENT_PUB    = f"mva/gateway/{GATEWAY_ID}/event/{{device_id}}"

DB_PATH      = os.path.expanduser("~/mva/data/mva.db")
CFG_PATH     = os.path.expanduser("~/mva/storage/config/thresholds.json")
LOG_PATH     = os.path.expanduser("~/mva/logs/event_processing.log")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# ======== DB ========
con = sqlite3.connect(DB_PATH, check_same_thread=False)
con.execute("PRAGMA journal_mode=WAL;")
con.execute("PRAGMA synchronous=NORMAL;")
con.execute("PRAGMA temp_store=MEMORY;")
cur = con.cursor()

def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def bucket_date(ts_iso):
    return ts_iso[:10]

def log(s):
    line = f"{iso_now()} event-proc {s}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ======== Introspect live schema (tolerant to partial migrations) ========
def table_has_column(table, col):
    try:
        rows = cur.execute(f"PRAGMA table_info({table});").fetchall()
        return any(r[1] == col for r in rows)
    except Exception:
        return False

INC_HAS_TRACE   = table_has_column("incidents", "trace_id")
INC_HAS_META    = table_has_column("incidents", "meta_json")
INC_HAS_SMIN    = table_has_column("incidents", "score_min")
INC_HAS_SMAX    = table_has_column("incidents", "score_max")
EVT_HAS_TRACE   = table_has_column("events",    "trace_id")

# ======== Load thresholds & hysteresis ========
cfg = {
  "thresholds_by_type": {"motor": {"warn":0.70,"high":0.80,"crit":0.90}},
  "hysteresis": {"open_N":3,"resolve_M":10,"cooldown_s":600}
}
if os.path.exists(CFG_PATH):
    try:
        with open(CFG_PATH) as f:
            user = json.load(f)
            # shallow merge
            for k, v in user.items():
                if isinstance(v, dict) and k in cfg:
                    cfg[k].update(v)
                else:
                    cfg[k] = v
    except Exception as e:
        log(f"[WARN] failed to load {CFG_PATH}: {e}")

open_N    = int(cfg["hysteresis"].get("open_N", 3))
resolve_M = int(cfg["hysteresis"].get("resolve_M", 10))
cooldown  = int(cfg["hysteresis"].get("cooldown_s", 600))
th_motor  = cfg["thresholds_by_type"].get("motor", {"warn":0.70,"high":0.80,"crit":0.90})

# ======== Runtime state (per-device) ========
state = {}  # device_id -> dict(anom_run, norm_run, cooldown_until)

def get_state(dev):
    if dev not in state:
        state[dev] = {"anom_run":0, "norm_run":0, "cooldown_until":None}
    return state[dev]

def rank(sev):
    # info < medium < high < critical
    order = {"info":0,"low":1,"medium":2,"high":3,"critical":4}
    return order.get(sev, 0)

def severity_from_score(s):
    # Check highest first
    if s >= th_motor.get("crit", 0.90): return "critical"
    if s >= th_motor.get("high", 0.80): return "high"
    if s >= th_motor.get("warn", 0.70): return "medium"
    return "info"

def incident_id_for(dev):  # deterministic id per device
    return f"INC-{dev}"

def jdump(obj):
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

# ======== Incident UPSERT with enriched fields ========
def upsert_incident(dev, sev, rule_id, now_iso, score, trace):
    inc_id = incident_id_for(dev)

    # Build SELECT columns based on availability
    sel_cols = ["incident_id","status","severity_current","severity_peak",
                "opened_at","last_seen_at","occurrences","rule_id"]
    if INC_HAS_TRACE: sel_cols.append("trace_id")
    if INC_HAS_META:  sel_cols.append("meta_json")
    if INC_HAS_SMIN:  sel_cols.append("score_min")
    if INC_HAS_SMAX:  sel_cols.append("score_max")

    row = cur.execute(f"SELECT {', '.join(sel_cols)} FROM incidents WHERE incident_id=?",
                      (inc_id,)).fetchone()

    if row is None:
        # INSERT new incident
        cols = ["incident_id","device_id","status","severity_current","severity_peak",
                "opened_at","last_seen_at","occurrences","rule_id","cleared_by","cleared_at"]
        vals = [ inc_id,      dev,        "OPEN",  sev,               sev,
                 now_iso,     now_iso,     1,       rule_id,          None,      None ]

        # Optional fields
        if INC_HAS_TRACE:
            cols.append("trace_id"); vals.append(trace)
        if INC_HAS_META:
            meta = {"trace_id": trace, "first_score": score, "last_score": score,
                    "peak_score": score, "last_scores":[score]}
            cols.append("meta_json"); vals.append(jdump(meta))
        if INC_HAS_SMIN:
            cols.append("score_min"); vals.append(score)
        if INC_HAS_SMAX:
            cols.append("score_max"); vals.append(score)

        placeholders = ",".join(["?"] * len(vals))
        cur.execute(f"INSERT INTO incidents ({', '.join(cols)}) VALUES ({placeholders})", vals)

        sql = f"INSERT INTO incidents ({', '.join(cols)}) VALUES ({placeholders})"
        cur.execute(sql, vals)

    else:
        # UPDATE existing
        idx = 0
        status        = row[idx+1];  idx += 1  # status
        sev_cur       = row[idx+1];  idx += 1  # severity_current
        sev_peak      = row[idx+1];  idx += 1  # severity_peak
        opened_at     = row[idx+1];  idx += 1
        last_seen_at  = row[idx+1];  idx += 1
        occurrences   = row[idx+1];  idx += 1
        rule_id_db    = row[idx+1];  idx += 1

        trace_old = meta_old = None
        smin = smax = None
        if INC_HAS_TRACE:
            trace_old = row[idx+1]; idx += 1
        if INC_HAS_META:
            meta_old = row[idx+1]; idx += 1
        if INC_HAS_SMIN:
            smin = row[idx+1]; idx += 1
        if INC_HAS_SMAX:
            smax = row[idx+1]; idx += 1

        # Peak severity
        sev_peak_new = sev_peak
        if rank(sev) > rank(sev_peak or "info"):
            sev_peak_new = sev

        occ_new = int(occurrences or 0) + 1

        # Score bounds
        smin_new = score if smin is None else min(smin, score)
        smax_new = score if smax is None else max(smax, score)

        # Meta merge
        meta_new_json = None
        if INC_HAS_META:
            try:
                m = json.loads(meta_old) if meta_old else {}
            except Exception:
                m = {}
            first = m.get("first_score", score)
            peak  = max(m.get("peak_score", score), score)
            lst   = m.get("last_scores", [])
            lst   = (lst + [score])[-10:]
            m2 = {"trace_id": (trace or m.get("trace_id")),
                  "first_score": first,
                  "last_score":  score,
                  "peak_score":  peak,
                  "last_scores": lst}
            meta_new_json = jdump(m2)

        assignments = [
            ("status",           "OPEN"),
            ("severity_current", sev),
            ("severity_peak",    sev_peak_new),
            ("last_seen_at",     now_iso),
            ("occurrences",      occ_new),
            ("rule_id",          rule_id),
        ]
        if INC_HAS_TRACE: assignments.append(("trace_id", trace or trace_old))
        if INC_HAS_META:  assignments.append(("meta_json", meta_new_json))
        if INC_HAS_SMIN:  assignments.append(("score_min", smin_new))
        if INC_HAS_SMAX:  assignments.append(("score_max", smax_new))

        set_sql = ", ".join(f"{k}=?" for k,_ in assignments)
        params  = [v for _,v in assignments] + [inc_id]
        cur.execute(f"UPDATE incidents SET {set_sql} WHERE incident_id=?", params)

    con.commit()
    return inc_id

def resolve_incident(dev, now_iso):
    inc_id = incident_id_for(dev)
    cur.execute("SELECT incident_id FROM incidents WHERE incident_id=?", (inc_id,))
    if cur.fetchone():
        cur.execute(
            "UPDATE incidents SET status=?, cleared_by=?, cleared_at=?, last_seen_at=? WHERE incident_id=?",
            ("RESOLVED", "AUTO", now_iso, now_iso, inc_id)
        )
        con.commit()

def publish_event(client, dev, sev, score, rule_id, now_iso, occurrences, hysteresis_applied, cooldown_applied, trace_id, msg_id):
    payload = {
      "schema_ver": "1",
      "trace_id": trace_id,
      "msg_id": msg_id,
      "device_id": dev,
      "gateway_id": GATEWAY_ID,
      "ts_utc": now_iso,
      "severity": sev,
      "rule_id": rule_id,
      "thresholds": {"warn": th_motor.get("warn",0.70), "high": th_motor.get("high",0.80), "crit": th_motor.get("crit",0.90)},
      "first_seen": None,
      "last_seen":  now_iso,
      "occurrences": occurrences,
      "hysteresis_applied": hysteresis_applied,
      "cooldown_applied": cooldown_applied,
      "actionable_hint": "Check vibration and load patterns if frequent highs."
    }
    topic = EVENT_PUB.format(device_id=dev)
    client.publish(topic, json.dumps(payload, separators=(",",":")), qos=0, retain=False)

# ======== MQTT HANDLERS ========
def on_connect(client, u, flags, rc, p=None):
    log(f"[MQTT] Connected rc={rc}; sub {PRED_SUB}")
    client.subscribe(PRED_SUB, qos=0)

def on_message(client, u, msg):
    try:
        d = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception as e:
        log(f"[WARN] bad json: {e}")
        return

    dev    = d.get("device_id", "unknown")
    score  = float(d.get("anomaly_score", 0.0))
    sev    = severity_from_score(score)
    nowi   = iso_now()
    s      = get_state(dev)

    # Cooldown guard
    cooldown_applied = False
    if s["cooldown_until"] and datetime.now(timezone.utc) < s["cooldown_until"]:
        cooldown_applied = True

    # Hysteresis counters
    if rank(sev) >= rank("medium"):
        s["anom_run"] += 1
        s["norm_run"]  = 0
    else:
        s["norm_run"] += 1
        s["anom_run"]  = 0

    rule_id = "default-001"

    # Open/Upsert incident when anomalous run sustained and not in cooldown
    if s["anom_run"] >= open_N and not cooldown_applied:
        inc_id = upsert_incident(
            dev=dev,
            sev=sev,
            rule_id=rule_id,
            now_iso=nowi,
            score=score,
            trace=d.get("trace_id")
        )

        # Persist event row (best-effort: with or without events.trace_id)
        evt_common = (
            d.get("msg_id"), dev, nowi, sev, rule_id,
            json.dumps({"warn": th_motor.get("warn",0.70),
                        "high": th_motor.get("high",0.80),
                        "crit": th_motor.get("crit",0.90)}),
            json.dumps({"score": score, "model_id": d.get("model_id"), "model_ver": d.get("model_ver"),
                        "trace_id": d.get("trace_id")}),
            bucket_date(nowi)
        )
        try:
            if EVT_HAS_TRACE:
                cur.execute(
                    "INSERT INTO events(msg_id, device_id, ts_utc, severity, rule_id, thresholds_json, meta_json, bucket_date, trace_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    evt_common + (d.get("trace_id"),)
                )
            else:
                cur.execute(
                    "INSERT INTO events(msg_id, device_id, ts_utc, severity, rule_id, thresholds_json, meta_json, bucket_date) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    evt_common
                )
            con.commit()
        except Exception as e:
            log(f"[ERR] insert event: {e}")

        # Notify downstream
        publish_event(
            client, dev, sev, score, rule_id, nowi,
            s["anom_run"], True, cooldown_applied,
            d.get("trace_id"), d.get("msg_id")
        )

    # Resolution when enough normal samples
    if s["norm_run"] >= resolve_M:
        resolve_incident(dev, nowi)
        s["norm_run"] = 0
        s["anom_run"] = 0
        s["cooldown_until"] = datetime.now(timezone.utc) + timedelta(seconds=cooldown)

def on_disconnect(client, u, rc, p=None):
    log(f"[MQTT] Disconnected rc={rc}")

def main():
    client = mqtt.Client(
        client_id=f"event-proc-{GATEWAY_ID}",
        protocol=mqtt.MQTTv311,
        transport="tcp",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    def shutdown(signum, frame):
        log("[SYS] shutting down")
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            con.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    client.loop_forever()

if __name__ == "__main__":
    main()
