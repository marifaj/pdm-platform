#!/usr/bin/env python3

import json
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import paho.mqtt.client as mqtt

# ---- optional .env auto-load ----
try:
    from dotenv import load_dotenv  # type: ignore
    env_path = Path.home() / "mva" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except Exception:
    pass


# ============================================================
# CONFIG
# ============================================================
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

INPUT_TOPIC = os.getenv("EVENT_PROC_INPUT_TOPIC", "mva/inference/telemetry")
OUTPUT_TOPIC = os.getenv("EVENT_PROC_OUTPUT_TOPIC", "mva/event/telemetry")

DB_PATH = os.path.expanduser(os.getenv("EVENT_PROC_DB_PATH", "~/mva/data/mva.db"))
LOG_PATH = os.path.expanduser(os.getenv("EVENT_PROC_LOG_PATH", "~/mva/logs/event_processing.log"))

# Hysteresis / incident behavior
OPEN_N = int(os.getenv("EVENT_OPEN_N", "3"))          # consecutive anomaly windows to open/update
RESOLVE_M = int(os.getenv("EVENT_RESOLVE_M", "10"))   # consecutive normal windows to resolve
COOLDOWN_S = int(os.getenv("EVENT_COOLDOWN_S", "60")) # after real resolution, pause re-open for this many seconds

DEBUG_LOG_EVERY_MESSAGE = os.getenv("EVENT_DEBUG_EVERY_MESSAGE", "true").lower() == "true"

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)


# ============================================================
# LOGGING
# ============================================================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def log(msg: str) -> None:
    line = f"{now_iso()} event-proc {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ============================================================
# SQLITE
# ============================================================
def connect_db():
    con = sqlite3.connect(
        DB_PATH,
        timeout=30,
        check_same_thread=False,
    )
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA busy_timeout=30000;")
    return con


def ensure_tables(con):
    # Keep old schema for compatibility, but add window columns
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_event TEXT NOT NULL,
          ts_inference TEXT NOT NULL,
          ts_gateway TEXT NOT NULL,
          factory_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          device_id TEXT NOT NULL,
          reading_index INTEGER NOT NULL,

          severity TEXT NOT NULL,
          anomaly_reason TEXT NOT NULL,

          if_score REAL,
          if_pred INTEGER,
          is_anomaly_ml INTEGER,
          temp_zscore REAL,
          is_anomaly_ewma INTEGER,
          is_anomaly_final INTEGER,

          payload_json TEXT
        )
        """
    )

    # Backward-compatible schema evolution
    con.execute("ALTER TABLE events ADD COLUMN window_start_ts TEXT")
    con.execute("ALTER TABLE events ADD COLUMN window_end_ts TEXT")
    con.execute("ALTER TABLE events ADD COLUMN window_start_index INTEGER")
    con.execute("ALTER TABLE events ADD COLUMN window_end_index INTEGER")
    con.execute("ALTER TABLE events ADD COLUMN window_size INTEGER")
    con.execute("ALTER TABLE events ADD COLUMN window_step INTEGER")
    con.execute("ALTER TABLE events ADD COLUMN temp_mean REAL")
    con.execute("ALTER TABLE events ADD COLUMN temp_std REAL")
    con.execute("ALTER TABLE events ADD COLUMN vib_mean REAL")
    con.execute("ALTER TABLE events ADD COLUMN vib_std REAL")
    con.execute("ALTER TABLE events ADD COLUMN vib_rms REAL")

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_events_device_ts
        ON events(device_id, ts_event)
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_events_reading
        ON events(reading_index)
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS incidents (
          incident_id TEXT PRIMARY KEY,
          factory_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          device_id TEXT NOT NULL,

          status TEXT NOT NULL,
          severity_current TEXT NOT NULL,
          severity_peak TEXT NOT NULL,

          anomaly_reason_first TEXT,
          anomaly_reason_last TEXT,

          opened_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          resolved_at TEXT,

          occurrences INTEGER NOT NULL DEFAULT 1,

          reading_index_first INTEGER,
          reading_index_last INTEGER,

          if_score_min REAL,
          if_score_max REAL,
          temp_zscore_max REAL,

          payload_json TEXT
        )
        """
    )

    # Backward-compatible schema evolution
    con.execute("ALTER TABLE incidents ADD COLUMN window_start_index_first INTEGER")
    con.execute("ALTER TABLE incidents ADD COLUMN window_end_index_first INTEGER")
    con.execute("ALTER TABLE incidents ADD COLUMN window_start_index_last INTEGER")
    con.execute("ALTER TABLE incidents ADD COLUMN window_end_index_last INTEGER")
    con.execute("ALTER TABLE incidents ADD COLUMN window_start_ts_first TEXT")
    con.execute("ALTER TABLE incidents ADD COLUMN window_end_ts_first TEXT")
    con.execute("ALTER TABLE incidents ADD COLUMN window_start_ts_last TEXT")
    con.execute("ALTER TABLE incidents ADD COLUMN window_end_ts_last TEXT")

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_incidents_device_status
        ON incidents(device_id, status)
        """
    )

    con.commit()


def safe_alter(con, sql: str):
    try:
        con.execute(sql)
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if "duplicate column name" in msg:
            return
        raise


def ensure_tables_safe(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_event TEXT NOT NULL,
          ts_inference TEXT NOT NULL,
          ts_gateway TEXT NOT NULL,
          factory_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          device_id TEXT NOT NULL,
          reading_index INTEGER NOT NULL,

          severity TEXT NOT NULL,
          anomaly_reason TEXT NOT NULL,

          if_score REAL,
          if_pred INTEGER,
          is_anomaly_ml INTEGER,
          temp_zscore REAL,
          is_anomaly_ewma INTEGER,
          is_anomaly_final INTEGER,

          payload_json TEXT
        )
        """
    )

    safe_alter(con, "ALTER TABLE events ADD COLUMN window_start_ts TEXT")
    safe_alter(con, "ALTER TABLE events ADD COLUMN window_end_ts TEXT")
    safe_alter(con, "ALTER TABLE events ADD COLUMN window_start_index INTEGER")
    safe_alter(con, "ALTER TABLE events ADD COLUMN window_end_index INTEGER")
    safe_alter(con, "ALTER TABLE events ADD COLUMN window_size INTEGER")
    safe_alter(con, "ALTER TABLE events ADD COLUMN window_step INTEGER")
    safe_alter(con, "ALTER TABLE events ADD COLUMN temp_mean REAL")
    safe_alter(con, "ALTER TABLE events ADD COLUMN temp_std REAL")
    safe_alter(con, "ALTER TABLE events ADD COLUMN vib_mean REAL")
    safe_alter(con, "ALTER TABLE events ADD COLUMN vib_std REAL")
    safe_alter(con, "ALTER TABLE events ADD COLUMN vib_rms REAL")

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_events_device_ts
        ON events(device_id, ts_event)
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_events_reading
        ON events(reading_index)
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS incidents (
          incident_id TEXT PRIMARY KEY,
          factory_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          device_id TEXT NOT NULL,

          status TEXT NOT NULL,
          severity_current TEXT NOT NULL,
          severity_peak TEXT NOT NULL,

          anomaly_reason_first TEXT,
          anomaly_reason_last TEXT,

          opened_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          resolved_at TEXT,

          occurrences INTEGER NOT NULL DEFAULT 1,

          reading_index_first INTEGER,
          reading_index_last INTEGER,

          if_score_min REAL,
          if_score_max REAL,
          temp_zscore_max REAL,

          payload_json TEXT
        )
        """
    )

    safe_alter(con, "ALTER TABLE incidents ADD COLUMN window_start_index_first INTEGER")
    safe_alter(con, "ALTER TABLE incidents ADD COLUMN window_end_index_first INTEGER")
    safe_alter(con, "ALTER TABLE incidents ADD COLUMN window_start_index_last INTEGER")
    safe_alter(con, "ALTER TABLE incidents ADD COLUMN window_end_index_last INTEGER")
    safe_alter(con, "ALTER TABLE incidents ADD COLUMN window_start_ts_first TEXT")
    safe_alter(con, "ALTER TABLE incidents ADD COLUMN window_end_ts_first TEXT")
    safe_alter(con, "ALTER TABLE incidents ADD COLUMN window_start_ts_last TEXT")
    safe_alter(con, "ALTER TABLE incidents ADD COLUMN window_end_ts_last TEXT")

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_incidents_device_status
        ON incidents(device_id, status)
        """
    )

    con.commit()


def retry_db_write(fn, *args, **kwargs):
    for attempt in range(5):
        try:
            return fn(*args, **kwargs)
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.05 * (attempt + 1))
            else:
                raise
    raise sqlite3.OperationalError("database remained locked after retries")


db_con = connect_db()
ensure_tables_safe(db_con)
log(f"[BOOT] events/incidents tables ensured at {DB_PATH}")


# ============================================================
# RUNTIME STATE
# ============================================================
state = {}
# state[device_id] = {
#   "anom_run": int,
#   "norm_run": int,
#   "cooldown_until": datetime | None,
# }


def get_state(device_id: str):
    if device_id not in state:
        state[device_id] = {
            "anom_run": 0,
            "norm_run": 0,
            "cooldown_until": None,
        }
    return state[device_id]


# ============================================================
# HELPERS
# ============================================================
def severity_rank(sev: str) -> int:
    order = {"info": 0, "medium": 1, "high": 2, "critical": 3}
    return order.get(sev, 0)


def severity_from_result(d: dict) -> str:
    reason = d.get("anomaly_reason", "normal")
    is_final = int(d.get("is_anomaly_final", 0))
    temp_zscore = abs(float(d.get("temp_zscore", 0.0)))
    is_ml = int(d.get("is_anomaly_ml", 0))
    is_ewma = int(d.get("is_anomaly_ewma", 0))

    if is_final == 0:
        return "info"

    if reason == "ml_and_ewma":
        return "critical"

    if reason == "ml_only":
        return "high"

    if reason == "ewma_only":
        if temp_zscore >= 4.0:
            return "high"
        return "medium"

    if is_ml and is_ewma:
        return "critical"
    if is_ml:
        return "high"
    if is_ewma:
        return "medium"
    return "info"


def incident_id_for(device_id: str) -> str:
    return f"INC-{device_id}"


def normalize_window_payload(d: dict) -> dict:
    """
    Convert windowed inference payload into a backward-compatible shape
    while preserving full window metadata.
    """
    out = dict(d)

    # Legacy compatibility bridge
    out["ts_gateway"] = d["window_end_ts"]
    out["reading_index"] = int(d["window_end_index"])

    # Defaults for optional aggregate fields
    out.setdefault("temp_mean", 0.0)
    out.setdefault("temp_std", 0.0)
    out.setdefault("vib_mean", 0.0)
    out.setdefault("vib_std", 0.0)
    out.setdefault("vib_rms", 0.0)

    return out


def insert_event(con, event_row: dict):
    con.execute(
        """
        INSERT INTO events(
          ts_event,
          ts_inference,
          ts_gateway,
          factory_id,
          machine_id,
          device_id,
          reading_index,
          severity,
          anomaly_reason,
          if_score,
          if_pred,
          is_anomaly_ml,
          temp_zscore,
          is_anomaly_ewma,
          is_anomaly_final,
          payload_json,
          window_start_ts,
          window_end_ts,
          window_start_index,
          window_end_index,
          window_size,
          window_step,
          temp_mean,
          temp_std,
          vib_mean,
          vib_std,
          vib_rms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_row["ts_event"],
            event_row["ts_inference"],
            event_row["ts_gateway"],
            event_row["factory_id"],
            event_row["machine_id"],
            event_row["device_id"],
            int(event_row["reading_index"]),
            event_row["severity"],
            event_row["anomaly_reason"],
            float(event_row["if_score"]),
            int(event_row["if_pred"]),
            int(event_row["is_anomaly_ml"]),
            float(event_row["temp_zscore"]),
            int(event_row["is_anomaly_ewma"]),
            int(event_row["is_anomaly_final"]),
            json.dumps(event_row, separators=(",", ":")),
            event_row["window_start_ts"],
            event_row["window_end_ts"],
            int(event_row["window_start_index"]),
            int(event_row["window_end_index"]),
            int(event_row["window_size"]),
            int(event_row["window_step"]),
            float(event_row["temp_mean"]),
            float(event_row["temp_std"]),
            float(event_row["vib_mean"]),
            float(event_row["vib_std"]),
            float(event_row["vib_rms"]),
        ),
    )
    con.commit()


def upsert_incident(con, d: dict, severity: str):
    incident_id = incident_id_for(d["device_id"])

    row = con.execute(
        """
        SELECT incident_id, status, severity_peak, occurrences,
               if_score_min, if_score_max, temp_zscore_max
        FROM incidents
        WHERE incident_id = ?
        """,
        (incident_id,),
    ).fetchone()

    if_score = float(d.get("if_score", 0.0))
    temp_zscore = abs(float(d.get("temp_zscore", 0.0)))
    nowi = now_iso()

    if row is None:
        payload_json = json.dumps(d, separators=(",", ":"))
        con.execute(
            """
            INSERT INTO incidents(
              incident_id,
              factory_id,
              machine_id,
              device_id,
              status,
              severity_current,
              severity_peak,
              anomaly_reason_first,
              anomaly_reason_last,
              opened_at,
              last_seen_at,
              resolved_at,
              occurrences,
              reading_index_first,
              reading_index_last,
              if_score_min,
              if_score_max,
              temp_zscore_max,
              payload_json,
              window_start_index_first,
              window_end_index_first,
              window_start_index_last,
              window_end_index_last,
              window_start_ts_first,
              window_end_ts_first,
              window_start_ts_last,
              window_end_ts_last
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                incident_id,
                d["factory_id"],
                d["machine_id"],
                d["device_id"],
                "OPEN",
                severity,
                severity,
                d.get("anomaly_reason", "unknown"),
                d.get("anomaly_reason", "unknown"),
                nowi,
                nowi,
                None,
                1,
                int(d["reading_index"]),
                int(d["reading_index"]),
                if_score,
                if_score,
                temp_zscore,
                payload_json,
                int(d["window_start_index"]),
                int(d["window_end_index"]),
                int(d["window_start_index"]),
                int(d["window_end_index"]),
                d["window_start_ts"],
                d["window_end_ts"],
                d["window_start_ts"],
                d["window_end_ts"],
            ),
        )
        con.commit()
        return incident_id, "OPENED"

    (
        _incident_id,
        status,
        severity_peak,
        occurrences,
        if_score_min,
        if_score_max,
        temp_zscore_max,
    ) = row

    severity_peak_new = severity_peak
    if severity_rank(severity) > severity_rank(severity_peak):
        severity_peak_new = severity

    action = "UPDATED"
    if status != "OPEN":
        action = "REOPENED"

    con.execute(
        """
        UPDATE incidents
        SET status = ?,
            severity_current = ?,
            severity_peak = ?,
            anomaly_reason_last = ?,
            last_seen_at = ?,
            resolved_at = ?,
            occurrences = ?,
            reading_index_last = ?,
            if_score_min = ?,
            if_score_max = ?,
            temp_zscore_max = ?,
            payload_json = ?,
            window_start_index_last = ?,
            window_end_index_last = ?,
            window_start_ts_last = ?,
            window_end_ts_last = ?
        WHERE incident_id = ?
        """,
        (
            "OPEN",
            severity,
            severity_peak_new,
            d.get("anomaly_reason", "unknown"),
            nowi,
            None,
            int(occurrences) + 1,
            int(d["reading_index"]),
            min(float(if_score_min), if_score),
            max(float(if_score_max), if_score),
            max(float(temp_zscore_max), temp_zscore),
            json.dumps(d, separators=(",", ":")),
            int(d["window_start_index"]),
            int(d["window_end_index"]),
            d["window_start_ts"],
            d["window_end_ts"],
            incident_id,
        ),
    )
    con.commit()
    return incident_id, action


def resolve_incident(con, device_id: str):
    incident_id = incident_id_for(device_id)
    cur = con.execute(
        """
        UPDATE incidents
        SET status = ?, resolved_at = ?, last_seen_at = ?
        WHERE incident_id = ? AND status = 'OPEN'
        """,
        ("RESOLVED", now_iso(), now_iso(), incident_id),
    )
    con.commit()
    return cur.rowcount


# ============================================================
# MQTT CALLBACKS
# ============================================================
def on_connect(client, userdata, flags, rc, properties=None):
    log(f"[MQTT] connected rc={rc}")
    client.subscribe(INPUT_TOPIC, qos=1)
    log(f"[MQTT] subscribed to {INPUT_TOPIC}")


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
        raw = json.loads(payload)
    except Exception as e:
        log(f"[WARN] invalid json: {e}")
        return

    required = [
        "ts_inference",
        "window_start_ts",
        "window_end_ts",
        "factory_id",
        "machine_id",
        "device_id",
        "window_start_index",
        "window_end_index",
        "window_size",
        "window_step",
        "if_score",
        "if_pred",
        "is_anomaly_ml",
        "temp_zscore",
        "is_anomaly_ewma",
        "is_anomaly_final",
        "anomaly_reason",
    ]
    for k in required:
        if k not in raw:
            log(f"[WARN] missing field: {k}")
            return

    d = normalize_window_payload(raw)

    dev = d["device_id"]
    st = get_state(dev)

    now_dt = datetime.now(timezone.utc)
    cooldown_until = st["cooldown_until"]
    in_cooldown = cooldown_until is not None and now_dt < cooldown_until

    is_final = int(d["is_anomaly_final"])
    severity = severity_from_result(d)

    if is_final == 1:
        st["anom_run"] += 1
        st["norm_run"] = 0
    else:
        st["norm_run"] += 1
        st["anom_run"] = 0

    if DEBUG_LOG_EVERY_MESSAGE:
        cooldown_str = cooldown_until.isoformat() if cooldown_until is not None else "None"
        log(
            f"[DEBUG] device={dev} "
            f"win={d['window_start_index']}-{d['window_end_index']} "
            f"idx={d['reading_index']} "
            f"is_final={is_final} "
            f"anom_run={st['anom_run']} norm_run={st['norm_run']} "
            f"in_cooldown={in_cooldown} cooldown_until={cooldown_str} "
            f"reason={d['anomaly_reason']} severity={severity}"
        )

    # Open/update incident only when threshold is first reached
    if is_final == 1 and st["anom_run"] == OPEN_N and not in_cooldown:
        event_row = {
            "ts_event": now_iso(),
            "ts_inference": d["ts_inference"],
            "ts_gateway": d["ts_gateway"],  # compatibility = window_end_ts
            "factory_id": d["factory_id"],
            "machine_id": d["machine_id"],
            "device_id": d["device_id"],
            "reading_index": int(d["reading_index"]),  # compatibility = window_end_index
            "severity": severity,
            "anomaly_reason": d["anomaly_reason"],
            "if_score": float(d["if_score"]),
            "if_pred": int(d["if_pred"]),
            "is_anomaly_ml": int(d["is_anomaly_ml"]),
            "temp_zscore": float(d["temp_zscore"]),
            "is_anomaly_ewma": int(d["is_anomaly_ewma"]),
            "is_anomaly_final": int(d["is_anomaly_final"]),
            "window_start_ts": d["window_start_ts"],
            "window_end_ts": d["window_end_ts"],
            "window_start_index": int(d["window_start_index"]),
            "window_end_index": int(d["window_end_index"]),
            "window_size": int(d["window_size"]),
            "window_step": int(d["window_step"]),
            "temp_mean": float(d.get("temp_mean", 0.0)),
            "temp_std": float(d.get("temp_std", 0.0)),
            "vib_mean": float(d.get("vib_mean", 0.0)),
            "vib_std": float(d.get("vib_std", 0.0)),
            "vib_rms": float(d.get("vib_rms", 0.0)),
        }

        try:
            retry_db_write(insert_event, db_con, event_row)
            incident_id, incident_action = retry_db_write(upsert_incident, db_con, d, severity)
        except Exception as e:
            log(
                f"[ERROR] db write failed for device={dev} "
                f"win={d['window_start_index']}-{d['window_end_index']} error={e}"
            )
            return

        downstream = {
            "ts_event": event_row["ts_event"],
            "incident_id": incident_id,
            "factory_id": d["factory_id"],
            "machine_id": d["machine_id"],
            "device_id": d["device_id"],
            "reading_index": int(d["reading_index"]),
            "window_start_index": int(d["window_start_index"]),
            "window_end_index": int(d["window_end_index"]),
            "window_start_ts": d["window_start_ts"],
            "window_end_ts": d["window_end_ts"],
            "severity": severity,
            "anomaly_reason": d["anomaly_reason"],
            "if_score": float(d["if_score"]),
            "temp_zscore": float(d["temp_zscore"]),
            "is_anomaly_final": 1,
            "event_action": incident_action,
        }

        client.publish(OUTPUT_TOPIC, json.dumps(downstream), qos=0, retain=False)

        log(
            f"[EVENT] {incident_action.lower()} incident "
            f"device={dev} "
            f"win={d['window_start_index']}-{d['window_end_index']} "
            f"severity={severity} reason={d['anomaly_reason']}"
        )

    elif is_final == 1 and st["anom_run"] > OPEN_N and not in_cooldown:
        log(
            f"[DEBUG] anomaly streak continuing "
            f"device={dev} win={d['window_start_index']}-{d['window_end_index']} "
            f"anom_run={st['anom_run']}"
        )

    elif is_final == 1 and in_cooldown:
        log(
            f"[DEBUG] anomaly ignored due to cooldown "
            f"device={dev} win={d['window_start_index']}-{d['window_end_index']}"
        )

    # Resolve after M consecutive normal windows
    if is_final == 0 and st["norm_run"] >= RESOLVE_M:
        try:
            updated = retry_db_write(resolve_incident, db_con, dev)
        except Exception as e:
            log(f"[ERROR] resolve failed for device={dev} error={e}")
            return

        if updated > 0:
            st["cooldown_until"] = datetime.now(timezone.utc) + timedelta(seconds=COOLDOWN_S)
            st["norm_run"] = 0
            st["anom_run"] = 0

            downstream = {
                "ts_event": now_iso(),
                "incident_id": incident_id_for(dev),
                "factory_id": d["factory_id"],
                "machine_id": d["machine_id"],
                "device_id": d["device_id"],
                "reading_index": int(d["reading_index"]),
                "window_start_index": int(d["window_start_index"]),
                "window_end_index": int(d["window_end_index"]),
                "window_start_ts": d["window_start_ts"],
                "window_end_ts": d["window_end_ts"],
                "severity": "info",
                "anomaly_reason": "normal",
                "if_score": float(d["if_score"]),
                "temp_zscore": float(d["temp_zscore"]),
                "is_anomaly_final": 0,
                "event_action": "RESOLVE",
            }

            client.publish(OUTPUT_TOPIC, json.dumps(downstream), qos=0, retain=False)
            log(f"[EVENT] resolved incident device={dev}")
        else:
            st["norm_run"] = 0
            st["anom_run"] = 0
            log(f"[DEBUG] resolve threshold reached but no OPEN incident existed for device={dev}")


def on_disconnect(client, userdata, rc, properties=None):
    log(f"[MQTT] disconnected rc={rc}")


# ============================================================
# MAIN
# ============================================================
def main():
    client = mqtt.Client(
        client_id="event-processing-service",
        protocol=mqtt.MQTTv311,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    def shutdown(signum, frame):
        log("[SYS] shutting down")
        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            db_con.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log("[BOOT] Event Processing service up")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()