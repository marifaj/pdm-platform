#!/usr/bin/env python3
"""
Notification Service — MVP (Extensible for Console / SMS / Email)

Current behavior:
- Subscribes to event_processing output topic
- Writes audit rows into notifications table
- Delivers to console immediately

Designed so you can later add:
- Twilio SMS
- SMTP email
without changing the core notification flow.

Core flow:
MQTT event -> build notification context -> choose channels -> deliver -> audit result
"""

import json
import os
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import paho.mqtt.client as mqtt

# ======== ENV AUTO-LOAD (.env) ========
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
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")

INPUT_TOPIC = os.getenv("NOTIFY_INPUT_TOPIC", "mva/event/telemetry")
DB_PATH = os.path.expanduser(os.getenv("NOTIFY_DB_PATH", "~/mva/data/mva.db"))
LOG_PATH = os.path.expanduser(os.getenv("NOTIFY_LOG_PATH", "~/mva/logs/notification.log"))

# Channel toggles
ENABLE_CONSOLE = os.getenv("NOTIFY_ENABLE_CONSOLE", "true").lower() == "true"
ENABLE_SMS = os.getenv("NOTIFY_ENABLE_SMS", "false").lower() == "true"
ENABLE_EMAIL = os.getenv("NOTIFY_ENABLE_EMAIL", "false").lower() == "true"

# SMS config placeholders (Twilio-ready later)
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "")
TWILIO_TO_NUMBER = os.getenv("TWILIO_TO_NUMBER", "")

# Email config placeholders (SMTP-ready later)
EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))
EMAIL_SMTP_USER = os.getenv("EMAIL_SMTP_USER", "")
EMAIL_SMTP_PASS = os.getenv("EMAIL_SMTP_PASS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


# ============================================================
# HELPERS
# ============================================================
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso_ms(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def diff_ms(start: Optional[str], end: Optional[str]) -> Optional[int]:
    start_ms = parse_iso_ms(start)
    end_ms = parse_iso_ms(end)
    if start_ms is None or end_ms is None:
        return None
    return end_ms - start_ms


def log(line: str) -> None:
    s = f"{iso_now()} notify {line}"
    print(s, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(s + "\n")
    except Exception:
        pass


# ============================================================
# DB
# ============================================================
def connect_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA busy_timeout=30000;")
    return con


def ensure_notifications_table(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          incident_id TEXT NOT NULL,
          channel TEXT NOT NULL,
          status TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 1,
          delivered_at TEXT NOT NULL,
          provider_msg_id TEXT,
          payload_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_notifications_incident
        ON notifications(incident_id, delivered_at)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_notifications_channel
        ON notifications(channel, delivered_at)
        """
    )
    con.commit()


def safe_add_column(con, table_name: str, column_sql: str) -> None:
    try:
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")
        con.commit()
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            return
        raise


def table_columns(con, table_name: str):
    return {row[1] for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()}


def ensure_latency_trace_table(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS latency_trace (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT,
          trace_id TEXT,
          deployment_mode TEXT,
          factory_id TEXT,
          machine_id TEXT,
          device_id TEXT,
          reading_index INTEGER,
          window_start_index INTEGER,
          window_end_index INTEGER,
          esp_millis INTEGER,
          ts_ingestion TEXT,
          ts_storage TEXT,
          ts_inference TEXT,
          ts_event TEXT,
          ts_notification TEXT,
          ingestion_to_inference_ms INTEGER,
          inference_to_event_ms INTEGER,
          event_to_notification_ms INTEGER,
          end_to_alert_ms INTEGER,
          ts_publish_client INTEGER,
          ts_ingestion_received INTEGER,
          ts_ingestion_published INTEGER,
          ts_storage_received INTEGER,
          ts_storage_inserted INTEGER,
          ts_inference_received INTEGER,
          ts_inference_start INTEGER,
          ts_inference_end INTEGER,
          ts_prediction_published INTEGER,
          ts_event_received INTEGER,
          ts_event_created INTEGER,
          ts_event_published INTEGER,
          ts_notification_received INTEGER,
          ts_notification_created INTEGER,
          created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    con.commit()
    for column_sql in [
        "deployment_mode TEXT",
        "window_start_index INTEGER",
        "window_end_index INTEGER",
        "ts_ingestion TEXT",
        "ts_storage TEXT",
        "ts_inference TEXT",
        "ts_event TEXT",
        "ts_notification TEXT",
        "ingestion_to_inference_ms INTEGER",
        "inference_to_event_ms INTEGER",
        "event_to_notification_ms INTEGER",
        "end_to_alert_ms INTEGER",
    ]:
        safe_add_column(con, "latency_trace", column_sql)
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_latency_trace_run_device_reading
        ON latency_trace(run_id, device_id, reading_index)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_latency_trace_trace_id
        ON latency_trace(trace_id)
        """
    )
    con.commit()


con = connect_db()
ensure_notifications_table(con)
ensure_latency_trace_table(con)
log(f"[BOOT] notifications table ensured at {DB_PATH}")


def insert_notification_with_retry(
    incident_id: str,
    channel: str,
    status: str,
    attempt_count: int,
    delivered_at: str,
    provider_msg_id: Optional[str],
    payload: dict,
    retries: int = 5,
    delay: float = 0.05,
) -> bool:
    payload_json = json.dumps(payload, separators=(",", ":"))

    for attempt in range(retries):
        try:
            con.execute(
                """
                INSERT INTO notifications(
                  incident_id,
                  channel,
                  status,
                  attempt_count,
                  delivered_at,
                  provider_msg_id,
                  payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    incident_id,
                    channel,
                    status,
                    attempt_count,
                    delivered_at,
                    provider_msg_id,
                    payload_json,
                ),
            )
            con.commit()
            return True
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
                continue
            log(f"[ERR] insert notification failed channel={channel}: {e}")
            return False
        except Exception as e:
            log(f"[ERR] insert notification failed channel={channel}: {e}")
            return False
    return False


# ============================================================
# NOTIFICATION MODEL
# ============================================================
@dataclass
class NotificationContext:
    ts_event: str
    ts_inference: Optional[str]
    ts_ingestion: Optional[str]
    ts_storage: Optional[str]
    ts_gateway: Optional[str]
    run_id: str
    deployment_mode: str
    trace_id: Optional[str]
    incident_id: str
    factory_id: str
    machine_id: str
    device_id: str
    reading_index: int
    window_start_index: Optional[int]
    window_end_index: Optional[int]
    severity: str
    anomaly_reason: str
    event_action: str
    if_score: Optional[float]
    temp_zscore: Optional[float]
    is_anomaly_final: int
    raw_event: dict

    @property
    def console_message(self) -> str:
        return (
            f"[ALERT] action={self.event_action} "
            f"severity={self.severity.upper()} "
            f"device={self.device_id} "
            f"machine={self.machine_id} "
            f"incident={self.incident_id} "
            f"reason={self.anomaly_reason} "
            f"idx={self.reading_index} "
            f"at {self.ts_event}"
        )

    @property
    def sms_message(self) -> str:
        return (
            f"{self.severity.upper()} {self.event_action} "
            f"{self.device_id}/{self.machine_id} "
            f"reason={self.anomaly_reason} "
            f"idx={self.reading_index} "
            f"time={self.ts_event}"
        )

    @property
    def email_subject(self) -> str:
        return (
            f"[MVA] {self.severity.upper()} {self.event_action} "
            f"{self.device_id}"
        )

    @property
    def email_body(self) -> str:
        return (
            f"Notification from MVA\n\n"
            f"Time: {self.ts_event}\n"
            f"Incident ID: {self.incident_id}\n"
            f"Factory ID: {self.factory_id}\n"
            f"Machine ID: {self.machine_id}\n"
            f"Device ID: {self.device_id}\n"
            f"Reading Index: {self.reading_index}\n"
            f"Severity: {self.severity}\n"
            f"Action: {self.event_action}\n"
            f"Reason: {self.anomaly_reason}\n"
            f"If Score: {self.if_score}\n"
            f"Temp Z-Score: {self.temp_zscore}\n"
            f"Is Anomaly Final: {self.is_anomaly_final}\n"
        )


def parse_event_to_context(d: dict) -> NotificationContext:
    return NotificationContext(
        ts_event=d.get("ts_event", iso_now()),
        ts_inference=d.get("ts_inference"),
        ts_ingestion=d.get("ts_ingestion"),
        ts_storage=d.get("ts_storage"),
        ts_gateway=d.get("ts_gateway"),
        run_id=d.get("run_id", "manual"),
        deployment_mode=d.get("deployment_mode", "unknown"),
        trace_id=d.get("trace_id"),
        incident_id=d.get("incident_id", f"INC-{d.get('device_id', 'unknown')}"),
        factory_id=d.get("factory_id", "unknown"),
        machine_id=d.get("machine_id", "unknown"),
        device_id=d.get("device_id", "unknown"),
        reading_index=int(d.get("reading_index", -1)),
        window_start_index=int(d["window_start_index"]) if d.get("window_start_index") is not None else None,
        window_end_index=int(d["window_end_index"]) if d.get("window_end_index") is not None else None,
        severity=d.get("severity", "info"),
        anomaly_reason=d.get("anomaly_reason", "unknown"),
        event_action=d.get("event_action", "UNKNOWN"),
        if_score=float(d["if_score"]) if "if_score" in d and d["if_score"] is not None else None,
        temp_zscore=float(d["temp_zscore"]) if "temp_zscore" in d and d["temp_zscore"] is not None else None,
        is_anomaly_final=int(d.get("is_anomaly_final", 0)),
        raw_event=d,
    )


# ============================================================
# DELIVERY CHANNELS
# ============================================================
def deliver_console(ctx: NotificationContext) -> Tuple[bool, Optional[str]]:
    log(ctx.console_message)
    return True, None


def deliver_sms(ctx: NotificationContext) -> Tuple[bool, Optional[str]]:
    """
    Placeholder for future Twilio integration.

    Later, this function can:
    - import the Twilio client
    - send ctx.sms_message
    - return (True, message_sid) on success
    """
    if not ENABLE_SMS:
        return False, None

    if not TWILIO_TO_NUMBER or not TWILIO_FROM_NUMBER:
        log("[WARN] SMS enabled but Twilio phone numbers are not configured")
        return False, None

    # Future Twilio implementation goes here.
    log(f"[TODO] SMS delivery not implemented yet: to={TWILIO_TO_NUMBER}")
    return False, None


def deliver_email(ctx: NotificationContext) -> Tuple[bool, Optional[str]]:
    """
    Placeholder for future SMTP/email integration.

    Later, this function can:
    - connect to SMTP
    - send ctx.email_subject / ctx.email_body
    - return (True, provider_msg_id) on success
    """
    if not ENABLE_EMAIL:
        return False, None

    if not EMAIL_TO or not EMAIL_FROM:
        log("[WARN] Email enabled but EMAIL_TO / EMAIL_FROM are not configured")
        return False, None

    # Future SMTP implementation goes here.
    log(f"[TODO] Email delivery not implemented yet: to={EMAIL_TO}")
    return False, None


def audit_delivery(
    ctx: NotificationContext,
    channel: str,
    delivered: bool,
    provider_msg_id: Optional[str],
) -> None:
    status = "DELIVERED" if delivered else "FAILED"
    ts_notification = utc_now_iso()
    ctx.raw_event["ts_notification"] = ts_notification
    inserted = insert_notification_with_retry(
        incident_id=ctx.incident_id,
        channel=channel,
        status=status,
        attempt_count=1,
        delivered_at=ts_notification,
        provider_msg_id=provider_msg_id,
        payload=ctx.raw_event,
    )
    if inserted:
        insert_latency_trace_with_retry(ctx, ts_notification)


def insert_latency_trace_with_retry(ctx: NotificationContext, ts_notification: str) -> None:
    # Main latency metric is service-chain latency from ts_ingestion to ts_notification.
    row = {
        "run_id": ctx.run_id,
        "trace_id": ctx.trace_id or f"{ctx.run_id}:{ctx.device_id}:{ctx.reading_index}",
        "deployment_mode": ctx.deployment_mode,
        "factory_id": ctx.factory_id,
        "machine_id": ctx.machine_id,
        "device_id": ctx.device_id,
        "reading_index": ctx.reading_index,
        "window_start_index": ctx.window_start_index,
        "window_end_index": ctx.window_end_index,
        "esp_millis": ctx.raw_event.get("espMillis"),
        "ts_ingestion": ctx.ts_ingestion,
        "ts_storage": ctx.ts_storage,
        "ts_inference": ctx.ts_inference,
        "ts_event": ctx.ts_event,
        "ts_notification": ts_notification,
        "ingestion_to_inference_ms": diff_ms(ctx.ts_ingestion, ctx.ts_inference),
        "inference_to_event_ms": diff_ms(ctx.ts_inference, ctx.ts_event),
        "event_to_notification_ms": diff_ms(ctx.ts_event, ts_notification),
        "end_to_alert_ms": diff_ms(ctx.ts_ingestion, ts_notification),
    }
    cols_supported = table_columns(con, "latency_trace")
    cols = [key for key in row.keys() if key in cols_supported]
    if not cols:
        return
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO latency_trace({', '.join(cols)}) VALUES ({placeholders})"
    values = tuple(row[col] for col in cols)
    for attempt in range(5):
        try:
            con.execute(sql, values)
            con.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < 4:
                time.sleep(0.05 * (attempt + 1))
                continue
            log(f"[ERR] insert latency_trace failed: {e}")
            return
        except Exception as e:
            log(f"[ERR] insert latency_trace failed: {e}")
            return


# ============================================================
# MQTT
# ============================================================
def on_connect(client, userdata, flags, rc, properties=None):
    log(f"[MQTT] connected rc={rc}; sub {INPUT_TOPIC}")
    client.subscribe(INPUT_TOPIC, qos=0)


def on_message(client, userdata, msg):
    try:
        d = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception as e:
        log(f"[WARN] bad json: {e}")
        return

    required = [
        "ts_event",
        "incident_id",
        "factory_id",
        "machine_id",
        "device_id",
        "reading_index",
        "severity",
        "anomaly_reason",
        "is_anomaly_final",
        "event_action",
    ]
    for k in required:
        if k not in d:
            log(f"[WARN] missing field: {k}")
            return

    ctx = parse_event_to_context(d)

    # Console delivery
    if ENABLE_CONSOLE:
        ok, provider_msg_id = deliver_console(ctx)
        audit_delivery(ctx, "console", ok, provider_msg_id)

    # SMS delivery (future-ready)
    if ENABLE_SMS:
        ok, provider_msg_id = deliver_sms(ctx)
        audit_delivery(ctx, "sms", ok, provider_msg_id)

    # Email delivery (future-ready)
    if ENABLE_EMAIL:
        ok, provider_msg_id = deliver_email(ctx)
        audit_delivery(ctx, "email", ok, provider_msg_id)


def on_disconnect(client, userdata, rc, properties=None):
    log(f"[MQTT] disconnected rc={rc}")


# ============================================================
# MAIN
# ============================================================
def main():
    client = mqtt.Client(
        client_id="notification-service",
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

    log("[BOOT] Notification service up")
    client.loop_forever()


if __name__ == "__main__":
    main()
