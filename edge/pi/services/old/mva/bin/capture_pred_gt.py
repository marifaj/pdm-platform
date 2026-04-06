#!/usr/bin/env python3
import json, sqlite3, os, time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

DB = os.path.expanduser("~/mva/data/mva.db")
HOST, PORT = "127.0.0.1", 1883
GW = "gw-01"  # adjust if needed

con = sqlite3.connect(DB, check_same_thread=False)
con.execute("PRAGMA journal_mode=WAL;")
cur = con.cursor()

def iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def on_msg_raw(client, userdata, msg):
    try:
        d = json.loads(msg.payload.decode("utf-8"))
        msg_id = d.get("msg_id"); 
        if not msg_id: return
        bucket = (d.get("ts_device") or iso())[:10]
        cur.execute("""INSERT OR IGNORE INTO ground_truth
            (msg_id,device_id,gateway_id,trace_id,ts_utc,label,bucket_date)
            VALUES (?,?,?,?,?,?,?)""",
            (d.get("msg_id"), d.get("device_id"), d.get("gateway_id"),
             d.get("trace_id"), d.get("ts_device") or iso(),
             int(1 if d.get("label") in (1, True, "1", "true") else 0),
             bucket))
        con.commit()
    except Exception as e:
        print("raw err:", e, flush=True)

def on_msg_pred(client, userdata, msg):
    try:
        d = json.loads(msg.payload.decode("utf-8"))
        if not d.get("msg_id"): return
        bucket = (d.get("ts_utc") or iso())[:10]
        cur.execute("""INSERT OR REPLACE INTO pred_messages
            (msg_id,device_id,gateway_id,trace_id,ts_utc,anomaly_score,model_id,model_ver,bucket_date)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (d.get("msg_id"), d.get("device_id"), d.get("gateway_id"),
             d.get("trace_id"), d.get("ts_utc") or iso(),
             float(d.get("anomaly_score") or 0.0),
             d.get("model_id"), d.get("model_ver"), bucket))
        con.commit()
    except Exception as e:
        print("pred err:", e, flush=True)

def on_connect(client, u, f, rc, p=None):
    client.subscribe(f"mva/gateway/{GW}/ingress/+/raw", qos=1)
    client.subscribe(f"mva/gateway/{GW}/pred/+", qos=1)
    print("[OK] subscribed to raw & pred")

cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
cli.on_connect = on_connect
cli.message_callback_add(f"mva/gateway/{GW}/ingress/+/raw", on_msg_raw)
cli.message_callback_add(f"mva/gateway/{GW}/pred/+", on_msg_pred)
cli.connect(HOST, PORT, keepalive=60)
cli.loop_forever()
