#!/usr/bin/env python3
import os, json, time, sqlite3, glob, shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ---- .env auto-load (works even if run directly) ----
try:
    from dotenv import load_dotenv  # type: ignore
    env_path = Path.home() / "mva" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except Exception:
    pass

DB_PATH   = os.path.expanduser("~/mva/data/mva.db")
SCHEMA    = os.path.expanduser("~/mva/storage/schema.sql")
MIG_DIR   = os.path.expanduser("~/mva/storage/migrations")  # optional; apply if files exist
CFG_PATH  = os.path.expanduser("~/mva/storage/config/retention.json")  # Appendix H aligned
LOG_PATH  = os.path.expanduser("~/mva/logs/data_storage.log")

# Ensure parent dirs exist
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
os.makedirs(os.path.dirname(CFG_PATH), exist_ok=True)
os.makedirs(MIG_DIR, exist_ok=True)  # harmless if unused

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def log(msg):
    line = f"{now_iso()} storage {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line+"\n")
    except Exception:
        pass

def load_cfg():
    # defaults
    cfg = {
        "retention": {
            "cap_percent_free": 70,  # keep >= this % free disk space
            "raw_min_hours": 24,     # keep at least this much raw window
            "events_min_days": 30,   # keep events this long (unless emergency trim)
            "pred_keep": False       # reserved for prediction table keep policy (future)
        },
        "trim_interval_s": 600,      # check every 10 min
        "vacuum_interval_s": 86400   # vacuum daily
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
            log(f"[WARN] failed to load cfg {CFG_PATH}: {e}")
    return cfg

def connect_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def apply_schema(con):
    if not os.path.exists(SCHEMA):
        log(f"[BOOT] schema file missing at {SCHEMA}; skipping (service will still run).")
        return
    with open(SCHEMA, "r") as f:
        con.executescript(f.read())
    con.commit()
    log("[BOOT] schema ensured")

def applied_migrations(con):
    con.execute(
        "CREATE TABLE IF NOT EXISTS storage_migrations("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT UNIQUE, applied_at TEXT)"
    )
    rows = con.execute("SELECT filename FROM storage_migrations").fetchall()
    return set(r[0] for r in rows)

def apply_migrations(con):
    if not os.path.isdir(MIG_DIR):
        return
    done = applied_migrations(con)
    for path in sorted(glob.glob(os.path.join(MIG_DIR, "*.sql"))):
        fname = os.path.basename(path)
        if fname in done:
            continue
        try:
            with open(path, "r") as f:
                sql = f.read()
            con.executescript(sql)
            con.execute(
                "INSERT INTO storage_migrations(filename, applied_at) VALUES(?,?)",
                (fname, now_iso()),
            )
            con.commit()
            log(f"[MIG] applied {fname}")
        except Exception as e:
            log(f"[MIG][ERR] {fname}: {e}")

def fs_free_percent(path):
    st = shutil.disk_usage(os.path.dirname(path))
    free_pct = 100.0 * st.free / st.total
    return free_pct, st

def days_ago_iso(days):
    return (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()

def hours_ago_iso(hours):
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds").replace("+00:00","Z")

def trim(con, cfg):
    # minimum windows
    raw_cutoff    = hours_ago_iso(cfg["retention"]["raw_min_hours"])
    events_cutoff = days_ago_iso(cfg["retention"]["events_min_days"])

    free_pct, _ = fs_free_percent(DB_PATH)
    if free_pct >= cfg["retention"]["cap_percent_free"]:
        log(f"[RET] free={free_pct:.1f}% (>= cap), nothing to trim")
        return

    log(f"[RET] free={free_pct:.1f}% (< cap). Trimming raw first…")
    cur = con.cursor()
    total_deleted = 0
    # delete raw older than cutoff in batches
    while True:
        try:
            cur.execute("DELETE FROM raw_messages WHERE ts_utc < ? LIMIT 1000", (raw_cutoff,))
            changed = cur.rowcount or 0
            con.commit()
            total_deleted += changed
        except Exception as e:
            log(f"[RET][ERR] raw delete: {e}")
            break
        free_pct, _ = fs_free_percent(DB_PATH)
        if changed == 0 or free_pct >= cfg["retention"]["cap_percent_free"]:
            break
    log(f"[RET] raw deleted rows={total_deleted}; free now ~{free_pct:.1f}%")

    if free_pct < cfg["retention"]["cap_percent_free"]:
        log("[RET] emergency-trim events…")
        try:
            cur.execute("DELETE FROM events WHERE bucket_date < ? LIMIT 5000", (events_cutoff,))
            con.commit()
        except Exception as e:
            log(f"[RET][ERR] events delete: {e}")
        free_pct, _ = fs_free_percent(DB_PATH)
        log(f"[RET] events trim done; free now ~{free_pct:.1f}%")

def vacuum_if_due(con, last_vacuum_ts, interval_s):
    now = time.time()
    if now - last_vacuum_ts >= interval_s:
        try:
            log("[VACUUM] starting…")
            con.execute("VACUUM;")
            con.commit()
            log("[VACUUM] done")
        except Exception as e:
            log(f"[VACUUM][ERR] {e}")
        return now
    return last_vacuum_ts

def main():
    cfg = load_cfg()
    con = connect_db()
    apply_schema(con)
    apply_migrations(con)

    last_vac = 0.0
    log("[BOOT] Data Storage service up")
    while True:
        try:
            trim(con, cfg)
            last_vac = vacuum_if_due(con, last_vac, cfg.get("vacuum_interval_s", 86400))
        except Exception as e:
            log(f"[ERR] retention/vacuum: {e}")
        time.sleep(cfg.get("trim_interval_s", 600))

if __name__ == "__main__":
    main()
