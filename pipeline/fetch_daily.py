"""
fetch_daily.py
──────────────
Runs the SQL queries against Trino for a given date, appends results to
data/daily_metrics.csv and data/velocity_history.csv.

Usage:
  python pipeline/fetch_daily.py                    # yesterday (D-1)
  python pipeline/fetch_daily.py --date 2026-04-09  # specific date
"""

import argparse
import csv
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Third-party
try:
    import trino
    import pandas as pd
except ImportError:
    sys.exit("Missing deps. Run: pip install trino pandas")

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
SQL_DIR    = ROOT / "sql"
DATA_DIR   = ROOT / "data"
CONFIG_DIR = ROOT / "config"

DATA_DIR.mkdir(exist_ok=True)

METRICS_CSV  = DATA_DIR / "daily_metrics.csv"
VELOCITY_CSV = DATA_DIR / "velocity_history.csv"

# ── Load settings ──────────────────────────────────────────────────────────────
with open(ROOT / "config" / "settings.json") as f:
    SETTINGS = json.load(f)

# ── Trino connection ───────────────────────────────────────────────────────────
def _load_env(path: Path) -> dict:
    env = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env

def get_connection():
    trino_env = _load_env(CONFIG_DIR / "trino.env")
    creds_env = _load_env(CONFIG_DIR / "credentials.env")

    host     = trino_env.get("TRINO_HOST",     os.environ.get("TRINO_HOST",     "10.60.34.154"))
    port     = int(trino_env.get("TRINO_PORT", os.environ.get("TRINO_PORT",     "8443")))
    user     = creds_env.get("TRINO_USER",     os.environ.get("TRINO_USER",     ""))
    password = creds_env.get("TRINO_PASSWORD", os.environ.get("TRINO_PASSWORD", ""))
    catalog  = trino_env.get("TRINO_CATALOG",  "hive")
    schema   = trino_env.get("TRINO_SCHEMA",   "kto_658")

    verify_ssl = trino_env.get("TRINO_VERIFY_SSL", "false").lower() != "false"

    return trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        auth=trino.auth.BasicAuthentication(user, password) if password else None,
        catalog=catalog,
        schema=schema,
        http_scheme="https",
        verify=verify_ssl,
    )

# ── SQL helpers ────────────────────────────────────────────────────────────────
def load_sql(filename: str, params: dict) -> str:
    sql = (SQL_DIR / filename).read_text(encoding="utf-8")
    for k, v in params.items():
        sql = sql.replace(f":{k}", f"'{v}'")
    return sql

def run_query(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

# ── Event flag ─────────────────────────────────────────────────────────────────
def get_event_flag(target_date: date) -> int:
    events_path = ROOT / "event_calendar" / "events.json"
    if not events_path.exists():
        return 0
    with open(events_path) as f:
        events = json.load(f)
    ds = target_date.isoformat()
    for ev in events:
        if ev.get("start_date", "") <= ds <= ev.get("end_date", ""):
            return 1
    return 0

# ── CSV append helpers ─────────────────────────────────────────────────────────
METRICS_COLS = [
    "ds", "cohort", "total_gold_spent", "active_spenders", "new_spenders",
    "returning_spenders", "avg_spend_per_role", "balance_velocity",
    "avg_balance_eod", "total_gold_received", "gold_from_recharge",
    "total_vnd_recharged", "event_flag",
]

VELOCITY_COLS = [
    "ds", "cohort", "total_gold_spent", "total_bod_balance",
    "total_gold_received", "total_available", "balance_velocity",
]

def append_csv(path: Path, df: pd.DataFrame, cols: list):
    write_header = not path.exists()
    df = df.reindex(columns=cols)
    df.to_csv(path, mode="a", header=write_header, index=False)

def remove_existing_date(path: Path, cols: list, ds: str):
    """Remove any rows for :ds so we can safely re-append without duplicates."""
    if not path.exists():
        return
    df = pd.read_csv(path, dtype=str)
    df = df[df["ds"] != ds]
    df.to_csv(path, index=False)

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYY-MM-DD, defaults to yesterday")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1)
    ds = target_date.isoformat()
    print(f"[fetch_daily] target_date = {ds}")

    event_flag = get_event_flag(target_date)
    print(f"[fetch_daily] event_flag  = {event_flag}")

    conn = get_connection()
    print("[fetch_daily] connected to Trino")

    # 1. Daily aggregation
    sql_agg = load_sql("daily_aggregation.sql", {"target_date": ds})
    df_metrics = run_query(conn, sql_agg)
    df_metrics["event_flag"] = event_flag
    print(f"[fetch_daily] aggregation: {len(df_metrics)} rows")

    # 2. Balance velocity (separate, more detailed)
    sql_vel = load_sql("balance_velocity.sql", {"target_date": ds})
    df_velocity = run_query(conn, sql_vel)
    print(f"[fetch_daily] velocity:    {len(df_velocity)} rows")

    # 3. Append to CSVs (idempotent — remove date first)
    remove_existing_date(METRICS_CSV,  METRICS_COLS,  ds)
    remove_existing_date(VELOCITY_CSV, VELOCITY_COLS, ds)

    append_csv(METRICS_CSV,  df_metrics,  METRICS_COLS)
    append_csv(VELOCITY_CSV, df_velocity, VELOCITY_COLS)

    print(f"[fetch_daily] wrote {METRICS_CSV}")
    print(f"[fetch_daily] wrote {VELOCITY_CSV}")
    print("[fetch_daily] done.")

if __name__ == "__main__":
    main()
