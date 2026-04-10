"""
backfill_history.py
────────────────────
Fetches historical KTO spending metrics for a date range in a single
Trino query per metric block, then writes daily_metrics.csv and
velocity_history.csv. Much faster than running fetch_daily.py per day.

Usage:
  python pipeline/backfill_history.py --start 2025-01-01 --end 2026-04-09
"""

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

try:
    import trino
    import pandas as pd
except ImportError:
    sys.exit("Missing deps. Run: pip install trino pandas")

ROOT       = Path(__file__).resolve().parent.parent
DATA_DIR   = ROOT / "data"
CONFIG_DIR = ROOT / "config"
EVENTS_JSON = ROOT / "event_calendar" / "events.json"

DATA_DIR.mkdir(exist_ok=True)

METRICS_CSV  = DATA_DIR / "daily_metrics.csv"
VELOCITY_CSV = DATA_DIR / "velocity_history.csv"

with open(CONFIG_DIR / "settings.json") as f:
    SETTINGS = json.load(f)

# ── Connection ─────────────────────────────────────────────────────────────────
def _load_env(path):
    env = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env

def get_connection():
    t = _load_env(CONFIG_DIR / "trino.env")
    c = _load_env(CONFIG_DIR / "credentials.env")
    host     = t.get("TRINO_HOST",     "trino.gio.vng.vn")
    port     = int(t.get("TRINO_PORT", "443"))
    user     = c.get("TRINO_USER",     "gs1_admin")
    password = c.get("TRINO_PASSWORD", "")
    catalog  = t.get("TRINO_CATALOG",  "hive")
    schema   = t.get("TRINO_SCHEMA",   "kto_658")
    verify   = t.get("TRINO_VERIFY_SSL", "false").lower() != "false"
    return trino.dbapi.connect(
        host=host, port=port, user=user,
        auth=trino.auth.BasicAuthentication(user, password) if password else None,
        catalog=catalog, schema=schema,
        http_scheme="https", verify=verify,
    )

def run_query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)

# ── Event flags ────────────────────────────────────────────────────────────────
def build_event_flags(start: date, end: date) -> dict:
    flags = {}
    if not EVENTS_JSON.exists():
        return flags
    with open(EVENTS_JSON) as f:
        events = json.load(f)
    d = start
    while d <= end:
        ds = d.isoformat()
        flag = 0
        for ev in events:
            if ev.get("start_date","") <= ds <= ev.get("end_date",""):
                flag = 1
                break
        flags[ds] = flag
        d += timedelta(days=1)
    return flags

# ── Main queries ───────────────────────────────────────────────────────────────
C = "CASE WHEN TRY_CAST(viplevel AS INTEGER) >= 12 THEN 'whale' WHEN TRY_CAST(viplevel AS INTEGER) >= 7 THEN 'dolphin' ELSE 'minnow' END"

def SPEND_SQL(s, e): return f"""
SELECT
  ds,
  {C} AS cohort,
  SUM(TRY_CAST(imoney AS BIGINT))   AS total_gold_spent,
  COUNT(DISTINCT roleid)             AS active_spenders,
  ROUND(
    CAST(SUM(TRY_CAST(imoney AS BIGINT)) AS DOUBLE) / NULLIF(COUNT(DISTINCT roleid), 0),
    2
  ) AS avg_spend_per_role
FROM hive.kto_658.moneychange_reduce
WHERE ds BETWEEN '{s}' AND '{e}'
  AND moneytype = 'Gold'
  AND big_type_logway NOT IN ('21','37','38','39','40','41','42','43')
GROUP BY 1, 2
ORDER BY ds, cohort
"""

def INFLOW_SQL(s, e): return f"""
SELECT
  ds,
  {C} AS cohort,
  SUM(TRY_CAST(imoney AS BIGINT)) AS total_gold_received,
  SUM(CASE WHEN logway_name = 'LogWay_Recharge' THEN TRY_CAST(imoney AS BIGINT) ELSE 0 END) AS gold_from_recharge
FROM hive.kto_658.moneychange_add
WHERE ds BETWEEN '{s}' AND '{e}'
  AND moneytype = 'Gold'
GROUP BY 1, 2
ORDER BY ds, cohort
"""

def RECHARGE_SQL(s, e): return f"""
SELECT
  ds,
  CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
  SUM(price / 100.0) AS total_vnd_recharged
FROM hive.kto_658.recharge_deliver
WHERE ds BETWEEN '{s}' AND '{e}'
GROUP BY 1, 2
ORDER BY ds, cohort
"""

def VELOCITY_SQL(s, e): return f"""
WITH spend_bod AS (
  SELECT
    ds,
    {C} AS cohort,
    roleid,
    MIN_BY(TRY_CAST(before AS BIGINT), time)  AS bod_balance,
    MAX_BY(TRY_CAST(after  AS BIGINT), time)  AS eod_balance,
    SUM(TRY_CAST(imoney AS BIGINT))           AS role_gold_spent
  FROM hive.kto_658.moneychange_reduce
  WHERE ds BETWEEN '{s}' AND '{e}'
    AND moneytype = 'Gold'
    AND big_type_logway NOT IN ('21','37','38','39','40','41','42','43')
  GROUP BY ds, 2, roleid
),
inflow AS (
  SELECT
    ds,
    {C} AS cohort,
    roleid,
    SUM(TRY_CAST(imoney AS BIGINT)) AS gold_received
  FROM hive.kto_658.moneychange_add
  WHERE ds BETWEEN '{s}' AND '{e}'
    AND moneytype = 'Gold'
  GROUP BY ds, 2, roleid
)
SELECT
  s.ds,
  s.cohort,
  SUM(s.role_gold_spent)           AS total_gold_spent,
  SUM(s.bod_balance)               AS total_bod_balance,
  SUM(COALESCE(i.gold_received,0)) AS total_gold_received,
  SUM(s.bod_balance) + SUM(COALESCE(i.gold_received,0)) AS total_available,
  ROUND(
    CAST(SUM(s.role_gold_spent) AS DOUBLE) /
    NULLIF(SUM(s.bod_balance) + SUM(COALESCE(i.gold_received,0)), 0),
    4
  ) AS balance_velocity,
  ROUND(AVG(CAST(s.eod_balance AS DOUBLE)), 2) AS avg_balance_eod
FROM spend_bod s
LEFT JOIN inflow i ON i.ds = s.ds AND i.roleid = s.roleid AND i.cohort = s.cohort
GROUP BY s.ds, s.cohort
ORDER BY s.ds, s.cohort
"""

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end",   default=(date.today() - timedelta(days=1)).isoformat())
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date   = date.fromisoformat(args.end)
    print(f"[backfill] range: {args.start} to {args.end}")

    # Parse event calendar first
    from parse_event_calendar import parse_xlsx, XLSX_PATH
    if XLSX_PATH.exists():
        events = parse_xlsx(XLSX_PATH)
        import json
        with open(ROOT / "event_calendar" / "events.json", "w", encoding="utf-8") as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        print(f"[backfill] parsed {len(events)} events from calendar")

    event_flags = build_event_flags(start_date, end_date)

    conn = get_connection()
    print("[backfill] connected to Trino")

    s, e = args.start, args.end

    print("[backfill] querying spend...")
    df_spend = run_query(conn, SPEND_SQL(s, e))
    print(f"           {len(df_spend)} rows")

    print("[backfill] querying inflow...")
    df_inflow = run_query(conn, INFLOW_SQL(s, e))
    print(f"           {len(df_inflow)} rows")

    print("[backfill] querying recharge VND...")
    df_recharge = run_query(conn, RECHARGE_SQL(s, e))
    print(f"           {len(df_recharge)} rows")

    print("[backfill] querying velocity (this may take a few minutes)...")
    df_velocity = run_query(conn, VELOCITY_SQL(s, e))
    print(f"           {len(df_velocity)} rows")

    # ── Merge into daily_metrics ────────────────────────────────────────────
    df = df_spend.copy()
    df = df.merge(df_inflow[["ds","cohort","total_gold_received","gold_from_recharge"]], on=["ds","cohort"], how="left")
    df = df.merge(df_recharge[["ds","cohort","total_vnd_recharged"]], on=["ds","cohort"], how="left")

    # Pull velocity & avg_balance_eod from velocity query
    vel_cols = df_velocity[["ds","cohort","balance_velocity","avg_balance_eod"]].copy()
    df = df.merge(vel_cols, on=["ds","cohort"], how="left")

    # Add event flag
    df["event_flag"] = df["ds"].map(event_flags).fillna(0).astype(int)

    # Placeholder columns (computed later by build_baseline.py)
    df["new_spenders"]       = None
    df["returning_spenders"] = None
    df["baseline_p50"]       = None
    df["baseline_upper"]     = None
    df["baseline_lower"]     = None
    df["signal"]             = None

    METRICS_COLS = [
        "ds","cohort","total_gold_spent","active_spenders","new_spenders",
        "returning_spenders","avg_spend_per_role","balance_velocity",
        "avg_balance_eod","total_gold_received","gold_from_recharge",
        "total_vnd_recharged","event_flag","baseline_p50","baseline_upper",
        "baseline_lower","signal",
    ]
    df = df.reindex(columns=METRICS_COLS)
    df.to_csv(METRICS_CSV, index=False)
    print(f"[backfill] wrote {len(df)} rows to {METRICS_CSV}")

    # ── Write velocity_history ──────────────────────────────────────────────
    VEL_COLS = ["ds","cohort","total_gold_spent","total_bod_balance","total_gold_received","total_available","balance_velocity"]
    df_velocity.reindex(columns=VEL_COLS).to_csv(VELOCITY_CSV, index=False)
    print(f"[backfill] wrote {len(df_velocity)} rows to {VELOCITY_CSV}")

    print("[backfill] done. Next: run build_baseline.py")

if __name__ == "__main__":
    main()
