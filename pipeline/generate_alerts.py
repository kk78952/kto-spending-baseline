"""
generate_alerts.py
──────────────────
Reads data/daily_metrics.csv and data/velocity_history.csv,
applies pattern-based alert rules, writes data/alerts.json.

Alert rules from spec Section 7:
  1. Spend↓ + velocity stable + spenders↓          → Churn
  2. Spend↓ + velocity↓ + spenders stable           → Hoarding
  3. Spend stable + recharge↓ + balance↓            → Reserve burn
  4. Spend↑ + velocity↑ + new_spenders↑             → UA working
  5. Spend > P75 + no event flagged                 → Unknown spike
  6. Spend < P25 + no event flagged                 → Content gap
  7. Whale velocity < threshold for N+ days         → Whale hoarding

Usage:
  python pipeline/generate_alerts.py
"""

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT        = Path(__file__).resolve().parent.parent
DATA_DIR    = ROOT / "data"
CONFIG_DIR  = ROOT / "config"

METRICS_CSV   = DATA_DIR / "daily_metrics.csv"
VELOCITY_CSV  = DATA_DIR / "velocity_history.csv"
ALERTS_JSON   = DATA_DIR / "alerts.json"

with open(CONFIG_DIR / "settings.json") as f:
    SETTINGS = json.load(f)

ALERT_CFG  = SETTINGS["alerts"]
COHORT_CFG = SETTINGS["cohorts"]

VEL_WHALE_THRESH  = ALERT_CFG["velocity_whale_threshold"]
CONSEC_DAYS       = ALERT_CFG["consecutive_days_trigger"]
WARN_PCT          = ALERT_CFG["deviation_warning_pct"] / 100
CRIT_PCT          = ALERT_CFG["deviation_critical_pct"] / 100

# ── Helpers ────────────────────────────────────────────────────────────────────
def pct_change(a, b) -> float | None:
    if b is None or b == 0:
        return None
    return (a - b) / b

def is_up(val, threshold=0.05) -> bool:
    return val is not None and val > threshold

def is_down(val, threshold=-0.05) -> bool:
    return val is not None and val < threshold

def is_stable(val, band=0.05) -> bool:
    return val is not None and abs(val) <= band

def last_n(df, cohort, col, n, today) -> list:
    rows = df[(df["cohort"] == cohort) & (df["ds"] <= today)].sort_values("ds").tail(n)
    return rows[col].tolist()

# ── Alert builders ─────────────────────────────────────────────────────────────
def check_cohort_patterns(df: pd.DataFrame, today: str) -> list[dict]:
    alerts = []
    window = 7

    for cohort in ["whale", "dolphin", "minnow"]:
        cdf = df[df["cohort"] == cohort].sort_values("ds")
        if len(cdf) < 2:
            continue

        recent = cdf[cdf["ds"] <= today].tail(window)
        if recent.empty:
            continue

        latest = recent.iloc[-1]
        prev   = recent.iloc[-2] if len(recent) >= 2 else None

        spend_chg   = pct_change(latest["total_gold_spent"], prev["total_gold_spent"])   if prev is not None else None
        vel_chg     = pct_change(latest["balance_velocity"], prev["balance_velocity"])   if prev is not None else None
        spender_chg = pct_change(latest["active_spenders"],  prev["active_spenders"])    if prev is not None else None
        new_sp_chg  = pct_change(latest["new_spenders"],     prev["new_spenders"])       if prev is not None else None
        recharge_chg= pct_change(latest["gold_from_recharge"],prev["gold_from_recharge"]) if prev is not None else None

        cohort_color = COHORT_CFG.get(cohort, {}).get("color", "#ccc")
        cohort_label = COHORT_CFG.get(cohort, {}).get("label", cohort)

        # Rule 1: Spend↓ + velocity stable + spenders↓ → Churn
        if is_down(spend_chg) and is_stable(vel_chg) and is_down(spender_chg):
            alerts.append({
                "type": "warning",
                "cohort": cohort,
                "title": "Churn signal",
                "message": (
                    f"{cohort_label}: Spend down {abs(spend_chg):.0%}, spenders down {abs(spender_chg):.0%}, "
                    f"velocity stable ({latest['balance_velocity']:.2f}). "
                    "Players leaving — not a content problem."
                ),
            })

        # Rule 2: Spend↓ + velocity↓ + spenders stable → Hoarding
        elif is_down(spend_chg) and is_down(vel_chg) and is_stable(spender_chg):
            alerts.append({
                "type": "warning",
                "cohort": cohort,
                "title": "Hoarding signal",
                "message": (
                    f"{cohort_label}: Spend down {abs(spend_chg):.0%}, velocity declining ({latest['balance_velocity']:.2f}), "
                    "spenders stable. Players online but saving Gold — event incoming?"
                ),
            })

        # Rule 3: Spend stable + recharge↓ + balance↓ → Reserve burn
        elif is_stable(spend_chg) and is_down(recharge_chg):
            alerts.append({
                "type": "warning",
                "cohort": cohort,
                "title": "Reserve burn",
                "message": (
                    f"{cohort_label}: Spend stable but recharge down {abs(recharge_chg):.0%}. "
                    "Spending fueled by old Gold — revenue risk ahead."
                ),
            })

        # Rule 4: Spend↑ + velocity↑ + new_spenders↑ → UA working
        elif is_up(spend_chg) and is_up(vel_chg) and is_up(new_sp_chg):
            alerts.append({
                "type": "success",
                "cohort": cohort,
                "title": "UA working",
                "message": (
                    f"{cohort_label}: Spend up {spend_chg:.0%}, velocity up, "
                    f"new spenders up {new_sp_chg:.0%}. UA campaign bringing actual spenders."
                ),
            })

        # Rule 5 & 6: vs baseline band
        bl  = latest.get("baseline_p50")
        bup = latest.get("baseline_upper")
        blo = latest.get("baseline_lower")
        ev  = latest.get("event_flag", 0)
        act = latest["total_gold_spent"]

        if pd.notna(bl) and bl > 0:
            if pd.notna(bup) and act > bup and not ev:
                alerts.append({
                    "type": "warning",
                    "cohort": cohort,
                    "title": "Unknown spike",
                    "message": (
                        f"{cohort_label}: Spend {act:,.0f} > P75 upper band {bup:,.0f} "
                        "with no event flagged. Investigate: unflagged event? Whale return?"
                    ),
                })
            elif pd.notna(blo) and act < blo and not ev:
                alerts.append({
                    "type": "info",
                    "cohort": cohort,
                    "title": "Content gap",
                    "message": (
                        f"{cohort_label}: Spend {act:,.0f} < P25 lower band {blo:,.0f} "
                        "with no event flagged. Consider pushing event earlier."
                    ),
                })

    return alerts


def check_whale_velocity(df: pd.DataFrame, today: str) -> list[dict]:
    """Rule 7: Whale velocity < threshold for consecutive days."""
    whale = df[(df["cohort"] == "whale") & (df["ds"] <= today)].sort_values("ds")
    if len(whale) < CONSEC_DAYS:
        return []

    recent = whale.tail(CONSEC_DAYS)
    vels = recent["balance_velocity"].tolist()

    if all(v is not None and v < VEL_WHALE_THRESH for v in vels):
        avg_vel = sum(vels) / len(vels)
        return [{
            "type": "warning",
            "cohort": "whale",
            "title": "Whale hoarding",
            "message": (
                f"Whale velocity below {VEL_WHALE_THRESH:.2f} for {CONSEC_DAYS}+ consecutive days "
                f"(avg {avg_vel:.2f}). Likely spike when next event drops."
            ),
        }]
    return []


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    if not METRICS_CSV.exists():
        sys.exit(f"[generate_alerts] {METRICS_CSV} not found. Run fetch_daily.py first.")

    df = pd.read_csv(METRICS_CSV)
    today = df["ds"].max()
    print(f"[generate_alerts] latest date in data = {today}")

    alerts = []
    alerts += check_cohort_patterns(df, today)
    alerts += check_whale_velocity(df, today)

    # Sort: warnings first, then info, then success
    order = {"warning": 0, "info": 1, "success": 2}
    alerts.sort(key=lambda a: order.get(a["type"], 3))

    output = {
        "generated_at": str(date.today()),
        "as_of_date": today,
        "alerts": alerts,
    }

    with open(ALERTS_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[generate_alerts] {len(alerts)} alerts written to {ALERTS_JSON}")
    for a in alerts:
        icon = {"warning": "!", "info": "i", "success": "ok"}.get(a["type"], "-")
        print(f"  {icon} [{a.get('cohort','all')}] {a['title']}")


if __name__ == "__main__":
    main()
