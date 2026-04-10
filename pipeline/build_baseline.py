"""
build_baseline.py
─────────────────
Trains ETS (Holt-Winters) models on historical spending data,
generates a 7-day forecast, and writes:
  data/forecast.json

Also updates the baseline/signal columns in data/daily_metrics.csv.

Usage:
  python pipeline/build_baseline.py
  python pipeline/build_baseline.py --cohort whale  # single cohort
"""

import argparse
import json
import sys
import warnings
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
except ImportError:
    sys.exit("Missing dep. Run: pip install statsmodels")

warnings.filterwarnings("ignore")

ROOT        = Path(__file__).resolve().parent.parent
DATA_DIR    = ROOT / "data"
CONFIG_DIR  = ROOT / "config"
EVENTS_JSON = ROOT / "event_calendar" / "events.json"

METRICS_CSV  = DATA_DIR / "daily_metrics.csv"
FORECAST_JSON = DATA_DIR / "forecast.json"

with open(CONFIG_DIR / "settings.json") as f:
    SETTINGS = json.load(f)

ETS_CFG    = SETTINGS["ets"]
ALERT_CFG  = SETTINGS["alerts"]
EV_CFG     = SETTINGS["event_calendar"]
COHORTS    = list(SETTINGS["cohorts"].keys())

# ── Event helpers ──────────────────────────────────────────────────────────────
def load_events() -> list[dict]:
    if EVENTS_JSON.exists():
        with open(EVENTS_JSON) as f:
            return json.load(f)
    return []

def build_event_set(events: list[dict], pad_before: int, pad_after: int) -> set[str]:
    """Return set of date strings that are within event windows (for exclusion)."""
    event_dates = set()
    for ev in events:
        start = date.fromisoformat(ev["start_date"])
        end   = date.fromisoformat(ev["end_date"])
        # Pad before and after
        padded_start = start - timedelta(days=pad_before)
        padded_end   = end   + timedelta(days=pad_after)
        d = padded_start
        while d <= padded_end:
            event_dates.add(d.isoformat())
            d += timedelta(days=1)
    return event_dates

def get_event_multiplier(series: pd.Series, event_dates: set[str]) -> float:
    """Estimate event multiplier from historical event days vs non-event avg."""
    if not event_dates:
        return 1.0
    baseline_vals = series[~series.index.isin(event_dates)]
    event_vals    = series[series.index.isin(event_dates)]
    if baseline_vals.empty or event_vals.empty:
        return 1.0
    baseline_avg = baseline_vals.mean()
    event_avg    = event_vals.mean()
    if baseline_avg == 0:
        return 1.0
    return max(1.0, event_avg / baseline_avg)

# ── ETS training ───────────────────────────────────────────────────────────────
def train_ets(series: pd.Series, exclude_dates: set[str]) -> ExponentialSmoothing | None:
    """Train ETS model, excluding event window dates from training."""
    clean = series[~series.index.isin(exclude_dates)].dropna()
    if len(clean) < ETS_CFG["min_training_days"]:
        print(f"  [warn] Only {len(clean)} training days (need {ETS_CFG['min_training_days']}). Skipping.")
        return None

    model = ExponentialSmoothing(
        clean.values,
        trend=ETS_CFG["trend"],
        seasonal=ETS_CFG["seasonal"],
        seasonal_periods=ETS_CFG["seasonal_period"],
        initialization_method="estimated",
    )
    fit = model.fit(smoothing_level=ETS_CFG["initial_alpha"], optimized=True)
    return fit

# ── Forecast ───────────────────────────────────────────────────────────────────
def build_forecast(fit, horizon: int, last_date: date, event_dates_future: set[str],
                   event_multiplier: float, band_pct: float) -> list[dict]:
    raw = fit.forecast(horizon)
    rows = []
    for i, val in enumerate(raw):
        fdate = last_date + timedelta(days=i + 1)
        ds = fdate.isoformat()
        is_event = ds in event_dates_future

        forecast = float(val) * (event_multiplier if is_event else 1.0)
        forecast = max(0, forecast)

        rows.append({
            "date":       ds,
            "forecast":   round(forecast),
            "lower":      round(forecast * (1 - band_pct)),
            "upper":      round(forecast * (1 + band_pct)),
            "confidence": f"±{round(band_pct * 100)}%",
            "is_event":   is_event,
        })
    return rows

# ── Signal detection ───────────────────────────────────────────────────────────
def compute_signal(actual: float, baseline: float, band_pct: float) -> str:
    if baseline == 0:
        return "ok"
    upper = baseline * (1 + band_pct)
    lower = baseline * (1 - band_pct)
    if actual > upper:
        return "over"
    if actual < lower:
        return "under"
    return "ok"

# ── MAPE ───────────────────────────────────────────────────────────────────────
def compute_mape(actual: pd.Series, predicted: pd.Series) -> float:
    mask = actual != 0
    if mask.sum() == 0:
        return float("nan")
    return float(np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])))

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohort", default=None, help="Run for single cohort only")
    args = parser.parse_args()

    if not METRICS_CSV.exists():
        sys.exit(f"[build_baseline] {METRICS_CSV} not found. Run fetch_daily.py first.")

    df = pd.read_csv(METRICS_CSV, parse_dates=["ds"], dtype={"ds": str})
    df["ds"] = pd.to_datetime(df["ds"]).dt.date.astype(str)

    events = load_events()
    exclude_dates = build_event_set(
        events,
        pad_before=EV_CFG["exclude_days_before_event"],
        pad_after=EV_CFG["exclude_days_after_event"],
    )

    cohorts_to_run = [args.cohort] if args.cohort else COHORTS
    forecast_output = {}
    band_pct = ETS_CFG["confidence_band_pct"]
    horizon  = ETS_CFG["forecast_horizon"]

    for cohort in cohorts_to_run:
        print(f"[build_baseline] cohort = {cohort}")
        cdf = df[df["cohort"] == cohort].sort_values("ds")

        if cdf.empty:
            print(f"  [warn] No data for cohort {cohort}")
            continue

        series = cdf.set_index("ds")["total_gold_spent"].astype(float)
        last_date = date.fromisoformat(cdf["ds"].max())

        # Future event dates for multiplier
        future_dates = {
            (last_date + timedelta(days=i+1)).isoformat()
            for i in range(horizon)
        }
        future_event_dates = future_dates & {
            ev_date for ev in events
            for ev_date in _date_range(ev["start_date"], ev["end_date"])
        }

        event_multiplier = get_event_multiplier(series, exclude_dates)
        print(f"  event_multiplier = {event_multiplier:.2f}")

        fit = train_ets(series, exclude_dates)
        if fit is None:
            continue

        # Back-fill baseline into metrics CSV
        in_sample = fit.fittedvalues
        if len(in_sample) == len(series[~series.index.isin(exclude_dates)]):
            clean_idx = [d for d in series.index if d not in exclude_dates]
            for i, idx_date in enumerate(clean_idx):
                bl = float(in_sample[i])
                df.loc[(df["ds"] == idx_date) & (df["cohort"] == cohort), "baseline_p50"] = round(bl)
                df.loc[(df["ds"] == idx_date) & (df["cohort"] == cohort), "baseline_upper"] = round(bl * (1 + band_pct))
                df.loc[(df["ds"] == idx_date) & (df["cohort"] == cohort), "baseline_lower"] = round(bl * (1 - band_pct))

        # Forecast
        rows = build_forecast(fit, horizon, last_date, future_event_dates, event_multiplier, band_pct)
        forecast_output[cohort] = rows

        mape = compute_mape(
            series[~series.index.isin(exclude_dates)],
            pd.Series(in_sample, index=[d for d in series.index if d not in exclude_dates]),
        )
        print(f"  MAPE = {mape:.1%}  |  training days = {len(series) - len([d for d in series.index if d in exclude_dates])}")
        if mape > ETS_CFG["recalibration_mape_threshold"]:
            print(f"  [warn] MAPE {mape:.1%} > {ETS_CFG['recalibration_mape_threshold']:.0%} — consider recalibration")

    # Write signal column to metrics CSV
    if "baseline_p50" not in df.columns:
        df["baseline_p50"] = None
    if "signal" not in df.columns:
        df["signal"] = None

    for cohort in cohorts_to_run:
        mask = df["cohort"] == cohort
        bl_col = df.loc[mask, "baseline_p50"].fillna(0)
        act_col = df.loc[mask, "total_gold_spent"]
        df.loc[mask, "signal"] = [
            compute_signal(float(a), float(b), band_pct)
            for a, b in zip(act_col, bl_col)
        ]

    df.to_csv(METRICS_CSV, index=False)
    print(f"[build_baseline] updated {METRICS_CSV}")

    # Write forecast JSON
    with open(FORECAST_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": date.today().isoformat(),
            "cohorts": forecast_output,
        }, f, indent=2)
    print(f"[build_baseline] wrote → {FORECAST_JSON}")
    print("[build_baseline] done.")


def _date_range(start_str: str, end_str: str) -> list[str]:
    d = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    result = []
    while d <= end:
        result.append(d.isoformat())
        d += timedelta(days=1)
    return result


if __name__ == "__main__":
    main()
