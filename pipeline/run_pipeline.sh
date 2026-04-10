#!/bin/bash
# =============================================================================
# run_pipeline.sh
# Daily pipeline: parse events → fetch data → build baseline → generate alerts
#
# Schedule: Run daily at ~8:00 AM (after D-1 data arrives in Trino)
# Usage:
#   bash pipeline/run_pipeline.sh               # yesterday (D-1)
#   bash pipeline/run_pipeline.sh 2026-04-09    # specific date
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
cd "$ROOT"

DATE_ARG="${1:-}"
DATE_FLAG=""
if [ -n "$DATE_ARG" ]; then
    DATE_FLAG="--date $DATE_ARG"
fi

echo "============================================================"
echo "KTO Spending Baseline Pipeline"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Target: ${DATE_ARG:-yesterday (D-1)}"
echo "============================================================"

# Activate virtualenv if present
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
elif [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# ── Step 1: Parse event calendar ──────────────────────────────────────────────
echo ""
echo "[1/4] Parsing event calendar..."
python pipeline/parse_event_calendar.py
echo "      Done."

# ── Step 2: Fetch daily metrics from Trino ────────────────────────────────────
echo ""
echo "[2/4] Fetching daily metrics from Trino..."
python pipeline/fetch_daily.py $DATE_FLAG
echo "      Done."

# ── Step 3: Build ETS baseline + forecast ────────────────────────────────────
echo ""
echo "[3/4] Building ETS baseline and forecast..."
python pipeline/build_baseline.py
echo "      Done."

# ── Step 4: Generate alerts ───────────────────────────────────────────────────
echo ""
echo "[4/4] Generating pattern-based alerts..."
python pipeline/generate_alerts.py
echo "      Done."

echo ""
echo "============================================================"
echo "Pipeline complete. Data written to data/"
echo "  daily_metrics.csv   — updated"
echo "  velocity_history.csv — updated"
echo "  forecast.json        — updated"
echo "  alerts.json          — updated"
echo "============================================================"

# ── Optional: Deploy to Netlify ───────────────────────────────────────────────
# Uncomment if you have Netlify CLI installed and NETLIFY_AUTH_TOKEN set:
#
# echo ""
# echo "[deploy] Pushing to Netlify..."
# netlify deploy --dir . --prod --auth "$NETLIFY_AUTH_TOKEN" --site "$NETLIFY_SITE_ID"
# echo "[deploy] Done."
