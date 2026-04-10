"""
parse_event_calendar.py
───────────────────────
Parses KTO_Nội_Dung_Vận_Hành.xlsx (Gantt-style layout) into events.json.

Layout per sheet (year):
  Row 0 (index): dates as datetime objects, starting at column index 2 (C)
  Row 1 (index): day-of-week numbers
  Rows 2+:       event tracks
    col 0 = "Plan vận hành"
    col 1 = track number
    col 2+ = event name at start cell, None for continuation cells

Usage:
  python pipeline/parse_event_calendar.py
  python pipeline/parse_event_calendar.py --xlsx event_calendar/KTO_Nội_Dung_Vận_Hành.xlsx
"""

import argparse
import json
import re
import sys
from datetime import datetime, date
from pathlib import Path

try:
    import openpyxl
except ImportError:
    sys.exit("Missing dep. Run: pip install openpyxl")

ROOT       = Path(__file__).resolve().parent.parent
XLSX_PATH  = ROOT / "event_calendar" / "KTO_Nội_Dung_Vận_Hành.xlsx"
EVENTS_OUT = ROOT / "event_calendar" / "events.json"

DATE_COL_START = 2  # Column C (0-indexed) is where dates begin

def _to_date_str(val) -> str | None:
    if isinstance(val, (datetime, date)):
        return val.strftime("%Y-%m-%d") if isinstance(val, datetime) else val.isoformat()
    return None

def parse_sheet(ws, year: int) -> list[dict]:
    """Parse one year sheet into a list of event dicts."""
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 3:
        return []

    # Row 0: date headers
    date_row = rows[0]
    # Build col_index → date_str map for valid date columns
    col_dates: dict[int, str] = {}
    for ci, val in enumerate(date_row):
        if ci < DATE_COL_START:
            continue
        ds = _to_date_str(val)
        if ds:
            col_dates[ci] = ds

    if not col_dates:
        return []

    date_cols_sorted = sorted(col_dates.keys())

    events = []
    for row in rows[2:]:  # Skip date row and dow row
        track_label = row[0]  # "Plan vận hành" or similar
        track_num   = row[1]

        # Scan for event cells
        ci = DATE_COL_START
        while ci < len(row):
            cell_val = row[ci]
            if cell_val is not None and ci in col_dates:
                event_name = str(cell_val).strip()
                if not event_name or event_name.lower() in ("plan vận hành", "nan"):
                    ci += 1
                    continue

                start_date = col_dates[ci]

                # Find end date: scan rightward until next non-None cell or end of dates
                end_ci = ci + 1
                while end_ci < len(row) and end_ci in col_dates:
                    if row[end_ci] is not None:
                        break
                    end_ci += 1

                # end_date is the last valid date column before the next event
                end_ci_actual = end_ci - 1
                # Make sure end_ci_actual is a valid date col
                while end_ci_actual > ci and end_ci_actual not in col_dates:
                    end_ci_actual -= 1

                end_date = col_dates.get(end_ci_actual, start_date)

                events.append({
                    "start_date": start_date,
                    "end_date":   end_date,
                    "event_name": event_name,
                    "track":      int(track_num) if track_num is not None and str(track_num).isdigit() else None,
                    "year":       year,
                })
                ci = end_ci  # Jump past this event
            else:
                ci += 1

    return events


def parse_xlsx(xlsx_path: Path) -> list[dict]:
    wb = openpyxl.load_workbook(str(xlsx_path), data_only=True)
    all_events = []

    for sheet_name in wb.sheetnames:
        # Skip non-year sheets (e.g. "REV", summary sheets)
        if not re.match(r"^\d{4}$", sheet_name.strip()):
            continue
        year = int(sheet_name.strip())
        ws = wb[sheet_name]
        events = parse_sheet(ws, year)
        all_events.extend(events)
        print(f"[parse_event_calendar] sheet {sheet_name}: {len(events)} events")

    # Deduplicate and sort
    seen = set()
    unique = []
    for ev in sorted(all_events, key=lambda e: (e["start_date"], e.get("track") or 0)):
        key = (ev["start_date"], ev["event_name"])
        if key not in seen:
            seen.add(key)
            unique.append(ev)

    return unique


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--xlsx", default=str(XLSX_PATH))
    args = parser.parse_args()

    xlsx_path = Path(args.xlsx)
    if not xlsx_path.exists():
        sys.exit(f"[parse_event_calendar] xlsx not found: {xlsx_path}")

    print(f"[parse_event_calendar] parsing {xlsx_path}")
    events = parse_xlsx(xlsx_path)
    print(f"[parse_event_calendar] total events: {len(events)}")

    with open(EVENTS_OUT, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

    print(f"[parse_event_calendar] wrote {EVENTS_OUT}")


if __name__ == "__main__":
    main()
