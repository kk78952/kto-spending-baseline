# KTO Daily Spending Baseline — Project Specification

**Version:** 1.0
**Date:** 9 Apr 2026
**Author:** KhanhNV5 (via Claude conversation)
**Game:** KTO (Kim Thánh Online / Kiếm Thế Origin) — VNG mobile MMORPG

---

## 1. Project goal

Build a daily dashboard that predicts how much Gold (ingame currency) KTO players spend per day, then compares actual spending against that prediction to detect anomalies, evaluate event performance, and forecast the next 7 days.

The dashboard runs on D-1 data (yesterday's data arrives this morning), generates a baseline using Exponential Triple Smoothing (ETS / Holt-Winters), and displays 7 sections of analysis.

**This is a static site dashboard project** — hosted similarly to existing Netlify dashboards. Not a chatbot feature.

---

## 2. Data sources

All data lives in **Trino/Iceberg** under catalog `hive`, schema `kto_658`.

### 2.1 Core tables

| Table | Purpose | Key filter |
|---|---|---|
| `hive.kto_658.moneychange_reduce` | Gold spent (every outflow transaction) | `moneytype = 'Gold'` |
| `hive.kto_658.moneychange_add` | Gold received (every inflow transaction) | `moneytype = 'Gold'` |
| `hive.kto_658.recharge_deliver` | Real money top-up (VND → Gold conversion) | — |

### 2.2 Shared column structure (moneychange_reduce & moneychange_add)

Both tables have identical columns:

```
log_name        — "moneychange.reduce" or "moneychange.add"
time            — Transaction timestamp (UTC, ISO 8601)
serverid        — Game server ID
channelid       — Platform: "vngktovn", "ios_vngktovn", "pc_vngktovn"
accountid       — Account identifier
roleid          — Player character ID (primary key for player)
rolename        — Character name
rolelevel       — Current level
viplevel        — VIP tier (integer, 0-18) — used as cohort proxy
fightpower      — Combat power score
faction         — Faction ID
sect            — Sect (sub-faction)
moneytype       — Currency type: "Gold", "Silver", etc.
before          — Balance BEFORE this transaction
imoney          — Amount changed in this transaction
after           — Balance AFTER this transaction
big_type_logway — Logway category number
small_type_logway — Logway sub-category
itemid          — Item involved (0 if none)
itemquantity    — Item quantity
logway_name     — Human-readable logway (e.g. "LogWay_ShopBuy")
ds              — Partition date (YYYY-MM-DD) — this is the query date
```

### 2.3 recharge_deliver columns

```
(same identity fields as above, plus:)
ordernumber     — Unique transaction GUID
cashtype        — "VND"
product_id      — Package ID (e.g. "com.ktovn.w.kt.2000")
price           — VND amount × 100 (MUST DIVIDE BY 100 to get real VND)
channel         — Payment channel: "web", "ios", "android"
firstpay        — "0" or "1" — first-time payer flag
moneytype       — "Gold"
imoney          — Gold received from this purchase
delivertype     — Delivery type
ds              — Partition date
```

### 2.4 Critical data notes

- **`price / 100` = actual VND.** The `price` field stores VND × 100. Always divide. Example: `price = 5.0E7` → actual value is 500,000 VND.
- **Exchange rate from sample:** `com.ktovn.w.kt.2000` = 500K VND → 2000 Gold = 250 VND per Gold.
- **Data availability:** D-1. Today's dashboard shows yesterday's data. Tables are partitioned by `ds`.
- **Gold is the only premium currency** that enters via real money. Other currencies (Silver, etc.) are downstream. Filter `moneytype = 'Gold'` on both moneychange tables.
- **Gold only enters the economy via recharge or player trade.** No free Gold from quests/gameplay. Player trade moves Gold between roles but doesn't create new Gold.

---

## 3. Logway classification

The `logway_name` field categorizes every Gold transaction. The logway reference data (provided as `kto_ingame_data_descriptions_no_item.json`) includes a "Nhóm lớn cách sinh ra tiền tệ" (major group) field.

### 3.1 Spending filter for baseline

When calculating "real spending" (Gold permanently leaving a player's wallet for game services), **exclude player-to-player transfers**:

**INCLUDE in spending baseline** (Gold sinks — Gold leaves the economy or buys services):
- Mua ở Cửa Hàng (ShopBuy, FashionShopBuy, LuckyStarBox)
- Trân Bảo Hành (TreasureShop)
- Đấu Giá (Auction logways)
- Trang Bị (EquipForge, EquipEnhance, EquipJingZhu, etc.)
- Bảo Thạch (Stone logways)
- Khí Linh (QiHun logways)
- Thú Cưỡi (Horse logways)
- Ngũ Hành Ấn (WuXingYin logways)
- Kỹ Năng Sống (LifeSkill logways)
- Đồng Hành (Partner logways)
- Bang Hội (KinDonate, CreateTong, CreateKin, etc.)
- Lì Xì / RedBag
- Nạp (Recharge-related: BattlePassBuy, etc.)
- Khác (most system costs: ChangeName, ResetSkill, AddBagExpandCount, etc.)
- Hoạt Động (activity entry fees, etc.)

**EXCLUDE from spending baseline** (player-to-player transfers — Gold moves between roles, not destroyed):
- "Đến ngay" group: LogWay_Trade (bigtype 21), LogWay_MarketStallCostMoneyBuy (37), LogWay_MarketStallCostItem (38), LogWay_MarketStallCancelItem (39), LogWay_MarketStallGetMoney (40), LogWay_MarketStallGetItem (41), LogWay_MarketUpdateItemCost (42), LogWay_MarketStallCostMoneyNewSell (43)

Note: MarketStall listing fees (bigtype 43) could arguably be included as a sink (fee goes to system), but for simplicity, exclude the entire "Đến ngay" group.

### 3.2 Gold inflow classification (for moneychange_add)

- **Recharge Gold:** `logway_name = 'LogWay_Recharge'` (bigtype 89) — real money top-up
- **Player transfer Gold:** "Đến ngay" group logways — Gold received from other players
- **System Gold:** Everything else in moneychange_add (rewards, refunds, mail, etc.)

---

## 4. Cohort definition

Players are segmented into 3 cohorts based on `viplevel` (available on every transaction row):

| Cohort | VIP level | % of players | % of revenue | Avg lifetime VND |
|---|---|---|---|---|
| **Whale** | 12+ | 1.1% | 47.5% | 62.7M |
| **Dolphin** | 7–11 | 10.8% | 39.5% | 5.5M |
| **Minnow** | 0–6 | 88.1% | 13.0% | 0.2M |

The VIP 12 cutoff was chosen because average lifetime spend doubles from VIP 11 (16M) to VIP 12 (34.7M) — a natural breakpoint.

`viplevel` is used as a **proxy** for lifetime spend because it's available on every row without needing a separate lookup query. The alternative (SUM of all historical recharge per roleid) is more accurate but expensive to compute daily.

---

## 5. Daily metrics schema

The pipeline should produce one row per (date, cohort) with these fields:

### 5.1 Primary metric (what ETS predicts)
- `total_gold_spent` — SUM(imoney) from moneychange_reduce, excluding player transfers

### 5.2 Context metrics (explain WHY spending moved)
- `active_spenders` — COUNT(DISTINCT roleid) in moneychange_reduce (excluding transfers)
- `new_spenders` — COUNT of roleid that appear in today's spend but NOT in the past 7 days
- `returning_spenders` — active_spenders minus new_spenders
- `avg_spend_per_role` — total_gold_spent / active_spenders
- `balance_velocity` — total_gold_spent / (total_gold_BOD + total_gold_received), where:
  - `total_gold_BOD` = SUM of first `before` value per role per day (from moneychange_reduce)
  - `total_gold_received` = SUM(imoney) from moneychange_add for that cohort
- `avg_balance_eod` — AVG of last `after` value per role per day
- `total_gold_received` — SUM(imoney) from moneychange_add
- `gold_from_recharge` — SUM(imoney) from moneychange_add WHERE logway_name = 'LogWay_Recharge'
- `total_vnd_recharged` — SUM(price/100) from recharge_deliver
- `event_flag` — 0 or 1, from the event calendar

### 5.3 Shop breakdown (separate table/query)
- GROUP BY ds, cohort, logway_group → SUM(imoney)
- Using the "Nhóm lớn" classification from the logway reference

---

## 6. ETS model specification

### 6.1 Method
Exponential Triple Smoothing (Holt-Winters) from Python `statsmodels.tsa.holtwinters.ExponentialSmoothing`.

### 6.2 Parameters
- Trend: additive (game spending doesn't grow exponentially)
- Seasonal: additive, period=7 (weekly cycle)
- Alpha (smoothing): start at 0.2, let statsmodels optimize
- Minimum training data: 56 days (8 weeks)
- Recommended training data: 180 days (6 months)

### 6.3 Event handling
- **Training:** Exclude days where `event_flag = 1` from the training set. The doc recommends excluding 3 days before + 5 days after each major event.
- **Forecasting:** If an event is scheduled in the forecast window, apply an `event_multiplier` estimated from historical event data (avg spending during past events / avg baseline).

### 6.4 Forecast horizon
- Reliable: 7 days (~85% accuracy, MAPE ~15%)
- Maximum useful: 14 days (~72% accuracy, MAPE ~28%)
- Beyond 14 days: switch to scenario planning, not point prediction
- Rule of thumb: horizon ≤ 1/10 of training window

### 6.5 Confidence band
- P50 = baseline forecast
- P75 (upper) = baseline × 1.15 (or use model's built-in confidence interval)
- P25 (lower) = baseline × 0.85
- Signal: actual > P75 = "over-baseline", actual < P25 = "under-baseline"

### 6.6 Recalibration triggers
- MAPE > 15% for 7 consecutive days
- Actual consistently outside confidence band without event
- Major game update changing earn/spend mechanics
- Large cohort composition shift (e.g., major UA campaign)

---

## 7. Alert rules

Pattern-based signals combining multiple metrics over multiple days:

| Pattern | Signal | Interpretation |
|---|---|---|
| Spend ↓ + velocity stable + spenders ↓ | Churn | Players leaving, not a content problem |
| Spend ↓ + velocity ↓ + spenders stable | Hoarding | Players online but saving Gold — event incoming? |
| Spend stable + recharge ↓ + balance ↓ | Reserve burn | Spending fueled by old Gold, revenue risk ahead |
| Spend ↑ + velocity ↑ + new_spenders ↑ | UA working | New campaign bringing actual spenders |
| Spend > P75 + no event flagged | Unknown spike | Investigate: unflagged event? Whale return? |
| Spend < P25 + no event flagged | Content gap | Consider pushing event earlier |
| Whale velocity < 0.40 for 3+ days | Whale hoarding | Likely spike when next event drops |

Threshold values (0.40, 3 days, etc.) should be configurable in `config/settings.json`.

---

## 8. Event calendar

### 8.1 Source
Excel file: `KTO_Nội_Dung_Vận_Hành.xlsx` with sheets per year (2023, 2024, 2025, 2026).

### 8.2 Structure
Gantt-style layout:
- Row 1: dates across columns (datetime objects, starting from column C)
- Row 2: day-of-week numbers
- Rows 3+: event tracks (column A = "Plan vận hành", column B = track number 1-N)
- Event names are placed at their start column, spanning across their duration
- Some events include revenue notes in the name like "(2.5B, 3.1K PU)" — extract but don't use for model training

### 8.3 Pipeline task
Parse the xlsx into a clean JSON:
```json
[
  {
    "start_date": "2026-01-07",
    "end_date": "2026-01-20",
    "event_name": "TNTB",
    "track": 2,
    "year": 2026
  }
]
```

Determine end_date by scanning rightward from the start cell until the next non-null cell in the same row (that's the next event) or end of dates.

**Important:** Ignore the REV sheet and any revenue numbers. Only extract event names and date ranges.

### 8.4 Rolling updates
The xlsx is maintained by the ops team and updated regularly. The pipeline should re-parse it on each run. Past events become historical reference for event multiplier calculation. Future events feed into the forecast.

---

## 9. Dashboard design

### 9.1 Overview
7-section dark-theme dashboard. React-based static site. Reference mockup: `kto_spending_dashboard_dark.jsx` (provided as artifact).

### 9.2 Sections

**Section 1 — Yesterday's health check**
4 metric cards: actual spend, baseline (P50), deviation %, spender count.
Color-coded: green if within ±15%, amber if ±15-25%, red if >±25%.

**Section 2 — Cohort breakdown**
Table with columns: Cohort, Spend, Velocity, Avg/role, Spenders (+new), vs Baseline.
One row per cohort (Whale, Dolphin, Minnow). Deviation color-coded.

**Section 3 — 30-day trend chart**
Line chart: actual (solid blue), baseline (dashed), confidence band (grey fill).
Green dots for over-baseline days, coral dots for under-baseline. Event periods highlighted in amber.

**Section 4 — Velocity & spender trends (7-day sparklines)**
3 rows (one per cohort), each with 2 mini-cards side by side:
- Left: velocity sparkline + current value + trend label (stable/declining)
- Right: spender count sparkline + current value + new spenders count

**Section 5 — Gold sinks by shop**
Horizontal bar chart showing spending by logway group. Excludes player transfers.
Uses Vietnamese logway group names from reference data.

**Section 6 — Alerts & signals**
List of pattern-based alert cards with severity (warning/info/success), cohort tag, and descriptive message combining multiple metrics.

**Section 7 — 7-day forecast**
Table with columns: Date, Forecast, Lower, Upper, Confidence.
Event days highlighted. Confidence column turns red when >±25%.

**Footer:** Data source, filter, cohort definition, model parameters.

### 9.3 Theme
Dark background (#0c0c0e), monospace typography. Cohort colors: Whale = coral (#F0997B), Dolphin = teal (#5DCAA5), Minnow = blue (#85B7EB).

---

## 10. Project folder structure

```
kto-spending-baseline/
├── config/
│   ├── trino.env                    ← Connection: host, port, catalog, schema (gitignored)
│   ├── credentials.env              ← Auth credentials (gitignored)
│   └── settings.json                ← Cohort thresholds, alert thresholds, model params
├── ref_data/
│   ├── logway_descriptions.json     ← From kto_ingame_data_descriptions_no_item.json
│   ├── logway_spending_filter.json  ← Which logways = "real spending" vs "transfer"
│   ├── item_mapping.json            ← From kto_item.json (itemid → name)
│   ├── vip_cohort_map.json          ← VIP level → cohort assignment
│   └── table_descriptions.json      ← Full KTO table/column reference
├── event_calendar/
│   ├── KTO_Nội_Dung_Vận_Hành.xlsx  ← Source file (updated by ops team)
│   ├── events.json                  ← Parsed output (generated by pipeline)
│   └── README.md                    ← Format docs for manual updates
├── sql/
│   ├── daily_aggregation.sql        ← Main: 3 tables → daily cohort metrics
│   ├── cohort_classification.sql    ← Lifetime spend → cohort assignment
│   ├── balance_velocity.sql         ← Daily velocity per cohort
│   ├── shop_breakdown.sql           ← Spend by logway group per day
│   └── forecast_input.sql           ← Historical data for ETS training
├── pipeline/
│   ├── fetch_daily.py               ← Execute SQL, save to data/
│   ├── parse_event_calendar.py      ← xlsx → events.json
│   ├── build_baseline.py            ← ETS model training + forecast
│   ├── generate_alerts.py           ← Rule-based warning signals
│   └── run_pipeline.sh              ← Daily cron: fetch → parse → model → alerts → deploy
├── data/
│   ├── daily_metrics.csv            ← Append-only daily output
│   ├── forecast.json                ← Latest 7-day forecast
│   ├── alerts.json                  ← Current active alerts
│   └── velocity_history.csv         ← Rolling velocity per cohort
├── dashboard/
│   ├── index.html                   ← Static dashboard entry point
│   ├── app.jsx                      ← React source
│   └── style.css                    ← Dark theme styles
├── docs/
│   ├── Mo_Hinh_Du_Doan_Daily_Spending.docx  ← Original framework document
│   ├── project_spec.md              ← THIS DOCUMENT
│   └── dashboard_layout.md          ← Section descriptions & mockup notes
├── .gitignore                       ← config/*.env, data/, node_modules/
├── .env.example                     ← Template for credentials
├── AGENTS.md                        ← Claude Code system prompt
└── README.md                        ← Project overview & setup
```

---

## 11. Reference files provided

These files should be placed in the project folder before starting:

| File | Destination | Contents |
|---|---|---|
| `Mo_Hinh_Du_Doan_Daily_Spending.docx` | `docs/` | Original framework document (Vietnamese) |
| `kto_ingame_data_descriptions_no_item.json` | `ref_data/` | 121 KTO tables + logway reference with groups |
| `kto_item.json` | `ref_data/` | Item ID → item name mapping |
| `KTO_Nội_Dung_Vận_Hành.xlsx` | `event_calendar/` | Event calendar (2023-2026, rolling updates) |
| `kto_spending_dashboard_dark.jsx` | `dashboard/` | Dashboard mockup with sample data |
| `moneychange_reduce sample` | `docs/` | 10-row sample for reference |
| `moneychange_add sample` | `docs/` | 10-row sample for reference |
| `recharge_deliver sample` | `docs/` | 10-row sample for reference |

---

## 12. Implementation priority

1. **Parse event calendar** → `events.json`
2. **Write SQL queries** → daily aggregation, shop breakdown
3. **Build fetch pipeline** → connect to Trino, execute queries, save CSV
4. **ETS model** → train on historical data, generate forecast
5. **Alert rules** → pattern detection across multiple days
6. **Dashboard** → React static site reading from `data/` folder
7. **Deploy** → Netlify or equivalent static hosting

Start with steps 1-3 using real KTO data to validate the metrics make sense before building the model and dashboard.

---

## 13. Config defaults (settings.json)

```json
{
  "cohorts": {
    "whale": { "vip_min": 12, "vip_max": 99, "color": "#F0997B" },
    "dolphin": { "vip_min": 7, "vip_max": 11, "color": "#5DCAA5" },
    "minnow": { "vip_min": 0, "vip_max": 6, "color": "#85B7EB" }
  },
  "price_divisor": 100,
  "moneytype_filter": "Gold",
  "transfer_logways_exclude": [21, 37, 38, 39, 40, 41, 42, 43],
  "recharge_logway_bigtype": 89,
  "ets": {
    "seasonal_period": 7,
    "trend": "add",
    "seasonal": "add",
    "min_training_days": 56,
    "forecast_horizon": 7,
    "confidence_band_pct": 0.15
  },
  "alerts": {
    "velocity_whale_threshold": 0.40,
    "consecutive_days_trigger": 3,
    "deviation_warning_pct": 15,
    "deviation_critical_pct": 25
  },
  "event_calendar": {
    "exclude_days_before_event": 3,
    "exclude_days_after_event": 5
  }
}
```
