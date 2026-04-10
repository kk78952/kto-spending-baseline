-- =============================================================================
-- daily_aggregation.sql
-- Main query: produce one row per (ds, cohort) with all baseline metrics.
-- Run for a single partition date by substituting :target_date.
--
-- Parameters:
--   :target_date  YYYY-MM-DD  (e.g. '2026-04-09')
-- =============================================================================

WITH cohort_def AS (
  -- VIP → cohort mapping (applied per transaction row)
  SELECT
    viplevel,
    CASE
      WHEN viplevel >= 12 THEN 'whale'
      WHEN viplevel >= 7  THEN 'dolphin'
      ELSE                     'minnow'
    END AS cohort
  FROM (SELECT DISTINCT viplevel FROM hive.kto_658.moneychange_reduce WHERE ds = :target_date)
),

-- ── Spending (Gold outflows, excluding player transfers) ──────────────────────
spend_raw AS (
  SELECT
    r.ds,
    CASE WHEN r.viplevel >= 12 THEN 'whale' WHEN r.viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
    r.roleid,
    r.viplevel,
    r.imoney,
    r.before,
    r.after,
    r.logway_name,
    r.big_type_logway,
    ROW_NUMBER() OVER (PARTITION BY r.ds, r.roleid ORDER BY r.time ASC)  AS rn_first,
    ROW_NUMBER() OVER (PARTITION BY r.ds, r.roleid ORDER BY r.time DESC) AS rn_last
  FROM hive.kto_658.moneychange_reduce r
  WHERE r.ds          = :target_date
    AND r.moneytype   = 'Gold'
    AND r.big_type_logway NOT IN (21, 37, 38, 39, 40, 41, 42, 43)
),

spend_agg AS (
  SELECT
    ds,
    cohort,
    SUM(imoney)                                   AS total_gold_spent,
    COUNT(DISTINCT roleid)                        AS active_spenders,
    SUM(CASE WHEN rn_first = 1 THEN before END)  AS total_gold_bod,
    AVG(CASE WHEN rn_last  = 1 THEN after  END)  AS avg_balance_eod
  FROM spend_raw
  GROUP BY ds, cohort
),

-- ── New spenders: roleid not seen in past 7 days ──────────────────────────────
past_7d_spenders AS (
  SELECT DISTINCT roleid
  FROM hive.kto_658.moneychange_reduce
  WHERE ds          >= DATE_FORMAT(DATE_ADD('day', -7, DATE(CAST(:target_date AS DATE))), '%Y-%m-%d')
    AND ds          <  :target_date
    AND moneytype   = 'Gold'
    AND big_type_logway NOT IN (21, 37, 38, 39, 40, 41, 42, 43)
),

new_spenders AS (
  SELECT
    CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
    COUNT(DISTINCT roleid) AS new_spender_count
  FROM hive.kto_658.moneychange_reduce
  WHERE ds          = :target_date
    AND moneytype   = 'Gold'
    AND big_type_logway NOT IN (21, 37, 38, 39, 40, 41, 42, 43)
    AND roleid NOT IN (SELECT roleid FROM past_7d_spenders)
  GROUP BY 1
),

-- ── Gold inflow (moneychange_add) ─────────────────────────────────────────────
inflow_agg AS (
  SELECT
    CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
    SUM(imoney)                                            AS total_gold_received,
    SUM(CASE WHEN logway_name = 'LogWay_Recharge' THEN imoney ELSE 0 END) AS gold_from_recharge
  FROM hive.kto_658.moneychange_add
  WHERE ds        = :target_date
    AND moneytype = 'Gold'
  GROUP BY 1
),

-- ── VND recharged (recharge_deliver) ─────────────────────────────────────────
recharge_vnd AS (
  SELECT
    CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
    SUM(CAST(price AS DOUBLE) / 100.0) AS total_vnd_recharged
  FROM hive.kto_658.recharge_deliver
  WHERE ds = :target_date
  GROUP BY 1
)

-- ── Final join ────────────────────────────────────────────────────────────────
SELECT
  s.ds,
  s.cohort,
  s.total_gold_spent,
  s.active_spenders,
  COALESCE(n.new_spender_count, 0)                          AS new_spenders,
  s.active_spenders - COALESCE(n.new_spender_count, 0)      AS returning_spenders,
  CASE WHEN s.active_spenders > 0
    THEN ROUND(CAST(s.total_gold_spent AS DOUBLE) / s.active_spenders, 2)
    ELSE 0
  END                                                        AS avg_spend_per_role,
  -- balance_velocity = total_gold_spent / (BOD_balance + gold_received)
  CASE WHEN (s.total_gold_bod + COALESCE(i.total_gold_received, 0)) > 0
    THEN ROUND(
      CAST(s.total_gold_spent AS DOUBLE) / (s.total_gold_bod + COALESCE(i.total_gold_received, 0)),
      4)
    ELSE NULL
  END                                                        AS balance_velocity,
  ROUND(s.avg_balance_eod, 2)                               AS avg_balance_eod,
  COALESCE(i.total_gold_received, 0)                        AS total_gold_received,
  COALESCE(i.gold_from_recharge, 0)                         AS gold_from_recharge,
  COALESCE(r.total_vnd_recharged, 0)                        AS total_vnd_recharged,
  0                                                          AS event_flag  -- set by pipeline after event calendar check
FROM spend_agg       s
LEFT JOIN new_spenders n   ON n.cohort  = s.cohort
LEFT JOIN inflow_agg   i   ON i.cohort  = s.cohort
LEFT JOIN recharge_vnd r   ON r.cohort  = s.cohort
ORDER BY s.cohort
