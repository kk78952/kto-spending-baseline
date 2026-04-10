-- =============================================================================
-- cohort_classification.sql
-- Alternative cohort assignment based on actual lifetime VND spend
-- (more accurate than viplevel proxy but expensive — run weekly, not daily).
--
-- This query scans all historical recharge_deliver to sum actual VND paid.
-- Use only for periodic calibration checks, not daily pipeline.
--
-- Parameters:
--   :as_of_date   YYYY-MM-DD  (upper bound of lifetime window)
-- =============================================================================

WITH lifetime_spend AS (
  SELECT
    roleid,
    SUM(CAST(price AS DOUBLE) / 100.0) AS lifetime_vnd
  FROM hive.kto_658.recharge_deliver
  WHERE ds <= :as_of_date
  GROUP BY roleid
)

SELECT
  roleid,
  lifetime_vnd,
  CASE
    WHEN lifetime_vnd >= 34700000 THEN 'whale'    -- VIP12+ equivalent (34.7M VND avg)
    WHEN lifetime_vnd >=  5500000 THEN 'dolphin'  -- VIP7-11 equivalent (5.5M VND avg)
    ELSE                               'minnow'
  END AS cohort_by_spend,
  -- Reference: current viplevel from latest transaction
  -- (join to moneychange_reduce or moneychange_add for current viplevel)
  :as_of_date AS as_of_date
FROM lifetime_spend
ORDER BY lifetime_vnd DESC
