-- =============================================================================
-- forecast_input.sql
-- Pull historical daily_gold_spent per cohort for ETS training.
-- Returns one row per (ds, cohort) for the past N days.
--
-- Parameters:
--   :start_date   YYYY-MM-DD  (e.g. 180 days ago for recommended training window)
--   :end_date     YYYY-MM-DD  (yesterday, D-1)
-- =============================================================================

SELECT
  ds,
  CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
  SUM(imoney) AS total_gold_spent
FROM hive.kto_658.moneychange_reduce
WHERE ds          BETWEEN :start_date AND :end_date
  AND moneytype   = 'Gold'
  AND big_type_logway NOT IN (21, 37, 38, 39, 40, 41, 42, 43)
GROUP BY
  ds,
  CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END
ORDER BY ds, cohort
