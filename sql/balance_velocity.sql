-- =============================================================================
-- balance_velocity.sql
-- Daily balance velocity per cohort:
--   velocity = total_gold_spent / (BOD_balance + gold_received_today)
--
-- Interpretation: fraction of available Gold actually spent.
--   High = players spending aggressively
--   Low  = players hoarding (watch for event-driven spike)
--
-- Parameters:
--   :target_date  YYYY-MM-DD
-- =============================================================================

WITH spend_bod AS (
  -- First transaction per role gives the beginning-of-day balance (before field)
  SELECT
    CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
    roleid,
    MIN_BY(before, time)  AS bod_balance,
    SUM(imoney)           AS role_gold_spent
  FROM hive.kto_658.moneychange_reduce
  WHERE ds          = :target_date
    AND moneytype   = 'Gold'
    AND big_type_logway NOT IN (21, 37, 38, 39, 40, 41, 42, 43)
  GROUP BY 1, roleid
),

inflow_per_role AS (
  SELECT
    CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
    roleid,
    SUM(imoney) AS gold_received
  FROM hive.kto_658.moneychange_add
  WHERE ds        = :target_date
    AND moneytype = 'Gold'
  GROUP BY 1, roleid
),

cohort_totals AS (
  SELECT
    s.cohort,
    SUM(s.bod_balance)                             AS total_bod_balance,
    SUM(COALESCE(i.gold_received, 0))              AS total_gold_received,
    SUM(s.role_gold_spent)                         AS total_gold_spent
  FROM spend_bod s
  LEFT JOIN inflow_per_role i ON i.roleid = s.roleid AND i.cohort = s.cohort
  GROUP BY s.cohort
)

SELECT
  :target_date                                    AS ds,
  cohort,
  total_gold_spent,
  total_bod_balance,
  total_gold_received,
  total_bod_balance + total_gold_received          AS total_available,
  ROUND(
    CAST(total_gold_spent AS DOUBLE) / NULLIF(total_bod_balance + total_gold_received, 0),
    4
  )                                               AS balance_velocity
FROM cohort_totals
ORDER BY cohort
