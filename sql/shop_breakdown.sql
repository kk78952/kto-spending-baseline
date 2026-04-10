-- =============================================================================
-- shop_breakdown.sql
-- Gold spending grouped by logway group (Nhóm lớn) and cohort for one day.
-- Used in dashboard Section 5 — Gold sinks bar chart.
--
-- Parameters:
--   :target_date  YYYY-MM-DD
-- =============================================================================

-- NOTE: logway_group values must be joined from the logway reference data
-- (ref_data/logway_descriptions.json → Nhóm lớn cách sinh ra tiền tệ).
-- Because Trino doesn't have that mapping as a table, the pipeline
-- infers it via a CASE WHEN on big_type_logway, derived from the JSON.

-- This query is a template. The pipeline substitutes the CASE WHEN block
-- from the logway reference at runtime. The static mapping below covers
-- the main groups from the spec.

WITH spending AS (
  SELECT
    ds,
    CASE WHEN viplevel >= 12 THEN 'whale' WHEN viplevel >= 7 THEN 'dolphin' ELSE 'minnow' END AS cohort,
    CASE big_type_logway
      -- Mua ở Cửa Hàng
      WHEN 7   THEN 'Mua ở Cửa Hàng'
      WHEN 14  THEN 'Mua ở Cửa Hàng'
      WHEN 68  THEN 'Mua ở Cửa Hàng'
      -- Trân Bảo Hành
      WHEN 24  THEN 'Trân Bảo Hành'
      WHEN 25  THEN 'Trân Bảo Hành'
      -- Trang Bị (equipment)
      WHEN 9   THEN 'Trang Bị'
      WHEN 10  THEN 'Trang Bị'
      WHEN 11  THEN 'Trang Bị'
      WHEN 12  THEN 'Trang Bị'
      WHEN 13  THEN 'Trang Bị'
      -- Bảo Thạch (stones)
      WHEN 15  THEN 'Bảo Thạch'
      WHEN 16  THEN 'Bảo Thạch'
      -- Thú Cưỡi (horses/mounts)
      WHEN 17  THEN 'Thú Cưỡi'
      WHEN 18  THEN 'Thú Cưỡi'
      -- Bang Hội (guild)
      WHEN 30  THEN 'Bang Hội'
      WHEN 31  THEN 'Bang Hội'
      WHEN 32  THEN 'Bang Hội'
      -- Lì Xì / RedBag
      WHEN 50  THEN 'Lì Xì'
      WHEN 51  THEN 'Lì Xì'
      -- Hoạt Động
      WHEN 8   THEN 'Hoạt Động'
      WHEN 55  THEN 'Hoạt Động'
      WHEN 56  THEN 'Hoạt Động'
      -- Đấu Giá (auction)
      WHEN 45  THEN 'Đấu Giá'
      WHEN 46  THEN 'Đấu Giá'
      -- Nạp (BattlePass etc.)
      WHEN 80  THEN 'Nạp'
      WHEN 81  THEN 'Nạp'
      -- Đến ngay group (EXCLUDED — player transfers)
      WHEN 21  THEN '__TRANSFER__'
      WHEN 37  THEN '__TRANSFER__'
      WHEN 38  THEN '__TRANSFER__'
      WHEN 39  THEN '__TRANSFER__'
      WHEN 40  THEN '__TRANSFER__'
      WHEN 41  THEN '__TRANSFER__'
      WHEN 42  THEN '__TRANSFER__'
      WHEN 43  THEN '__TRANSFER__'
      ELSE 'Khác'
    END AS logway_group,
    imoney
  FROM hive.kto_658.moneychange_reduce
  WHERE ds        = :target_date
    AND moneytype = 'Gold'
)

SELECT
  ds,
  cohort,
  logway_group,
  SUM(imoney)           AS total_gold_spent,
  COUNT(*)              AS transaction_count
FROM spending
WHERE logway_group != '__TRANSFER__'
GROUP BY ds, cohort, logway_group
ORDER BY total_gold_spent DESC
