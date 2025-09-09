-- DQ checks for raw_orders
-- This script returns a table of checks and failing_count

WITH base AS (
  SELECT
    order_id,
    NULLIF(TRIM(order_date),'') AS order_date,
    amount,
    profit,
    quantity
  FROM public.raw_orders
)

, check_null_order_id AS (
  SELECT 'null_order_id' AS check_name, COUNT(*) AS failing_count
  FROM base WHERE order_id IS NULL
)
, check_null_order_date AS (
  SELECT 'null_order_date' AS check_name, COUNT(*) AS failing_count
  FROM base WHERE order_date IS NULL
)
, check_negative_amount AS (
  SELECT 'negative_amount' AS check_name, COUNT(*) AS failing_count
  FROM base WHERE amount < 0
)
, check_negative_profit AS (
  SELECT 'negative_profit' AS check_name, COUNT(*) AS failing_count
  FROM base WHERE profit < -10000 -- example: extremely negative profit threshold
)
, check_profit_ratio AS (
  SELECT 'profit_ratio_gt_5' AS check_name, COUNT(*) AS failing_count
  FROM base
  WHERE amount IS NOT NULL AND amount > 0 AND (profit/amount) > 5
)
, check_orderid_duplicates AS (
  SELECT 'duplicate_order_id' AS check_name, COUNT(*) - COUNT(DISTINCT order_id) AS failing_count
  FROM base WHERE order_id IS NOT NULL
)

SELECT * FROM check_null_order_id
UNION ALL
SELECT * FROM check_null_order_date
UNION ALL
SELECT * FROM check_negative_amount
UNION ALL
SELECT * FROM check_negative_profit
UNION ALL
SELECT * FROM check_profit_ratio
UNION ALL
SELECT * FROM check_orderid_duplicates;
