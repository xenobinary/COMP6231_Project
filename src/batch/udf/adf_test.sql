-- Approximate ADF UDF (screening only)
CREATE OR REPLACE FUNCTION `project.dataset.adf_test`(prices ARRAY<FLOAT64>)
RETURNS STRUCT<adf_stat FLOAT64, p_value FLOAT64>
AS (
  WITH indexed AS (
    SELECT OFFSET + 1 AS t, price, LAG(price) OVER (ORDER BY OFFSET) AS prev_price
    FROM UNNEST(prices) AS price WITH OFFSET
  ), diffs AS (
    SELECT t, price, prev_price, price - prev_price AS diff
    FROM indexed WHERE prev_price IS NOT NULL
  ), components AS (
    SELECT SUM(diff * prev_price) AS num, SUM(prev_price * prev_price) AS denom
    FROM diffs
  )
  SELECT STRUCT(
    SAFE_DIVIDE(num, denom) AS adf_stat,
    CASE
      WHEN SAFE_DIVIDE(num, denom) < -0.25 THEN 0.01
      WHEN SAFE_DIVIDE(num, denom) < -0.15 THEN 0.05
      WHEN SAFE_DIVIDE(num, denom) < -0.08 THEN 0.10
      ELSE 0.30
    END AS p_value
  )
  FROM components
);
