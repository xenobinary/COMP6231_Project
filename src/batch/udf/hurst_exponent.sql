-- Hurst Exponent UDF (simplified R/S method)
CREATE OR REPLACE FUNCTION `project.dataset.hurst_exponent`(prices ARRAY<FLOAT64>)
RETURNS FLOAT64
AS (
  WITH indexed AS (
    SELECT OFFSET AS idx, price FROM UNNEST(prices) AS price WITH OFFSET
  ), stats AS (
    SELECT
      COUNT(*) AS n,
      AVG(price) AS mean_price,
      SUM(price) AS sum_price
    FROM indexed
  ), cum AS (
    SELECT i.idx,
      SUM(j.price - s.mean_price) OVER (ORDER BY i.idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_dev
    FROM indexed i
    CROSS JOIN indexed j
    CROSS JOIN stats s
    WHERE j.idx <= i.idx
  ), range_series AS (
    SELECT MAX(cum_dev) - MIN(cum_dev) AS range_val FROM cum
  ), stdev AS (
    SELECT STDDEV_SAMP(price) AS sd FROM indexed
  )
  SELECT CASE
    WHEN sd = 0 OR sd IS NULL THEN 0.5
    ELSE LEAST(1.0, GREATEST(0.0, (LOG(range_val / sd) / LOG(n))))
  END
  FROM range_series, stdev, stats
);
