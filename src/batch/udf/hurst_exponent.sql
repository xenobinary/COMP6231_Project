-- Hurst Exponent UDF (simplified R/S method)
CREATE OR REPLACE FUNCTION `comp6231-project.stock.hurst_exponent`(prices ARRAY<FLOAT64>)
RETURNS FLOAT64
AS (
  -- Wrap everything in outer parentheses to treat it as a single scalar expression
  (SELECT CASE
    -- Reference aliases from the final FROM clause
    WHEN stdev.sd = 0 OR stdev.sd IS NULL THEN 0.5
    ELSE LEAST(1.0, GREATEST(0.0, (LOG(range_series.range_val / stdev.sd) / LOG(stats.n))))
  END
  FROM
    -- Subquery for Range Calculation
    (
      SELECT MAX(cum_dev) - MIN(cum_dev) AS range_val
      FROM
        -- Subquery for Cumulative Deviation
        (
          SELECT 
            -- Calculate cumulative deviation using window functions directly
            SUM(price - stats.mean_price) OVER (ORDER BY idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_dev
          FROM 
            (SELECT OFFSET AS idx, price FROM UNNEST(prices) AS price WITH OFFSET) AS indexed,
            -- Need to access stats.mean_price in this scope
            (
              SELECT COUNT(*) AS n, AVG(price) AS mean_price FROM (SELECT OFFSET AS idx, price FROM UNNEST(prices) AS price WITH OFFSET)
            ) AS stats
        ) AS cum
    ) AS range_series,

    -- Subquery for Standard Deviation
    (
      SELECT STDDEV_SAMP(price) AS sd 
      FROM (SELECT OFFSET AS idx, price FROM UNNEST(prices) AS price WITH OFFSET) AS indexed
    ) AS stdev,

    -- Subquery for basic stats (specifically N)
    (
      SELECT COUNT(*) AS n, AVG(price) AS mean_price 
      FROM (SELECT OFFSET AS idx, price FROM UNNEST(prices) AS price WITH OFFSET) AS indexed
    ) AS stats
  )
);
