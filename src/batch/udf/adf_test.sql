-- Approximate ADF UDF (screening only)
CREATE OR REPLACE FUNCTION `comp6231-project.stock.adf_test`(prices ARRAY<FLOAT64>)
RETURNS STRUCT<adf_stat FLOAT64, p_value FLOAT64>
AS (
  -- Wrap the entire logic in an outer set of parentheses to form a scalar subquery
  (SELECT STRUCT(
    SAFE_DIVIDE(components.num, components.denom) AS adf_stat,
    CASE
      -- Reference the calculated adf_stat from the same scope if possible, or recalculate
      WHEN SAFE_DIVIDE(components.num, components.denom) < -0.25 THEN 0.01
      WHEN SAFE_DIVIDE(components.num, components.denom) < -0.15 THEN 0.05
      WHEN SAFE_DIVIDE(components.num, components.denom) < -0.08 THEN 0.10
      ELSE 0.30
    END AS p_value
  )
  FROM 
    -- Use subqueries (inline views) instead of CTEs
    (
      SELECT SUM(diffs.diff * diffs.prev_price) AS num, SUM(diffs.prev_price * diffs.prev_price) AS denom
      FROM 
        (
          SELECT 
            price, 
            prev_price, 
            price - prev_price AS diff
          FROM 
            (
              SELECT 
                price, 
                LAG(price) OVER (ORDER BY OFFSET) AS prev_price
              FROM UNNEST(prices) AS price WITH OFFSET
            ) AS indexed
          WHERE prev_price IS NOT NULL
        ) AS diffs
    ) AS components
  )
);
