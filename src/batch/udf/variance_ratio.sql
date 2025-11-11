-- Variance Ratio UDF (single lag k=2)
CREATE OR REPLACE FUNCTION `comp6231-project.stock.variance_ratio`(prices ARRAY<FLOAT64>)
RETURNS FLOAT64
AS (
  -- Wrap the entire body in outer parentheses
  (SELECT CASE 
      -- Reference the aliases from the final FROM clause
      WHEN var1_data.v1 = 0 OR var1_data.v1 IS NULL THEN 0 
      ELSE SAFE_DIVIDE(var_k_data.vk, 2 * var1_data.v1) 
    END
  FROM
    -- Subquery for the first variance (k=1 lag)
    (
      SELECT VAR_SAMP(diff) AS v1 
      FROM 
        ( -- Calculate lag 1 differences
          SELECT price - LAG(price) OVER (ORDER BY idx) AS diff
          FROM (SELECT OFFSET AS idx, price FROM UNNEST(prices) AS price WITH OFFSET) AS indexed
        ) AS diffs
      WHERE diff IS NOT NULL
    ) AS var1_data,
    
    -- Subquery for the second variance (k=2 lag)
    (
      SELECT VAR_SAMP(diff2) AS vk 
      FROM 
        ( -- Calculate lag 2 differences
          SELECT price - LAG(price, 2) OVER (ORDER BY idx) AS diff2
          FROM (SELECT OFFSET AS idx, price FROM UNNEST(prices) AS price WITH OFFSET) AS indexed
        ) AS aggregated
      WHERE diff2 IS NOT NULL
    ) AS var_k_data
  )
);

