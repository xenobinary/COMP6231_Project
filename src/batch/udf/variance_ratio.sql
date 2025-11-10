-- Variance Ratio UDF (single lag k=2)
CREATE OR REPLACE FUNCTION `project.dataset.variance_ratio`(prices ARRAY<FLOAT64>)
RETURNS FLOAT64
AS (
  WITH indexed AS (
    SELECT OFFSET AS idx, price FROM UNNEST(prices) AS price WITH OFFSET
  ), diffs AS (
    SELECT idx, price - LAG(price) OVER (ORDER BY idx) AS diff
    FROM indexed
  ), var1 AS (
    SELECT VAR_SAMP(diff) AS v1 FROM diffs WHERE diff IS NOT NULL
  ), aggregated AS (
    SELECT idx, price - LAG(price, 2) OVER (ORDER BY idx) AS diff2
    FROM indexed
  ), var_k AS (
    SELECT VAR_SAMP(diff2) AS vk FROM aggregated WHERE diff2 IS NOT NULL
  )
  SELECT CASE WHEN v1 = 0 OR v1 IS NULL THEN 0 ELSE SAFE_DIVIDE(vk, 2 * v1) END FROM var1, var_k
);
