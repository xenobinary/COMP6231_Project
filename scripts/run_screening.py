"""Execute BigQuery screening query combining UDF outputs.

Requires GOOGLE_APPLICATION_CREDENTIALS and GCP_PROJECT env vars.
"""
import os
try:
  from google.cloud import bigquery  # type: ignore
except Exception:  # pragma: no cover
  bigquery = None

QUERY = """
-- Simplified screening query (expects table project.dataset.daily_ohlcv)
WITH base AS (
  SELECT symbol, ARRAY_AGG(close ORDER BY date DESC LIMIT 200) AS price_array
  FROM `PROJECT.dataset.daily_ohlcv`
  GROUP BY symbol
), stats AS (
  SELECT
    symbol,
    `project.dataset.adf_test`(price_array).adf_stat AS adf_stat,
    `project.dataset.adf_test`(price_array).p_value AS adf_pvalue,
    `project.dataset.hurst_exponent`(price_array) AS hurst,
    `project.dataset.variance_ratio`(price_array) AS vr
  FROM base
)
SELECT * FROM stats
WHERE adf_pvalue < 0.10 AND hurst < 0.5 AND vr < 1.2
ORDER BY adf_pvalue ASC
LIMIT 50
""".replace("PROJECT", os.getenv("GCP_PROJECT", "PROJECT"))


def main():
  if bigquery is None:
    raise RuntimeError("google-cloud-bigquery not installed. Add to requirements.txt")
  client = bigquery.Client(project=os.getenv("GCP_PROJECT"))
  job = client.query(QUERY)
  rows = list(job.result())
  print(f"Screened {len(rows)} symbols:")
  for r in rows:
    print(r.symbol, r.adf_pvalue, r.hurst, r.vr)


if __name__ == "__main__":
    main()
