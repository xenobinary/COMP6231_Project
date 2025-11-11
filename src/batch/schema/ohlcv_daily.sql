-- Daily OHLCV raw table (partitioned + clustered)
CREATE TABLE IF NOT EXISTS `comp6231-project.stock.ohlcv_daily` (
  date DATE,
  symbol STRING,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  close FLOAT64,
  adj_close FLOAT64,
  volume INT64
)
PARTITION BY date
CLUSTER BY symbol
OPTIONS (
  description = 'Daily OHLCV raw data, partitioned by date and clustered by symbol',
  require_partition_filter = TRUE
);
