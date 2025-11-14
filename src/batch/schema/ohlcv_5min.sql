-- 5-min OHLCV raw table (partitioned + clustered)
CREATE TABLE IF NOT EXISTS `comp6231-project.stock.ohlcv_5min` (
  timestamp TIMESTAMP,
  symbol STRING,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  close FLOAT64,
  volume INT64
)
PARTITION BY DATE(timestamp)
CLUSTER BY symbol
OPTIONS (
  description = '5-min OHLCV raw data, partitioned by date and clustered by symbol',
  require_partition_filter = TRUE
);
