# COMP6231 Project

Serverless mean reversion trading system (Two-Phase) per design document in `reports/design.tex`.

## Structure

```
src/
  batch/
    ingestion.py          # Daily OHLCV ingestion (yfinance)
    udf/                  # BigQuery UDF SQL definitions (ADF, Hurst, VR)
      adf_test.sql
      hurst_exponent.sql
      variance_ratio.sql
  realtime/
    producer.py           # Synthetic market data publisher (Pub/Sub)
    monitor.py            # Subscriber computing indicators & publishing signals
  indicators/
    technical.py          # Shared indicator computations
scripts/
  run_screening.py        # Executes screening query combining UDF outputs
configs/
  .env.example            # Environment variable template
reports/
  design.tex              # Architecture & implementation details
tests/
  test_indicators.py      # Basic indicator tests
requirements.txt          # Python dependencies
```

## Environment

Copy `.env.example` to `.env` and adjust values. Ensure Google credentials are set:

```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
export GCP_PROJECT=your-project-id
```

## Install

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Phase 1 Ingestion

This script fetches last-week 1d OHLCV for a symbol universe (S&P 500 by default) and writes directly to BigQuery.

Quick local dry run (no BigQuery):

```
SYMBOL_SOURCE=sp500 MAX_SYMBOLS=5 DRY_RUN=true LOOKBACK_DAYS=7 BATCH_SIZE=5 \
python src/batch/ingestion.py
```

Generate static S&P 500 list (recommended to avoid external fetches):

```
python scripts/make_sp500_symbols.py
```

Real BigQuery load:

```
export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/key.json
export GCP_PROJECT=your-project-id
export BQ_DATASET=stock
export BQ_TABLE=ohlcv_daily

SYMBOL_SOURCE=sp500 \
SYMBOLS_FILE=data/metadata/sp500_symbols.csv \
DRY_RUN=false LOOKBACK_DAYS=7 BATCH_SIZE=40 \
python src/batch/ingestion.py
```

If credentials are missing, you'll see a helpful error. The script creates the dataset/table if they don't exist, partitioned by `date` and clustered by `symbol`.

Deploy UDFs (example using bq CLI):

```
bq query --use_legacy_sql=false < src/batch/udf/adf_test.sql
bq query --use_legacy_sql=false < src/batch/udf/hurst_exponent.sql
bq query --use_legacy_sql=false < src/batch/udf/variance_ratio.sql
```

Run screening:

```
python scripts/run_screening.py
```

## Phase 2 Realtime

Start synthetic producer locally:

```
python src/realtime/producer.py
```

Start monitor (in another terminal):

```
python src/realtime/monitor.py
```

## Tests

```
pytest -q
```

## Next Steps

1. Implement BigQuery load job in `ingestion.py`.
2. Replace synthetic producer with real market data API integration.
3. Add Firestore watchlist management script.
4. Add deployment scripts (Cloud Run / Scheduler setup).
5. Enhance statistical rigor (full ADF via Python backtest pipeline).

## License

Academic project work — internal use.
# Distributed Stock Price Prediction Using Mean Reversion Signals

This project implements a two-phase, high-performance mean reversion trading workflow using Google's serverless analytics stack (BigQuery) and a streaming backbone (Kafka or RabbitMQ).

## Project Structure

- \`data/\`
  - \`historical/\`: Historical OHLCV data for batch screening (e.g., CSVs, Parquet files).
  - \`realtime/\`: Streaming data fixtures or local replicas for real-time signal testing.
  - \`metadata/\`: Universe definitions, corporate actions, trading calendars.

- \`sql/\`: BigQuery SQL scripts for batch screening and schema definitions.

- \`src/\`
  - \`batch/\`: Batch feature engineering and screening (ADF, VR, Hurst tests).
  - \`streaming/\`: Real-time indicator computation and signal generation.
  - \`backtest/\`: Backtesting, transaction-cost modeling, and parameter sweeps.
  - \`execution/\`: Interfaces for emitting signals to broker/execution simulator.

- \`notebooks/\`
  - \`batch/\`, \`streaming/\`, \`backtesting/\`: Jupyter notebooks for exploration and prototyping.

- \`configs/\`: Configuration files and runbooks (deployment, monitoring, cost controls).

- \`docs/\`: Documentation, runbooks, and operational guides.

- \`reports/\`
  - \`proposal.tex\`: Project proposal.
  - \`figures/\`: Generated figures for reports.
  - \`final_report.tex\`: Final results and recommendations.

- \`scripts/\`: Helper scripts (launch pipelines, backtests, etc.).

- \`tests/\`: Unit and integration tests.

## Getting Started

See \`docs/\` for setup and runbooks.
