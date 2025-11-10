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

```
python src/batch/ingestion.py
```

Upload CSV to BigQuery or adapt script to load directly (TODO).

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
