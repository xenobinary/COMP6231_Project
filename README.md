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
