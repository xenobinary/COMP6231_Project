"""Intraday 5-min backfill ingestion using yfinance.

Fetches 5-min OHLCV for S&P 500 symbols and writes to BigQuery.
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional
from io import StringIO

import pandas as pd
import pandas_gbq as pd_gbq

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

# BigQuery imports are loaded lazily in write_to_bigquery to avoid import errors
# during local development without GCP libs installed.

# Configuration (env-overridable)
SYMBOL_SOURCE = os.getenv("SYMBOL_SOURCE", "sp500")  # sp500 | nasdaq | file | env
INCLUDE_ETF = os.getenv("INCLUDE_ETF", "false").lower() in {"1", "true", "yes", "y"}
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "0")) or None  # optional cap for testing
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))  # yfinance multi-download batch
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
_DEFAULT_END = datetime.now(timezone.utc).strftime("%Y-%m-%d")
_DEFAULT_START = (
    datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
).strftime("%Y-%m-%d")
START_DATE = os.getenv("START_DATE", _DEFAULT_START)
END_DATE = os.getenv("END_DATE", _DEFAULT_END)

# Local testing: skip BigQuery load and just print summary
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes", "y"}

# BigQuery configuration
BQ_PROJECT = os.getenv("BQ_PROJECT", "comp6231-project")  # default project for testing
BQ_DATASET = os.getenv("BQ_DATASET", "stock")
BQ_TABLE = os.getenv("BQ_TABLE", "ohlcv_5min")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")  # optional fully-qualified table: project.dataset.table

# Back-compat: allow explicit UNIVERSE env if provided
UNIVERSE_ENV = os.getenv("UNIVERSE")


def _read_symbols_file(path: str) -> List[str]:
    df = pd.read_csv(path)
    col = None
    for c in df.columns:
        if str(c).lower() in {"symbol", "ticker", "asset"}:
            col = c
            break
    if col is None:
        raise RuntimeError("Symbols file must contain a 'symbol'/'ticker'/'Asset' column")
    syms = (
        df[col]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(".", "-", regex=False)
        .tolist()
    )
    return syms


def get_all_symbols() -> List[str]:
    """Return a list of stock symbols to ingest.

    Priority:
      1) If UNIVERSE env is set, use it (comma-separated)
      2) If SYMBOL_SOURCE=sp500 (default), fetch S&P500 symbols
    """
    if UNIVERSE_ENV:
        syms = [s.strip().upper() for s in UNIVERSE_ENV.split(",") if s.strip()]
        return syms[:MAX_SYMBOLS] if MAX_SYMBOLS else syms

    if SYMBOL_SOURCE == "sp500":
        # Priority order: explicit file -> local default file -> Wikipedia -> DataHub -> minimal fallback
        explicit_file = os.getenv("SYMBOLS_FILE")
        # Always prefer an explicit override, otherwise fall back to the local metadata file in the repo
        local_default = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data",
            "metadata",
            "sp500_symbols.csv",
        )
        try:
            if explicit_file and os.path.exists(explicit_file):
                syms = _read_symbols_file(explicit_file)
                if MAX_SYMBOLS:
                    syms = syms[:MAX_SYMBOLS]
                return syms
            if os.path.exists(local_default):
                syms = _read_symbols_file(local_default)
                if MAX_SYMBOLS:
                    syms = syms[:MAX_SYMBOLS]
                return syms

            # Try Wikipedia with a browser-like User-Agent to avoid 403
            try:
                import requests  # type: ignore
            except Exception:
                requests = None  # type: ignore

            if requests is not None:
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
                    )
                }
                url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
                resp = requests.get(url, headers=headers, timeout=20)
                resp.raise_for_status()
                tables = pd.read_html(StringIO(resp.text))
                sp = tables[0]
                sym_col = next((c for c in sp.columns if str(c).lower() == "symbol"), None)
                if sym_col is None:
                    raise RuntimeError("Cannot find Symbol column in S&P 500 table")
                syms = (
                    sp[sym_col]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                    .str.replace(".", "-", regex=False)
                    .tolist()
                )
                if MAX_SYMBOLS:
                    syms = syms[:MAX_SYMBOLS]
                return syms

            # Try DataHub CSV mirror
            if requests is not None:
                datahub = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
                resp = requests.get(datahub, timeout=20)
                resp.raise_for_status()
                syms = _read_symbols_file(StringIO(resp.text))  # type: ignore[arg-type]
                if MAX_SYMBOLS:
                    syms = syms[:MAX_SYMBOLS]
                return syms

        except Exception as e:
            print(f"Failed to load S&P 500 list: {e}")
            # fall through to minimal fallback below

        # Minimal fallback
        syms = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]
        if MAX_SYMBOLS:
            syms = syms[:MAX_SYMBOLS]
        return syms

    raise RuntimeError(f"Unsupported SYMBOL_SOURCE for 5min data: {SYMBOL_SOURCE}")


def chunked(it: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for x in it:
        batch.append(x)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def fetch_batch(symbols: List[str], start: str, end: str) -> pd.DataFrame:
    """Fetch a batch of tickers with yfinance and return a tidy DataFrame."""
    if yf is None:
        raise RuntimeError("yfinance not installed. Add to requirements.txt")
    tickers = " ".join(symbols)
    df = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        interval="5m",
        group_by="ticker",
        threads=True,
        progress=False,
        auto_adjust=False,
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["Datetime", "symbol", "Open", "High", "Low", "Close", "Volume"])

    # Single-ticker case: columns are simple, make it look like multi
    if not isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df["symbol"] = symbols[0]
        df = df.reset_index().rename(columns={"index": "Datetime"})
        return df[["Datetime", "symbol", "Open", "High", "Low", "Close", "Volume"]]

    # Multi-ticker case: normalize to tidy
    tidy_frames: List[pd.DataFrame] = []
    for sym in df.columns.get_level_values(0).unique():
        try:
            sub = df[sym].copy()
            if sub.empty:
                continue
            sub = sub.reset_index()
            sub["symbol"] = str(sym)
            # Ensure expected cols exist; fill missing if needed
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if col not in sub.columns:
                    sub[col] = pd.NA
            tidy_frames.append(sub[["Datetime", "symbol", "Open", "High", "Low", "Close", "Volume"]])
        except Exception:
            # Skip problematic symbol in this batch
            continue
    if not tidy_frames:
        return pd.DataFrame(columns=["Datetime", "symbol", "Open", "High", "Low", "Close", "Volume"])
    return pd.concat(tidy_frames, ignore_index=True)


def run_ingestion():
    start_dt = datetime.fromisoformat(START_DATE).date()
    end_dt = datetime.fromisoformat(END_DATE).date()

    # Build full symbol list
    try:
        symbols = get_all_symbols()
    except Exception as e:
        # Fallback to a tiny default if symbol discovery fails, but log error
        print(f"Symbol discovery failed: {e}")
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

    print(f"Fetching {len(symbols)} symbols from {start_dt} to {end_dt} (5m)")

    frames: List[pd.DataFrame] = []
    for batch in chunked(symbols, max(1, BATCH_SIZE)):
        try:
            batch_df = fetch_batch(batch, START_DATE, END_DATE)
            if not batch_df.empty:
                frames.append(batch_df)
        except Exception as e:
            print(f"Batch failed ({len(batch)} symbols): {e}")

    if not frames:
        print("No data fetched.")
        return

    all_df = pd.concat(frames, ignore_index=True)
    # Drop rows with all-NA prices to keep file clean
    price_cols = ["Open", "High", "Low", "Close", "Volume"]
    all_df = all_df.dropna(subset=price_cols, how="all")

    # Normalize columns to BigQuery schema
    all_df = all_df.rename(
        columns={
            "Datetime": "timestamp",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    all_df["timestamp"] = pd.to_datetime(all_df["timestamp"], errors="coerce", utc=True)
    # Enforce dtypes to match BQ schema best-effort
    for col in ["open", "high", "low", "close"]:
        all_df[col] = pd.to_numeric(all_df[col], errors="coerce")
    all_df["volume"] = pd.to_numeric(all_df["volume"], errors="coerce").astype("Int64")

    if DRY_RUN:
        print("DRY_RUN=true: skipping BigQuery load. Showing sample rows:")
        print(all_df.head(10))
        print(f"Total rows: {len(all_df)} | Symbols: {all_df['symbol'].nunique()}")
    else:
        # Write to BigQuery
        write_to_bigquery(all_df)
        print(f"Loaded {len(all_df)} rows into BigQuery table")


def write_to_bigquery(df: pd.DataFrame) -> None:
    try:
        from google.cloud import bigquery  # type: ignore
        from google.api_core.exceptions import NotFound  # type: ignore
        from google.auth.exceptions import DefaultCredentialsError  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "google-cloud-bigquery not installed or import failed. Add to requirements.txt"
        ) from e

    client = bigquery.Client(project=BQ_PROJECT) if BQ_PROJECT else bigquery.Client()
    table_id = BQ_TABLE_ID
    if not table_id:
        project = client.project
        table_id = f"{project}.{BQ_DATASET}.{BQ_TABLE}"

    # Ensure dataset exists
    dataset_ref = bigquery.DatasetReference.from_string(".".join(table_id.split(".")[:2]))
    try:
        client.get_dataset(dataset_ref)
    except NotFound:  # create dataset
        client.create_dataset(bigquery.Dataset(dataset_ref))

    # Ensure table exists with partitioning and clustering
    try:
        client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    if not table_exists:
        # Provide schema when creating
        job_config.schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INT64"),
        ]
        job_config.time_partitioning = bigquery.TimePartitioning(field="timestamp", type_="DAY")
        job_config.clustering_fields = ["symbol"]

    try:
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # wait for completion
    except DefaultCredentialsError as e:
        raise RuntimeError(
            "GCP credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS to a valid service account JSON file. "
            "Example: export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/key.json"
        ) from e


if __name__ == "__main__":
    run_ingestion()
