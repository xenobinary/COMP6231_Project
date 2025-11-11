"""Daily batch ingestion using yfinance.

Fetches daily OHLCV for a large universe of symbols and writes CSV locally.
Designed to run on Cloud Run: sources S&P 500 symbols and downloads one week
of 1d bars, then writes directly to BigQuery.
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional
from io import StringIO

import pandas as pd

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
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "50"))
# Local testing: skip BigQuery load and just print summary
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes", "y"}
# OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/daily_ohlcv.csv")  # legacy; unused when writing to BQ

# BigQuery configuration
BQ_PROJECT = os.getenv("BQ_PROJECT", "comp6231-project")  # default project for testing
BQ_DATASET = os.getenv("BQ_DATASET", "stock")
BQ_TABLE = os.getenv("BQ_TABLE", "ohlcv_daily")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")  # optional fully-qualified table: project.dataset.table

# Back-compat: allow explicit UNIVERSE env if provided
UNIVERSE_ENV = os.getenv("UNIVERSE")


def _read_nasdaq_symbol_file(url: str, symbol_col: str) -> pd.DataFrame:
    """Read a NASDAQ Trader symbol directory text file into a DataFrame.

    Both nasdaqlisted.txt and otherlisted.txt are pipe-delimited and end with a
    footer line that must be skipped. We also filter out test issues by default.
    """
    try:
        df = pd.read_csv(
            url,
            sep="|",
            engine="python",
            dtype=str,
            skipfooter=1,  # skip the trailing metadata line
        )
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"Failed to load NASDAQ symbol file {url}: {e}")

    # Normalize columns if present
    cols = {c.lower(): c for c in df.columns}
    # Filter out test issues when column exists
    if "test issue" in cols:
        df = df[df[cols["test issue"]].fillna("").str.upper() != "Y"]
    # ETF filter if present
    if not INCLUDE_ETF and "etf" in cols:
        df = df[df[cols["etf"]].fillna("").str.upper() != "Y"]

    # Keep only the symbol column requested
    if symbol_col not in df.columns:
        # Try to fall back to case-insensitive match
        matches = [c for c in df.columns if c.lower() == symbol_col.lower()]
        if matches:
            symbol_col = matches[0]
        else:
            raise RuntimeError(f"Column {symbol_col} not found in {url}")

    return df[[symbol_col]].rename(columns={symbol_col: "symbol"})


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
      2) If SYMBOL_SOURCE=nasdaq (default), fetch NASDAQ + otherlisted
      3) If SYMBOL_SOURCE=file, read CSV with an 'Asset' or 'symbol' column
    """
    if UNIVERSE_ENV:
        syms = [s.strip().upper() for s in UNIVERSE_ENV.split(",") if s.strip()]
        return syms[:MAX_SYMBOLS] if MAX_SYMBOLS else syms

    if SYMBOL_SOURCE == "sp500":
        # Priority order: explicit file -> local default file -> Wikipedia -> DataHub -> minimal fallback
        explicit_file = os.getenv("SYMBOLS_FILE")
        local_default = "data/metadata/sp500_symbols.csv"
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

    if SYMBOL_SOURCE == "nasdaq":
        nasdaq_url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
        other_url = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
        df1 = _read_nasdaq_symbol_file(nasdaq_url, "Symbol")
        df2 = _read_nasdaq_symbol_file(other_url, "ACT Symbol")
        all_df = pd.concat([df1, df2], ignore_index=True)
        # Basic hygiene: drop blanks, uppercase, drop obvious non-standard entries
        syms = (
            all_df["symbol"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .tolist()
        )
        # Optional cap
        if MAX_SYMBOLS:
            syms = syms[:MAX_SYMBOLS]
        return syms

    if SYMBOL_SOURCE == "file":
        path = os.getenv("SYMBOLS_FILE", "data/historical/combined_stock_data.csv")
        if not os.path.exists(path):
            raise RuntimeError(f"SYMBOL_SOURCE=file but file not found: {path}")
        df = pd.read_csv(path)
        if "Asset" in df.columns:
            syms = sorted(df["Asset"].dropna().astype(str).str.upper().unique().tolist())
        elif "symbol" in df.columns:
            syms = sorted(df["symbol"].dropna().astype(str).str.upper().unique().tolist())
        else:
            raise RuntimeError("File must contain 'Asset' or 'symbol' column")
        if MAX_SYMBOLS:
            syms = syms[:MAX_SYMBOLS]
        return syms

    raise RuntimeError(f"Unknown SYMBOL_SOURCE: {SYMBOL_SOURCE}")


def chunked(it: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for x in it:
        batch.append(x)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def fetch_batch(symbols: List[str], start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch a batch of tickers with yfinance and return a tidy DataFrame."""
    if yf is None:
        raise RuntimeError("yfinance not installed. Add to requirements.txt")
    tickers = " ".join(symbols)
    df = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        interval="1d",
        group_by="ticker",
        threads=True,
        progress=False,
        auto_adjust=False,
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date", "symbol", "Open", "High", "Low", "Close", "Adj Close", "Volume"])

    # Single-ticker case: columns are simple, make it look like multi
    if not isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df["symbol"] = symbols[0]
        df = df.reset_index().rename(columns={"index": "Date"})
        return df[["Date", "symbol", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]

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
            for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
                if col not in sub.columns:
                    sub[col] = pd.NA
            tidy_frames.append(sub[["Date", "symbol", "Open", "High", "Low", "Close", "Adj Close", "Volume"]])
        except Exception:
            # Skip problematic symbol in this batch
            continue
    if not tidy_frames:
        return pd.DataFrame(columns=["Date", "symbol", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
    return pd.concat(tidy_frames, ignore_index=True)


def run_ingestion():
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=LOOKBACK_DAYS)

    # Build full symbol list
    try:
        symbols = get_all_symbols()
    except Exception as e:
        # Fallback to a tiny default if symbol discovery fails, but log error
        print(f"Symbol discovery failed: {e}")
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

    print(f"Fetching {len(symbols)} symbols from {start.date()} to {end.date()} (1d)")

    frames: List[pd.DataFrame] = []
    for batch in chunked(symbols, max(1, BATCH_SIZE)):
        try:
            batch_df = fetch_batch(batch, start, end)
            if not batch_df.empty:
                frames.append(batch_df)
        except Exception as e:
            print(f"Batch failed ({len(batch)} symbols): {e}")

    if not frames:
        print("No data fetched.")
        return

    all_df = pd.concat(frames, ignore_index=True)
    # Drop rows with all-NA prices to keep file clean
    price_cols = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    all_df = all_df.dropna(subset=price_cols, how="all")

    # Normalize columns to BigQuery schema
    all_df = all_df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    all_df["date"] = pd.to_datetime(all_df["date"], errors="coerce").dt.date
    # Enforce dtypes to match BQ schema best-effort
    for col in ["open", "high", "low", "close", "adj_close"]:
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
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("adj_close", "FLOAT"),
            bigquery.SchemaField("volume", "INT64"),
        ]
        job_config.time_partitioning = bigquery.TimePartitioning(field="date")
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

