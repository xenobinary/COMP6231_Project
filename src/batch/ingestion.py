"""Daily batch ingestion using yfinance.

Fetches daily OHLCV for a universe of symbols and writes CSV locally.
Stub for BigQuery load job included.
"""
import os
from datetime import datetime, timedelta
import pandas as pd
try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

UNIVERSE = os.getenv("UNIVERSE", "AAPL,MSFT,GOOG").split(",")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "365"))
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/daily_ohlcv.csv")


def fetch_symbol(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    if yf is None:
        raise RuntimeError("yfinance not installed. Add to requirements.txt")
    df = yf.download(symbol, start=start, end=end, progress=False, interval="1d")
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date", "symbol", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
    df.reset_index(inplace=True)
    df["symbol"] = symbol
    return df[["Date", "symbol", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]


def run_ingestion():
    end = datetime.utcnow()
    start = end - timedelta(days=LOOKBACK_DAYS)
    frames = []
    for sym in UNIVERSE:
        try:
            frames.append(fetch_symbol(sym, start, end))
        except Exception as e:
            print(f"Failed {sym}: {e}")
    if not frames:
        print("No data fetched.")
        return
    all_df = pd.concat(frames, ignore_index=True)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    all_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(all_df)} rows to {OUTPUT_PATH}")
    # TODO: Load into BigQuery (use google-cloud-bigquery load job)


if __name__ == "__main__":
    run_ingestion()
