"""Backtest data producer using historical 5-min OHLCV data from BigQuery.

Reads symbols from Firestore watchlist and publishes each 5-min bar to a Pub/Sub
topic at a configurable interval (default 5s).
"""
import os
import json
import time

from google.cloud import firestore, bigquery  # type: ignore
from google.cloud import pubsub_v1  # type: ignore
from google.api_core.exceptions import NotFound  # type: ignore

PROJECT = os.getenv("GCP_PROJECT", "comp6231-project")
TOPIC_5M = os.getenv("PRICES_TOPIC_5M", "prices-5m")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))

WATCHLIST_COLLECTION = os.getenv("WATCHLIST_COLLECTION", "watchlists")
WATCHLIST_DOC_ID = os.getenv("WATCHLIST_DOC_ID", "adf_hurst_vr_screened")

BQ_DATASET = os.getenv("BQ_DATASET", "stock")
BQ_TABLE = os.getenv("BQ_TABLE", "ohlcv_5min")
# How many days back to pull for partition filter (must match table partition by DATE(timestamp))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC_5M)
    # Ensure Pub/Sub topic exists (auto-create if missing)
    try:
        publisher.get_topic(topic=topic_path)
    except NotFound:
        publisher.create_topic(name=topic_path)

    db = firestore.Client(project=PROJECT)
    doc = db.collection(WATCHLIST_COLLECTION).document(WATCHLIST_DOC_ID).get()
    symbols = (doc.to_dict() or {}).get("symbols", [])
    if not symbols:
        print(f"No symbols found in Firestore '{WATCHLIST_COLLECTION}/{WATCHLIST_DOC_ID}'.")
        return

    client = bigquery.Client(project=PROJECT)
    table_id = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    sql = f"""
SELECT symbol, timestamp, open, high, low, close, volume
FROM `{table_id}`
WHERE symbol IN UNNEST(@symbols)
  /* Partition filter required: only pull last N days */
  AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
ORDER BY timestamp
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("symbols", "STRING", symbols)
        ]
    )
    df = client.query(sql, job_config=job_config).result().to_dataframe()
    if df.empty:
        print("No historical data returned from BigQuery; exiting.")
        return

    print(
        f"Publishing bars slice-by-slice (per timestamp then symbols) "
        f"every {POLL_INTERVAL_SECONDS}s..."
    )
    # Ensure we emit in timestamp order, cycling through all symbols at each timestamp
    df.sort_values(["timestamp", "symbol"], inplace=True)
    for ts, group in df.groupby("timestamp"):
        for row in group.itertuples(index=False):
            event = {
                "symbol": row.symbol,
                "interval": "5m",
                "bar": {
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": int(row.volume),
                    "ts": int(row.timestamp.timestamp()),
                },
            }
            print(f"Publishing: {event}")
            publisher.publish(
                topic_path,
                json.dumps(event).encode("utf-8"),
            )
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
