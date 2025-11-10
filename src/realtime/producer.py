"""Market data polling producer.

Polls external data provider APIs (e.g., Alpha Vantage) and publishes
normalized OHLCV bars to Pub/Sub topics (prices-1m, prices-5m).
Simplified stub: generates synthetic bars for a small symbol set.
"""
import os
import json
import time
import random
from datetime import datetime
try:
    from google.cloud import pubsub_v1  # type: ignore
except Exception:  # pragma: no cover
    pubsub_v1 = None  # Allows static analysis without dependency installed

SYMBOLS = os.getenv("SYMBOLS", "AAPL,MSFT,GOOG").split(",")
PROJECT = os.getenv("GCP_PROJECT", "PROJECT")
TOPIC_1M = os.getenv("PRICES_TOPIC_1M", "prices-1m")
TOPIC_5M = os.getenv("PRICES_TOPIC_5M", "prices-5m")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))


def synthetic_bar(prev_close: float) -> dict:
    change = random.uniform(-0.5, 0.5)
    close = max(1.0, prev_close + change)
    high = close + random.uniform(0, 0.3)
    low = close - random.uniform(0, 0.3)
    open_ = prev_close
    volume = random.randint(1000, 5000)
    return {
        "open": round(open_, 2),
        "high": round(high, 2),
        "low": round(low, 2),
        "close": round(close, 2),
        "volume": volume,
        "ts": int(time.time()),
    }


def main():
    if pubsub_v1 is None:
        raise RuntimeError("google-cloud-pubsub not installed. Add to requirements.txt")
    publisher = pubsub_v1.PublisherClient()
    topic_1m_path = publisher.topic_path(PROJECT, TOPIC_1M)
    topic_5m_path = publisher.topic_path(PROJECT, TOPIC_5M)
    last_prices = {s: 100.0 + i * 10 for i, s in enumerate(SYMBOLS)}
    iteration = 0
    print("Starting synthetic market data producer...")
    while True:
        iteration += 1
        for symbol in SYMBOLS:
            bar = synthetic_bar(last_prices[symbol])
            last_prices[symbol] = bar["close"]
            event = {"symbol": symbol, "interval": "1m", "bar": bar}
            publisher.publish(
                topic_1m_path,
                json.dumps(event).encode("utf-8"),
                ordering_key=symbol,
            )
        # Every 5 iterations emit a 5m bar (aggregate last 5 synthetic bars)
        if iteration % 5 == 0:
            for symbol in SYMBOLS:
                # For demo just reuse the last bar
                bar = synthetic_bar(last_prices[symbol])
                last_prices[symbol] = bar["close"]
                event = {"symbol": symbol, "interval": "5m", "bar": bar}
                publisher.publish(
                    topic_5m_path,
                    json.dumps(event).encode("utf-8"),
                    ordering_key=symbol,
                )
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
