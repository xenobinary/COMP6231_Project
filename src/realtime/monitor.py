import os
import json
import time
from collections import deque, defaultdict

from google.cloud import firestore, pubsub_v1
import pandas as pd

try:
    import ta  # Technical Analysis library
except ImportError:
    ta = None

PRICE_WINDOW = int(os.getenv("PRICE_WINDOW", "120"))  # last 120 bars


class TradingMonitor:
    """
    Pub/Sub subscriber that consumes price bars (1m/5m), computes indicators,
    and publishes trading signals to the trading-signals topic.

    Env vars:
      GCP_PROJECT
      PRICES_SUBSCRIPTION_1M (e.g., prices-1m-sub)
      PRICES_SUBSCRIPTION_5M (e.g., prices-5m-sub)
      SIGNALS_TOPIC (e.g., trading-signals)
      FIRESTORE_COLLECTION (e.g., config)
    """

    def __init__(self):
        self.project = os.environ.get("GCP_PROJECT", "PROJECT")
        self.signals_topic_name = os.environ.get("SIGNALS_TOPIC", "trading-signals")
        self.firestore_collection = os.environ.get("FIRESTORE_COLLECTION", "config")

        self.db = firestore.Client()
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

        self.signal_topic = self.publisher.topic_path(self.project, self.signals_topic_name)
        self.subscription_1m = self.subscriber.subscription_path(
            self.project, os.environ.get("PRICES_SUBSCRIPTION_1M", "prices-1m-sub")
        )
        self.subscription_5m = self.subscriber.subscription_path(
            self.project, os.environ.get("PRICES_SUBSCRIPTION_5M", "prices-5m-sub")
        )

        self.watchlist = set(self.load_watchlist())
        self.price_windows = defaultdict(lambda: deque(maxlen=PRICE_WINDOW))

    def load_watchlist(self):
        doc = self.db.collection(self.firestore_collection).document("watchlist").get()
        data = doc.to_dict() or {}
        return data.get("symbols", [])

    def compute_indicators(self, symbol):
        window = list(self.price_windows[symbol])
        if len(window) < 25:
            return None
        df = pd.DataFrame(window)

        # If 'ta' is available, use it; otherwise compute simple z-score and bands
        indicators = {}
        if ta is not None:
            macd = ta.trend.MACD(df["close"])  # default fast=12, slow=26, signal=9
            rsi = ta.momentum.RSIIndicator(df["close"], window=14)
            bb = ta.volatility.BollingerBands(df["close"], window=20, window_dev=2)
            indicators.update(
                macd=float(macd.macd().iloc[-1]),
                rsi=float(rsi.rsi().iloc[-1]),
                bb_lower=float(bb.bollinger_lband().iloc[-1]),
                bb_upper=float(bb.bollinger_hband().iloc[-1]),
            )
        # z-score
        mean = df["close"].rolling(20).mean().iloc[-1]
        std = df["close"].rolling(20).std(ddof=0).iloc[-1]
        z = (df["close"].iloc[-1] - mean) / std if std and not pd.isna(std) else 0.0
        indicators["z_score"] = float(z)
        return indicators

    def generate_signal(self, symbol, price, indicators):
        if not indicators:
            return None
        # Simple MR rule: oversold bounce near lower band
        if (
            indicators.get("z_score", 0) < -2
            and indicators.get("rsi", 50) < 30
            and price <= indicators.get("bb_lower", price)
        ):
            return {
                "symbol": symbol,
                "action": "BUY",
                "timestamp": int(time.time()),
                "price": float(price),
                "strategy": "mr_v1",
            }
        return None

    def publish_signal(self, signal):
        data = json.dumps(signal).encode("utf-8")
        ordering_key = signal["symbol"]
        self.publisher.publish(self.signal_topic, data, ordering_key=ordering_key)

    def handle_message(self, message: pubsub_v1.subscriber.message.Message):
        try:
            event = json.loads(message.data.decode("utf-8"))
            symbol = event.get("symbol")
            if symbol not in self.watchlist:
                message.ack()
                return
            bar = event.get("bar")  # {open, high, low, close, volume, ts}
            if not bar:
                message.ack()
                return
            self.price_windows[symbol].append(bar)
            indicators = self.compute_indicators(symbol)
            if indicators:
                sig = self.generate_signal(symbol, bar["close"], indicators)
                if sig:
                    self.publish_signal(sig)
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()

    def run(self):
        flow = pubsub_v1.types.FlowControl(max_messages=100, max_bytes=10 * 1024 * 1024)
        f1 = self.subscriber.subscribe(self.subscription_1m, callback=self.handle_message, flow_control=flow)
        f5 = self.subscriber.subscribe(self.subscription_5m, callback=self.handle_message, flow_control=flow)
        print("Monitoring service started. Listening for price events...")
        try:
            f1.result()
            f5.result()
        except KeyboardInterrupt:
            f1.cancel()
            f5.cancel()


if __name__ == "__main__":
    TradingMonitor().run()
