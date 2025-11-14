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
        self.project = os.environ.get("GCP_PROJECT", "comp6231-project")
        self.signals_topic_name = os.environ.get("SIGNALS_TOPIC", "prices-5m")
        self.firestore_collection = os.environ.get("FIRESTORE_COLLECTION", "watchlists")

        self.db = firestore.Client()
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

        self.signal_topic = self.publisher.topic_path(self.project, self.signals_topic_name)
        # self.subscription_1m = self.subscriber.subscription_path(
        #     self.project, os.environ.get("PRICES_SUBSCRIPTION_1M", "prices-1m-sub")
        # )
        self.subscription_5m = self.subscriber.subscription_path(
            self.project, os.environ.get("PRICES_SUBSCRIPTION_5M", "prices-5m-sub")
        )

        self.watchlist = set(self.load_watchlist())
        self.price_windows = defaultdict(lambda: deque(maxlen=PRICE_WINDOW))

    def load_watchlist(self):
        doc = self.db.collection(self.firestore_collection).document("adf_hurst_vr_screened").get()
        data = doc.to_dict() or {}
        return data.get("symbols", [])

    def compute_indicators(self, symbol):
        window = list(self.price_windows[symbol])
        # need at least two bars plus indicator windows
        if len(window) < 25:
            print(f"Not enough data to compute indicators for {symbol} (have {len(window)} bars)")
            return None
        df = pd.DataFrame(window)

        if ta is None:
            raise RuntimeError("ta (Technical Analysis) library is required for indicators")

        # Compute ADX for trend strength
        adx_ind = ta.trend.ADXIndicator(df["high"], df["low"], df["close"], window=14)
        adx_val = float(adx_ind.adx().iloc[-1])
        print(f"Computed ADX for {symbol}: {adx_val}")

        # Compute Bollinger Bands
        bb = ta.volatility.BollingerBands(df["close"], window=20, window_dev=2)
        lower_band = bb.bollinger_lband()
        curr_bb_lower = float(lower_band.iloc[-1])
        prev_bb_lower = float(lower_band.iloc[-2])
        print(f"Computed Bollinger Bands for {symbol}: lower_band={curr_bb_lower}")

        # Detect crossing of close through lower band (prev > band & curr <= band)
        prev_close = float(df["close"].iloc[-2])
        curr_close = float(df["close"].iloc[-1])
        cross_lower = prev_close > prev_bb_lower and curr_close <= curr_bb_lower

        return {
            "adx": adx_val,
            "bb_lower": curr_bb_lower,
            "cross_lower": cross_lower,
        }

    def generate_signal(self, symbol, price, indicators):
        """
        Generate a buy signal when ADX indicates a strong trend and price just crossed
        below the lower Bollinger band.
        """
        if not indicators:
            return None

        # ADX >= 50 and price crossing lower Bollinger band
        if indicators.get("adx", 0) >= 50 and indicators.get("cross_lower", False):
            return {
                "symbol": symbol,
                "action": "BUY",
                "timestamp": int(time.time()),
                "price": float(price),
                "strategy": "adx_bb_cross",
            }
        return None

    def publish_signal(self, signal):
        data = json.dumps(signal).encode("utf-8")
        self.publisher.publish(self.signal_topic, data)

    def handle_message(self, message: pubsub_v1.subscriber.message.Message):
        try:
            event = json.loads(message.data.decode("utf-8"))
            symbol = event.get("symbol")
            if symbol not in self.watchlist:
                print(f"Ignoring symbol {symbol} not in watchlist")
                message.ack()
                return
            bar = event.get("bar")  # {open, high, low, close, volume, ts}
            if not bar:
                print(f"Invalid bar data in message")
                message.ack()
                return
            self.price_windows[symbol].append(bar)
            print(f"Received {symbol} bar at {bar['ts']}: close={bar['close']}")
            indicators = self.compute_indicators(symbol)
            if indicators:
                sig = self.generate_signal(symbol, bar["close"], indicators)
                # Log ADX, lower BB and whether it triggers a BUY
                ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(bar["ts"]))
                print(
                    f"[{ts_iso}] {symbol} 5m bar: ADX={indicators['adx']:.2f}, "
                    f"bb_lower={indicators['bb_lower']:.2f}, cross_lower={indicators['cross_lower']} "
                    f"-> {'BUY' if sig else 'no signal'}"
                )
                if sig:
                    print(f"Publishing signal: {sig}")
                    self.publish_signal(sig)
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()

    def run(self):
        flow = pubsub_v1.types.FlowControl(max_messages=100, max_bytes=10 * 1024 * 1024)
        # f1 = self.subscriber.subscribe(self.subscription_1m, callback=self.handle_message, flow_control=flow)
        f5 = self.subscriber.subscribe(self.subscription_5m, callback=self.handle_message, flow_control=flow)
        print("Monitoring service started. Listening for price events...")
        try:
            # f1.result()
            f5.result()
        except KeyboardInterrupt:
            # f1.cancel()
            f5.cancel()


if __name__ == "__main__":
    TradingMonitor().run()
