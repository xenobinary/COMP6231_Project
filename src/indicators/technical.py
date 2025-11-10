"""Technical indicator helpers used by the monitor.

Wraps 'ta' library when available and provides fallbacks for simple metrics.
"""
from __future__ import annotations

import pandas as pd

try:
    import ta  # type: ignore
except Exception:  # pragma: no cover
    ta = None


def compute_indicators(df: pd.DataFrame) -> dict:
    """Compute a minimal set of indicators from a DataFrame with 'close'.

    Returns keys: macd, rsi, bb_lower, bb_upper, z_score (when possible).
    """
    out: dict[str, float] = {}
    if df.empty or "close" not in df:
        return out

    if ta is not None:
        macd = ta.trend.MACD(df["close"])  # default parameters
        rsi = ta.momentum.RSIIndicator(df["close"], window=14)
        bb = ta.volatility.BollingerBands(df["close"], window=20, window_dev=2)
        out.update(
            macd=float(macd.macd().iloc[-1]),
            rsi=float(rsi.rsi().iloc[-1]),
            bb_lower=float(bb.bollinger_lband().iloc[-1]),
            bb_upper=float(bb.bollinger_hband().iloc[-1]),
        )

    # z-score fallback
    mean = df["close"].rolling(20).mean().iloc[-1]
    std = df["close"].rolling(20).std(ddof=0).iloc[-1]
    if pd.notna(mean) and pd.notna(std) and std != 0:
        out["z_score"] = float((df["close"].iloc[-1] - mean) / std)
    else:
        out["z_score"] = 0.0
    return out
