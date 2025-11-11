#!/usr/bin/env python3
import os
from io import StringIO

import pandas as pd

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

DEST = os.path.join(os.path.dirname(__file__), "..", "data", "metadata", "sp500_symbols.csv")
DEST = os.path.abspath(DEST)


def ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def fetch_sp500_symbols() -> list[str]:
    # Try Wikipedia with user-agent header to avoid 403s
    if requests is not None:
        try:
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
            if syms:
                return syms
        except Exception:
            pass

    # Try DataHub CSV mirror
    if requests is not None:
        try:
            datahub = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
            resp = requests.get(datahub, timeout=20)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text))
            col = next((c for c in df.columns if str(c).lower() in {"symbol", "ticker"}), None)
            if col is None:
                raise RuntimeError("No symbol/ticker column in DataHub CSV")
            syms = (
                df[col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.upper()
                .str.replace(".", "-", regex=False)
                .tolist()
            )
            if syms:
                return syms
        except Exception:
            pass

    # Minimal fallback
    return ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]


def main() -> None:
    syms = fetch_sp500_symbols()
    ensure_dir(DEST)
    pd.DataFrame({"symbol": syms}).to_csv(DEST, index=False)
    print(f"Wrote {len(syms)} symbols to {DEST}")


if __name__ == "__main__":
    main()
