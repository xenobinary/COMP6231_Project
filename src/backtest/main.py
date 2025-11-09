"""
Backtesting and transaction-cost modeling for mean reversion strategy.
"""

def main(config_path: str):
    # TODO: load config and run backtest
    pass

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backtest mean reversion strategy")
    parser.add_argument("--config", dest="config_path", default="configs/config.yaml")
    args = parser.parse_args()
    main(args.config_path)
