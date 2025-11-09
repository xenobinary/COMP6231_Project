"""
Real-time signal generation for mean reversion trading.
"""

def main(config_path: str):
    # TODO: load config and start streaming pipeline
    pass

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Streaming signal generation")
    parser.add_argument("--config", dest="config_path", default="configs/config.yaml")
    args = parser.parse_args()
    main(args.config_path)
