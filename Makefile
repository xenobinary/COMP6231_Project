.PHONY: batch stream backtest notebooks

batch:
	python -m src.batch.main --config configs/config.yaml

stream:
	python -m src.streaming.main --config configs/config.yaml

backtest:
	python -m src.backtest.main --config configs/config.yaml

notebooks:
	jupyter lab notebooks
