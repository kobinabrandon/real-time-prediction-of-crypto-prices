run-live:
	LIVE=True \
	poetry run python src/main.py

run-dev-historical:
	LIVE=False \
	poetry run python src/main.py

build:
	docker build -t trade_to_ohlc .

run:
	docker run --network=redpanda_network trade_to_ohlc