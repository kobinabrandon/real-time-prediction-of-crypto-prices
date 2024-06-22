run-live:
	LIVE=True \
	OHLC_WINDOWS_SECONDS=60 \
	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

run-historical:
	LIVE=False \
	OHLC_WINDOWS_SECONDS=60 \
	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

build:
	docker build -t trade_to_ohlc .

run:
	docker run --network=redpanda_network trade_to_ohlc