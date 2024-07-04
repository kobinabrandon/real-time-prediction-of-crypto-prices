run-live:
	LIVE=live \
	OHLC_WINDOW_SECONDS=60 \
	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

run-historical:
	LIVE=historical \
	OHLC_WINDOW_SECONDS=60 \
	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

build:
	docker build -t trade_to_ohlc .

run:
	docker run \
	--network=redpanda_network \
	--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	--env OHLC_WINDOW_SECONDS=60 \
	trade_to_ohlc