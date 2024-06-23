run-live:
	LIVE=live \
	LAST_N_DAYS=30 \
	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

run-historical:
	LIVE=historical\
  	LAST_N_DAYS=30 \
  	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

build:
	docker build -t trade_producer .

run:
	docker run --network=redpanda_network trade_producer
