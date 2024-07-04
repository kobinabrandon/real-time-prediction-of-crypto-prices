run-live:
	source ../../../credentials.sh && \
	LIVE=live \
	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

run-historical:
	source ../../../credentials.sh && \
	LIVE=historical \
  	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

build:
	docker build -t trade_producer .

run:
	docker run --network=redpanda_network trade_producer
