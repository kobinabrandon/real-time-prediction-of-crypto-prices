run-dev:
	KAFKA_BROKER_ADDRESS="localhost:19092" \
	source ./live.env && poetry run python src/main.py

run-dev-historical:
	KAFKA_BROKER_ADDRESS="localhost:19092" \
	source ./historical.env && poetry run python src/main.py


build:
	docker build -t trade_producer .

run:
	docker run --network=redpanda_network trade_producer