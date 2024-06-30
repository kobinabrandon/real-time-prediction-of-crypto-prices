run-live:
	LIVE=live \
	KAFKA_BROKER_ADDRESS=localhost:19092 \
	poetry run python src/main.py

run-historical:
	LIVE=historical \
	KAFKA_BROKER_ADDRESS=localhost:19092 \
	SAVE_EVERY_N_SECONDS=600 \
	poetry run python src/main.py

build:
	docker build -t kafka_to_feature_store .

run: build
	docker run \
	--network=redpanda_network kafka_to_feature_store --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	--env-file .env kafka_to_feature_store