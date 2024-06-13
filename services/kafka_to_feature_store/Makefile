build:
	docker build -t kafka_to_feature_store .

run: build
	docker run --network=redpanda_network kafka_to_feature_store --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	--env-file .env kafka_to_feature_store