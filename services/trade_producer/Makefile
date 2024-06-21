run-live:
	LIVE=True \
	poetry run python src/main.py

run-historical:
	LIVE=False \
	poetry run python src/main.py

build:
	docker build -t trade_producer .

run:
	docker run --network=redpanda_network trade_producer


