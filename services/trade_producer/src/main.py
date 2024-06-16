import time
from loguru import logger
from quixstreams import Application

from producer_config import config
from kraken_api import KrakenWebsocketAPI, KrakenRestAPIMultiplePairs


def produce_trades(
        kafka_broker_address: str,
        kafka_topic_name: str,
        last_n_days: int | None,
        live: bool
) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    :param kafka_broker_address: The address of the Kafka broker.
    :param kafka_topic_name: The name of the Kafka topic.
    :param live: whether we want live or historical data.
    :param last_n_days: the number of days of historical data that we will get if live is set to False.
    :return: None
    """
    app = Application(broker_address=kafka_broker_address)
    topic = app.topic(name=kafka_topic_name, value_serializer="json")

    logger.info("Creating the producer")
    with app.get_producer() as producer:
        if live:
            kraken_api = KrakenWebsocketAPI(product_id=config.product_ids[0])
        else:
            to_ms = int(time.time() * 1000)  # Convert current time in seconds into milliseconds
            from_ms = to_ms - last_n_days * 24 * 60 * 60 * 1000
            kraken_api = KrakenRestAPIMultiplePairs(product_ids=config.product_ids, from_ms=from_ms, to_ms=to_ms)

        while True:
            trade_data = kraken_api.get_trades()
            for trade in trade_data:
                message = topic.serialize(key=trade["product_id"], value=trade)
                producer.produce(topic=topic.name, value=message.value, key=message.key)  # Produce into Kafka topic
                logger.info(message.value)

            if kraken_api.is_finished:
                logger.success("Done fetching historical data")
                break


if __name__ == "__main__":
    produce_trades(
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic_name=config.input_kafka_topic,
        last_n_days=config.last_n_days,
        live=False
    )
