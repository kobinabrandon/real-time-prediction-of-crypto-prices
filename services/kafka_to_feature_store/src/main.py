import json
from datetime import datetime
from loguru import logger
from quixstreams import Application

from hopsworks_config import config
from hopsworks_api import push_data_to_feature_store


def get_current_time() -> int:
    return int(
        datetime.utcnow().timestamp()
    )


def kafka_to_feature_store(
    kafka_topic: str,
    kafka_broker_address: str,
    feature_group_name: str,
    feature_group_version: int,
    buffer_size: int | None,
    live: bool | None = config.live
) -> None:
    """
    Read OHLC data (that is, the features) from the Kafka topic, and send them to
    the feature group in the Hopsworks feature store.

    :param kafka_topic: the kafka topic that we are listening to for the features
    :param kafka_broker_address: the address of the kafka broker
    :param feature_group_name: the name of the feature group that the data is being sent to
    :param feature_group_version: the version of said feature group
    :param buffer_size: the number of trades that are gathered, and subsequently sent to Hopsworks
    :param live: whether we are uploading live or historical data
    :return: None
    """
    buffer = []
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store"
    )

    input_topic = app.topic(name=kafka_topic, value_deserializer="json")
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])
        while True:
            msg = consumer.poll(timeout=1)  # Wait for a second for each message

            if msg is None:
                logger.warning(f"No new message have come through the topic {kafka_topic}")

                if get_current_time() - last_time_saved_to_store > config.patience:
                    logger.warning("Exceeded the timer limit. Force pushing data to the feature store")
                    push_data_to_feature_store(
                        feature_group_name=feature_group_name,
                        feature_group_version=feature_group_version,
                        data=buffer,
                        to_offline_store=False if live else True
                    )
                    buffer = []
                else:
                    logger.info("Did not exceed the timer limit. Skipping ")
                    continue

            elif msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            else:
                value = msg.value()
                ohlc = json.loads(value.decode("utf-8"))

                if len(buffer) >= buffer_size:
                    push_data_to_feature_store(
                        feature_group_name=feature_group_name,
                        feature_group_version=feature_group_version,
                        data=ohlc,
                        to_offline_store=False if live else True
                    )
                    buffer = []

                last_time_saved_to_store = get_current_time()
            consumer.store_offsets(message=msg)


if __name__ == "__main__":
    kafka_to_feature_store(
        kafka_topic=config.output_kafka_topic,
        kafka_broker_address=config.kafka_broker_address,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        buffer_size=config.buffer_size
    )
