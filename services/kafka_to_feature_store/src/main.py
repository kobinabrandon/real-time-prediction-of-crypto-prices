import json
from loguru import logger
from quixstreams import Application

from hopsworks_config import config
from hopsworks_api import push_data_to_feature_store


def kafka_to_feature_store(
    kafka_topic: str,
    kafka_broker_address: str,
    feature_group_name: str,
    feature_group_version: int,
    buffer_size: int
) -> None:
    """
    Read OHLC data (that is, the features) from the Kafka topic, and send them to
    the feature group in the Hopsworks feature store.

    :param kafka_topic: the kafka topic that we are listening to for the features
    :param kafka_broker_address: the address of the kafka broker
    :param feature_group_name: the name of the feature group that the data is being sent to
    :param feature_group_version: the version of said feature group
    :param buffer_size: the number of trades that are gathered, and subsequently sent to Hopsworks
    :return: None
    """
    def _push_features_in_batches(loaded_msg: dict, buffer_size: int):
        buffer = []
        buffer.append(loaded_msg)

        if len(buffer) >= buffer_size:
            push_data_to_feature_store(
                feature_group_name=feature_group_name,
                feature_group_version=feature_group_version,
                data=ohlc
            )
            buffer = []

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store"
    )

    input_topic = app.topic(name=kafka_topic, value_deserializer="json")
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])
        while True:
            msg = consumer.poll(timeout=0.5)  # Wait for half a second for each message
            if msg is None:
                continue
            elif msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            else:
                value = msg.value()
                ohlc = json.loads(value.decode("utf-8"))
                _push_features_in_batches(loaded_msg=ohlc, buffer_size=buffer_size)

            consumer.store_offsets(message=msg)


if __name__ == "__main__":
    kafka_to_feature_store(
        kafka_topic=config.output_kafka_topic,
        kafka_broker_address=config.kafka_broker_address,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        buffer_size=config.buffer_size
    )
