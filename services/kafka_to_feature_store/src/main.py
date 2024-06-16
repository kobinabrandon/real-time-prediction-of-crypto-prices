import json
from loguru import logger
from quixstreams import Application

from hopsworks_config import config
from hopsworks_api import push_data_to_feature_store


def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_group_version: int
) -> None:
    """
    Read OHLC data from the Kafka topic, and write it to 
    the feature group in the Hopsworks feature store.
    """

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
                # Parse the message from Kafka
                ohlc = json.loads(value.decode("utf-8"))

                push_data_to_feature_store(
                    feature_group_name=feature_group_name,
                    feature_group_version=feature_group_version,
                    data=ohlc
                )

            consumer.store_offsets(message=msg)


if __name__ == "__main__":
    kafka_to_feature_store(
        kafka_topic=config.output_kafka_topic,
        kafka_broker_address=config.kafka_broker_address,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version
    )
