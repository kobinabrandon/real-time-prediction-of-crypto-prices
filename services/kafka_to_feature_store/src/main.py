import json
import os
from datetime import datetime
from loguru import logger
from quixstreams import Application

from hopsworks_config import set_vars
from hopsworks_api import push_data_to_feature_store


def get_current_time() -> int:
    return int(
        datetime.utcnow().timestamp()
    )


def kafka_to_feature_store(live_or_historical: str) -> None:
    """
    Read OHLC data (that is, the features) from the Kafka topic, and send them to
    the feature group in the Hopsworks feature store.

    :param live_or_historical: whether we are uploading live or historical data
    :return: None
    """
    buffer = []
    config = set_vars(live_or_historical=live_or_historical.lower())
    kafka_topic = config["output_kafka_topic"]  # the kafka topic that we are listening to for the features
    buffer_size = config["buffer_size"]  # the number of trades that are gathered, and subsequently sent to Hopsworks

    app = Application(
        broker_address=config["kafka_broker_address"],
        consumer_group="kafka_to_feature_store"
    )

    input_topic = app.topic(name=kafka_topic, value_deserializer="json")
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])

        while True:
            msg = consumer.poll(timeout=1)  # Wait for a second for each message

            if msg is None:
                logger.warning(f"No new messages have come through the topic {kafka_topic}")
                if "last_time_saved_to_store" not in locals():  # if there's no such variable yet
                    last_time_saved_to_store = get_current_time()
                elif get_current_time() - last_time_saved_to_store > config["patience"]:
                    logger.warning("Exceeded the timer limit. Force pushing data to the feature store")
                    push_data_to_feature_store(
                        features=buffer,
                        to_offline_store=False if live_or_historical else True
                    )
                    buffer = []
                else:
                    logger.info("Did not exceed the timer limit. Continuing to poll messages from the input topic")
                    continue

            elif msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            else:  # if there is a message and there are no errors
                value = msg.value(payload=msg)
                ohlc = json.loads(value.decode("utf-8"))
                buffer.append(ohlc)

                if len(buffer) >= buffer_size:
                    push_data_to_feature_store(
                        features=buffer,
                        to_offline_store=False if live_or_historical.lower() == "live" else True
                    )
                    buffer = []  # Empty the buffer to prepare for the next message

                last_time_saved_to_store = get_current_time()

#            consumer.store_offsets(message=msg)


if __name__ == "__main__":
    kafka_to_feature_store(
        live_or_historical=os.environ["LIVE"]
    )
