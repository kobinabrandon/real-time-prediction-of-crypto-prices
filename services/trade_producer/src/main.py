import os
import time
from loguru import logger
from quixstreams import Application

from producer_config import set_vars, Trade
from kraken_api import KrakenWebsocketAPI, KrakenRestAPI


def produce_trades(live_or_historical: str) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    :param live_or_historical: whether we want live or historical data.
    :return: None
    """
    config = set_vars(live_or_historical=live_or_historical)
    app = Application(broker_address=config["kafka_broker_address"])
    topic = app.topic(name=config["input_kafka_topic"], value_serializer="json")

    logger.info("Creating the producer")
    with app.get_producer() as producer:
        if live_or_historical.lower() == "live":
            kraken_api = KrakenWebsocketAPI(product_ids=config["product_ids"])
        elif live_or_historical.lower() == "historical":
            to_ms = int(time.time() * 1000)  # Convert current time in seconds into milliseconds
            from_ms = to_ms - config["last_n_days"] * 24 * 60 * 60 * 1000
            kraken_api = KrakenRestAPI(product_ids=config["product_ids"], from_ms=from_ms, to_ms=to_ms)
        while True:
            trade_data: list[Trade] = kraken_api.get_trades()
            for trade in trade_data:
                message = topic.serialize(
                    key=trade.product_id,
                    value=trade.to_dict(),
                    timestamp_ms=trade.timestamp_ms
                )

                # Produce into Kafka topic
                producer.produce(
                    topic=topic.name,
                    value=message.value,
                    key=message.key,
                    timestamp=int(message.timestamp)
                )

                logger.info(message.value)

                if kraken_api.is_finished:
                    logger.success("Done fetching historical data")
                    break

            time.sleep(1)


if __name__ == "__main__":
    produce_trades(
        live_or_historical=os.environ["LIVE"]
    )
