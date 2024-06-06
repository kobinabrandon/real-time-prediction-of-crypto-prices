from time import sleep
from loguru import logger
from quixstreams import Application

from kraken_api import KrakenWebsocketTradeAPI
 

def produce_trades(kafka_broker_address: str, kafka_topic_name: str) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_topic_name (str): The name of the Kafka topic.

    Returns:
        None
    """
    app = Application(broker_address=kafka_broker_address)
    topic = app.topic(name=kafka_topic_name, value_serializer="json")

    # Create a Producer instance
    with app.get_producer() as producer:
        while True:
            kraken_api = KrakenWebsocketTradeAPI(product_id="BTC/USD")
            trades : list[dict] = kraken_api.get_trades()

            for trade in trades:
                # Serialize event using the defined topic 
                message = topic.serialize(key=trade["product_id"], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(
                    topic=topic.name, 
                    value=message.value, 
                    key=message.key
                )

                logger.success('Message sent')
            sleep(1)


if __name__ == '__main__':
    produce_trades(
        kafka_broker_address="localhost:19092",
        kafka_topic_name="trade"
    )
    