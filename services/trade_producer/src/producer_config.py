import os
from dotenv import load_dotenv, find_dotenv

from pydantic import BaseModel
from pydantic_settings import BaseSettings

# Load the .env file variables as environment variables to enable access
load_dotenv(
    find_dotenv(filename="../.env")
)


class Trade(BaseModel):
    product_id: str
    price: float
    volume: float
    timestamp_ms: int

    def to_dict(self) -> dict[str, float | int]:
        """
        Exists to make the contents of the Trade object JSON serializable
        by the Kafka topic
        :return: the dictionary of metrics for each trade
        """
        return {
            "product_id": self.product_id,
            "price": self.price,
            "volume": self.volume,
            "timestamp_ms": self.timestamp_ms
        }


class Config(BaseSettings):
    product_ids: list[str] = ["ETH/USD", "ETH/EUR", "BTC/USD"]
    live: bool = os.environ["LIVE"]
    last_n_days: int | None = 7

    input_kafka_topic: str = os.environ["INPUT_KAFKA_TOPIC"]
    output_kafka_topic: str = os.environ["OUTPUT_KAFKA_TOPIC"]
    kafka_broker_address: str = os.environ["KAFKA_BROKER_ADDRESS"]
    ohlc_windows_seconds: int = os.environ["OHLC_WINDOWS_SECONDS"]


config = Config()
