import os

from pydantic import BaseModel


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


def set_vars(live: bool) -> dict[str, list[str] | int | str]:
    live_or_historical = "live" if live else "historical"
    return {
        "last_n_days": os.environ["LAST_N_DAYS"],
        "ohlc_windows_seconds": 60,
        "product_ids": ["ETH/EUR", "ETH/USD", "BTC/USD"],
        "input_kafka_topic": f"trade_producer_{live_or_historical}",
        "output_kafka_topic": f"ohlc_producer_{live_or_historical}",
        "kafka_consumer_group": f"trade_producer_{live_or_historical}",
        "kafka_broker_address": os.environ["KAFKA_BROKER_ADDRESS"]
    }
