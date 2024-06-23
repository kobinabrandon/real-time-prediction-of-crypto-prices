import os
from pydantic import BaseModel


class Trade(BaseModel):
    product_id: str
    price: float
    volume: float
    timestamp_ms: int

    def to_dict(self) -> dict[str, float | int]:
        """
        Exists to make the contents of the Trade object JSON serializable by the Kafka topic
        :return: the dictionary of metrics for each trade
        """
        return {
            "product_id": self.product_id,
            "price": self.price,
            "volume": self.volume,
            "timestamp_ms": self.timestamp_ms*1000
        }


def set_vars(live_or_historical: str) -> dict[str, list[str] | int | str]:
    assert live_or_historical.lower() == "live" or "historical"
    return {
        "last_n_days": 30 if live_or_historical.lower() == "historical" else None,
        "ohlc_window_seconds": 60,
        "product_ids": ["ETH/USD", "BTC/USD", "ETH/EUR"],
        "input_kafka_topic": f"trade_producer_{live_or_historical}",
        "output_kafka_topic": f"ohlc_producer_{live_or_historical}",
        "kafka_consumer_group": f"trade_producer_{live_or_historical}",
        "kafka_broker_address": os.environ["KAFKA_BROKER_ADDRESS"]
    }
