from pydantic_settings import BaseSettings


class Config(BaseSettings):
    kafka_broker_address: str | None = None
    input_kafka_topic: str
    output_kafka_topic: str
    kafka_consumer_group: str
    ohlc_windows_seconds: int


config = Config()
