import os 
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# Load the .env file variables as environment variables so they can be accessed
load_dotenv(
    find_dotenv(filename=".env")
)

class Config(BaseSettings):
    product_id: str = "ETH/USD"
    kafka_broker_address: str = os.environ["KAFKA_BROKER_ADDRESS"]
    kafka_input_topic_name: str = "trade"
    kafka_output_topic_name: str = "ohlc"
    ohlc_windows_seconds: int = os.environ["OHLC_WINDOWS_SECONDS"]

config = Config()
