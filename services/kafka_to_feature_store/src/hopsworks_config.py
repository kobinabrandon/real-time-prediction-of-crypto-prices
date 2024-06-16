import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# Load the .env file variables as environment variables to enable access
load_dotenv(
    find_dotenv(filename="../.env")
)


class Config(BaseSettings):
    product_id: str = "BTC/USD"

    input_kafka_topic: str = os.environ["INPUT_KAFKA_TOPIC"]
    output_kafka_topic: str = os.environ["OUTPUT_KAFKA_TOPIC"]
    kafka_broker_address: str = os.environ["KAFKA_BROKER_ADDRESS"]
    ohlc_windows_seconds: int = os.environ["OHLC_WINDOWS_SECONDS"]

    hopsworks_api_key: str = os.environ["HOPSWORKS_API_KEY"]
    hopsworks_project_name: str = os.environ["HOPSWORKS_PROJECT_NAME"]
    feature_group_name: str = os.environ["FEATURE_GROUP_NAME"]
    feature_group_version: int = os.environ["FEATURE_GROUP_VERSION"]


config = Config()
