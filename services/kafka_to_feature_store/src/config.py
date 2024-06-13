import os 
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings


load_dotenv(
    find_dotenv(filename=".env")
)

class Config(BaseSettings):

    kafka_broker_address: str = os.environ["KAFKA_BROKER_ADDRESS"]
    kafka_topic: str = os.environ["KAFKA_TOPIC"]
    feature_group_name: str = os.environ["FEATURE_GROUP_NAME"]
    feature_group_version: int = os.environ["FEATURE_GROUP_VERSION"]

    hopsworks_project_name: str = os.environ["HOPSWORKS_PROJECT_NAME"]
    hopsworks_api_key: str = os.environ["HOPSWORKS_API_KEY"]

config = Config()
