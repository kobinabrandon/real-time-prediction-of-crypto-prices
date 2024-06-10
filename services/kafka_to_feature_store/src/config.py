import os 
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings


load_dotenv(
    find_dotenv(str=".env")
)

class Config(BaseSettings):

    kafka_broker_address: str = "localhost: 19092"
    kafka_topic: str
    feature_group_name: str
    feature_group_version: int

    hopsworks_project_name: str
    hopsworks_api_key: str

config = Config()
