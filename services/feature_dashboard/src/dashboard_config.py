from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings


load_dotenv(
    find_dotenv(filename=".env", raise_error_if_not_found=True)
)


class Config(BaseSettings):

    feature_group_name: str
    feature_group_version: int

    feature_view_name: str
    feature_view_version: int

    hopsworks_api_key: str
    hopsworks_project_name: str


config = Config()
