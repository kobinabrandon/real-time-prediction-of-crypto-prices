import hopsworks 
import pandas as pd 
from feature_store_config import config


def push_data_to_feature_store(
    feature_group_name: str,
    feature_group_version: int,
    data: dict
) -> None:

    project = hopsworks.login(
        project=config.hopsworks_project_name,
        api_key_value=config.hopsworks_api_key
    )

    feature_store = project.get_feature_store()

    ohlc_feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        description="OHLC Data from Kraken",
        primary_key=["product_id", "timestamp"],
        event_time="timestamp",
        online_enabled=True
    )

    data = pd.DataFrame([data])
    ohlc_feature_group.insert(features=data)
    