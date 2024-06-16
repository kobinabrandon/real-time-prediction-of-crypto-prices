import hopsworks 
import pandas as pd
from hopsworks_config import config


def push_data_to_feature_store(feature_group_name: str, feature_group_version: int, data: dict) -> None:
    """
    Upload the data generated by the trade_to_ohlc service, and tunneled through the output Kafka topic to
    the specified feature group on Hopsworks.

    :param feature_group_name: the name of the feature group that we'll be sending features to
    :param feature_group_version: the version of the feature group
    :param data: the features to be uploaded
    :return: None
    """
    project = hopsworks.login(project=config.hopsworks_project_name, api_key_value=config.hopsworks_api_key)
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
    