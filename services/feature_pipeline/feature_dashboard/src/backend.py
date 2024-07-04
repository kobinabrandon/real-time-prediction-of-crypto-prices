import time

import hopsworks
import pandas as pd

from loguru import logger
from hsfs.feature_view import FeatureView

from dashboard_config import config
from tooling.src.feature_reader import FeatureReader


def get_primary_keys(last_n_minutes: int) -> list[dict]:
 
    current_utc = int(time.time()*1000)
    current_utc_rounded = current_utc - (current_utc % 60000)  # Rounding to the nearest minute
    timestamps_last_n_minutes = [current_utc_rounded - i*60000 for i in range(last_n_minutes)]
 
    primary_keys = [
        {"product_id": config.product_id, "timestamp": timestamp} for timestamp in timestamps_last_n_minutes
    ]

    return primary_keys


def get_feature_view(project: str, api_key: str, feature_group_name: str, feature_group_version: str) -> FeatureView:
    """

    Args:
        project:
        api_key:
        feature_group_name: the name of the Hopsworks feature group
        feature_group_version: the version of that Hopsworks feature group

    Returns:

    """
    project = hopsworks.login(project=project, api_key_value=api_key)
    feature_store = project.get_feature_store()
    feature_group = feature_store.get_feature_group(name=feature_group_name, version=feature_group_version)

    return feature_store.get_or_create_feature_view(
        name=config.feature_view_name,
        version=config.feature_view_version,
        query=feature_group.select_all()
    )


def get_features_from_store(live_or_historical: str, feature_view_object: FeatureView) -> pd.DataFrame:
    """
    Fetch the features from the store and return them as a dataframe.

    Args:
        live_or_historical: a string that determines whether we will fetch data from the online or offline feature
                            stores. Specifying "live" will cause data to be fetched from the online store, and
                            "historical" will result in data being fetched from the offline store.

        feature_view_object: the feature view from which we are fetching the features

    Returns:
        pd.DataFrame: features saved in the feature store.
    """
    assert live_or_historical.lower() == "live" or "historical"
    logger.success("Got feature view")

    if live_or_historical.lower() == "live":
        features = feature_view_object.get_feature_vectors(
            entry=get_primary_keys(last_n_minutes=config.last_n_minutes),
            return_type="pandas"
        )

    elif live_or_historical.lower() == "historical":
        features: pd.DataFrame = feature_view_object.get_batch_data()
    else:
        raise Exception('We are only able to take "live" or "historical" as options')

    return features.sort_values(by="timestamp", ascending=True)


if __name__ == "__main__":

    feature_view = get_feature_view(
        project=config.hopsworks_project_name,
        api_key=config.hopsworks_api_key,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version
    )

    data = get_features_from_store(live_or_historical=config.live, feature_view_object=feature_view)
    print(data.head())
