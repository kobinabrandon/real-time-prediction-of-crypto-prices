import hopsworks
import pandas as pd

from src.dashboard_config import config


def get_features_from_store(feature_group_name: str, feature_group_version: int) -> pd.DataFrame:
    """
    Fetch the features from the store and return them as a dataframe.

    Args:
        feature_group_name:
        feature_group_version:

    Returns:
        pd.DataFrame: 
    """
    project = hopsworks.login(
        project=config.hopsworks_project_name,
        api_key_value=config.hopsworks_api_key
    )

    feature_store = project.get_feature_store()
    feature_group = feature_store.get_feature_group(
        name=feature_group_name,
        version=feature_group_version
    )

    feature_view = feature_store.get_or_create_feature_view(
        name=config.feature_view_name,
        version=config.feature_view_version,
        query=feature_group.select_all()
    )

    features: pd.DataFrame = feature_view.get_batch_data()
    return features


if __name__ == "__main__":
    data = get_features_from_store(
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_name
    )
    print(data.head())