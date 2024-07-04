import time 
import hopsworks

import pandas as pd


class FeatureReader:

    def __init__(
        self, 
        ohlc_window_seconds: int,
        feature_view_name: str,
        feature_view_version: str,
        feature_group_name: str,
        feature_group_version: int
    ) -> None:

        self.ohlc_window_seconds = ohlc_window_seconds
        self.feature_view_name = feature_view_name
        self.feature_view_version = feature_view_version
        self.feature_group_name = feature_group_name
        self.feature_view_version = feature_group_version

        self._feature_store = self._connect_to_store()

    
    @staticmethod
    def _connect_to_store():
        project = hopsworks.login(project=project, api_key_value=api_key)
        return project.get_feature_store()
    

    def _get_feature_view(project: str, api_key: str, feature_group_name: str, feature_group_version: str) -> FeatureView:
        """

        Args:
            project: the name of the Hopsworks project where the features are stored
            api_key: Hopswoorks API key
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


    def _get_primary_keys_for_online_store(product_id: str, last_n_minutes: int) -> list[dict]:
        """

        Args:
            product_id (str): _description_
            last_n_minutes (int): _description_

        Returns:
            list[dict]: 
        """
        current_utc = int(time.time()*1000)
        current_utc_rounded = current_utc - (current_utc % 60000)  # Rounding to the nearest minute
        timestamps_last_n_minutes = [current_utc_rounded - i*60000 for i in range(last_n_minutes)]

        return [
            {"product_id": config.product_id, "timestamp": timestamp} for timestamp in timestamps_last_n_minutes
        ]


    def read_from_online_store(self, product_id: str, last_n_minutes: int | None) -> pd.DataFrame:
        
        primary_keys = self._get_primary_keys_for_online_store(product_id=product_id, last_n_minutes=last_n_minutes)
        features: pd.DataFrame = feature_view_object.get_feature_vectors(entry=primary_keys, return_type="pandas")
        return features.sort_values(by="timestamp", ascending=True)


    def read_from_offline_store(self) -> pd.DataFrame:
        features: pd.DataFrame = feature_view_object.get_batch_data()
        return features.sort_values(by="timestamp", ascending=True)