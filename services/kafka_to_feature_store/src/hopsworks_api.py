import hopsworks 
import pandas as pd
from hopsworks_config import set_vars


def push_data_to_feature_store(features: list[dict], to_offline_store: bool) -> None:
    """
    Upload the data generated by the trade_to_ohlc service, and tunneled through the output Kafka topic to
    the specified feature group on Hopsworks

    :param features: the features to be uploaded
    :param to_offline_store: whether to send data to the offline feature store (or the online one).
    :return: None
    """
    live_or_historical = "historical" if to_offline_store else "live"
    config = set_vars(live_or_historical=live_or_historical)

    project = hopsworks.login(
        project=config["hopsworks_project_name"],
        api_key_value=config["hopsworks_api_key"]
    )

    feature_store = project.get_feature_store()
    ohlc_feature_group = feature_store.get_or_create_feature_group(
        name=config["feature_group_name"],
        version=config["feature_group_version"],
        description="OHLC Data from Kraken",
        primary_key=["product_id", "timestamp"],  # Provide these primary keys to avoid duplicate data
        event_time="timestamp",
        online_enabled=True
    )

    data_to_push = pd.DataFrame(features)

    ohlc_feature_group.insert(
        features=data_to_push,
        write_options={"start_offline_materialization": to_offline_store}
    )
