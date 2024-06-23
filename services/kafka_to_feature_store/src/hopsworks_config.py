import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(
    find_dotenv(filename=".env", raise_error_if_not_found=True)
)


def set_vars(live_or_historical: str) -> dict:
    assert live_or_historical == "live" or "historical"
    return {
        "product_ids": ["ETH/EUR", "ETH/USD", "BTC/USD"],
        "input_kafka_topic": f"trade_{live_or_historical}",
        "output_kafka_topic": f"ohlc_{live_or_historical}",
        "kafka_consumer_group": f"ohlc_{live_or_historical}_consumer_group",
        "kafka_broker_address": os.environ["KAFKA_BROKER_ADDRESS"],
        "ohlc_windows_seconds": os.environ["OHLC_WINDOWS_SECONDS"],
        "hopsworks_api_key": os.environ["HOPSWORKS_API_KEY"],
        "hopsworks_project_name": os.environ["HOPSWORKS_PROJECT_NAME"],
        "feature_group_name":  os.environ["FEATURE_GROUP_NAME"],
        "feature_group_version": os.environ["FEATURE_GROUP_VERSION"],
        "save_every_n_seconds": os.environ["SAVE_EVERY_N_SECONDS"],
        "buffer_size": 1 if live_or_historical == "live" else 3000,
        "patience": 10
    }
