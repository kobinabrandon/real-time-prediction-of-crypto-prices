import os


def set_vars(live: bool) -> dict:
    live_or_historical = "live" if live else "historical"
    return {
        "ohlc_windows_seconds": os.environ["OHLC_WINDOWS_SECONDS"],
        "kafka_broker_address": os.environ["KAFKA_BROKER_ADDRESS"],
        "input_kafka_topic": f"trade_{live_or_historical}",
        "output_kafka_topic": f"ohlc_{live_or_historical}",
        "kafka_consumer_group": f"trade_to_ohlc_{live_or_historical}"
    }
