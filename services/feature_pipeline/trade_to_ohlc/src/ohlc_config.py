import os


def set_vars(live_or_historical: str) -> dict:
    assert live_or_historical == "live" or "historical"
    return {
        "ohlc_window_seconds": os.environ["OHLC_WINDOW_SECONDS"],
        "kafka_broker_address": os.environ["KAFKA_BROKER_ADDRESS"],
        "input_kafka_topic": f"trade_{live_or_historical}",
        "output_kafka_topic": f"ohlc_{live_or_historical}",
        "kafka_consumer_group": f"trade_to_ohlc_{live_or_historical}"
    }
