import os

from loguru import logger
from datetime import timedelta
from quixstreams import Application
from quixstreams.models import TimestampType

from ohlc_config import set_vars


def extract_columns_of_interest(dataframe: Application.dataframe) -> Application.dataframe:
    """
    Extract columns of interest from the dictionary which is the value which corresponds to
    the key called "value" key in the message.

    Args:
        dataframe: the streaming dataframe coming from the Kafka input topic
    Return:
         Application.dataframe: streaming dataframe containing the desired information only.
    """
    for metric in ["open", "high", "low", "close", "product_id"]:
        dataframe[metric] = dataframe["value"][metric]
    
    # Add a timestamp key, which is the value corresponding to the "end" key in the message
    dataframe["timestamp"] = dataframe["end"]

    # Dataframe containing the desired columns, since their names are now keys in the original dictionary
    dataframe = dataframe[["timestamp", "open", "high", "low", "close", "product_id"]]
    return dataframe


def init_ohlc_candle(trade: dict) -> dict:
    """
    Initialise OHLC candle with the first trade  
    """
    return {
        "open": trade["price"],
        "high": trade["price"],
        "low": trade["price"],
        "close": trade["price"],
        "product_id": trade["product_id"]
    }


def update_ohlc_candle(ohlc_candle: dict, trade: dict) -> dict:
    """
    Update candle with the new trade, and return the updated
    candle.

    Args:
        ohlc_candle (dict): current OHLC candle
        trade (dict): incoming trade

    Returns:
        dict: the updated OHLC values
    """
    return {
        "open": ohlc_candle["open"],
        "high": max(ohlc_candle["high"], trade["price"]),
        "low": min(ohlc_candle["low"], trade["price"]),
        "close": trade["price"],
        "product_id": trade["product_id"]
    }


def custom_timestamp_extractor(
        value: any,
        headers: list[tuple[str, bytes]] | None,
        timestamp: float,
        timestamp_type: TimestampType
) -> int:
    """
    A custom timestamp extractor to get the timestamp from the message payload
    instead of the timestamp that Kafka generates when the message is saved into the topic.
    The parameters after value must be specified lest the function no longer work.
    Args:
        value:
        headers:
        timestamp:
        timestamp_type:

    Returns:
        int: the timestamp in the message payload.
    """
    return value["timestamp_ms"]


def trade_to_ohlc(live_or_historical: str) -> None:
    config = set_vars(live_or_historical=live_or_historical)
    app = Application(
        broker_address=config["kafka_broker_address"],
        consumer_group=config["kafka_consumer_group"],
        auto_offset_reset="earliest"
    )
    
    input_topic = app.topic(
        name=config["input_kafka_topic"],
        value_serializer="json",
        timestamp_extractor=custom_timestamp_extractor
    )

    output_topic = app.topic(name=config["output_kafka_topic"], value_serializer="json")
    streaming_df = app.dataframe(topic=input_topic)

    # Apply transformations to the incoming data
    # TO DO 

    streaming_df = streaming_df.tumbling_window(
        duration_ms=timedelta(
            seconds=int(config["ohlc_window_seconds"])
        )
    )

    streaming_df = streaming_df.reduce(reducer=update_ohlc_candle, initializer=init_ohlc_candle).current()
    streaming_df = extract_columns_of_interest(dataframe=streaming_df)

    streaming_df = streaming_df.update(logger.info)
    streaming_df = streaming_df.to_topic(topic=output_topic)

    # Start the streaming application
    app.run(dataframe=streaming_df)


if __name__ == "__main__":
    trade_to_ohlc(
        live_or_historical=os.environ["LIVE"]
    )
