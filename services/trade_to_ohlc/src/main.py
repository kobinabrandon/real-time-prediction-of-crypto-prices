from datetime import timedelta
from quixstreams import Application

from loguru import logger 

from config import config


def extract_columns_of_interest(dataframe: Application.dataframe) -> Application.dataframe:
    # Extracts columns of interest from the dictionary which is the value which 
    # corresponds to the key called "value" key in the message
    metrics = ["open", "high", "low", "close", "product_id"] 
    for metric in metrics:
        dataframe[metric] = dataframe["value"][metric]
    
    # Add a timestamp key, which is the value corresponding to the "end" key in the message
    dataframe["timestamp"] = dataframe["end"]

    # Return the dataframe containing only those columns, now that they're 
    # keys in the original dictionary
    dataframe = dataframe[["timestamp", "open", "high", "low", "close", "product_id"]]
    return dataframe


def init_ohlc_candle(trade: dict) -> dict:
    """
    Initialise OHLC candle with the first trade
    """
    return {
        "timestamp": trade["timestamp"],
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
        dict: _description_
    """
    return {
        "open": ohlc_candle["open"],
        "high": max(ohlc_candle["high"], trade["price"]),
        "low": min(ohlc_candle["low"], trade["price"]),
        "close": trade["price"],
        "product_id": trade["product_id"]
    }


def trade_to_ohlc(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_output_topic: str,
    ohlc_window_seconds: int
) -> None:

    app = Application(
        broker_address=kafka_broker_address, 
        consumer_group="trade_to_ohlc",
        auto_offset_reset="earliest"
    )
    
    input_topic = app.topic(name=kafka_input_topic, value_serializer="json")
    output_topic = app.topic(name=kafka_output_topic, value_serializer="json")

    streaming_df = app.dataframe(topic=input_topic)

    # Apply transformations to the incoming data
    # TO DO 

    streaming_df = streaming_df.tumbling_window(
        duration_ms=timedelta(seconds=ohlc_window_seconds)
    )

    streaming_df = streaming_df.reduce(reducer=update_ohlc_candle, initializer=init_ohlc_candle).final()

    streaming_df = extract_columns_of_interest(dataframe=streaming_df)

    streaming_df = streaming_df.update(logger.info)
    streaming_df = streaming_df.to_topic(topic=output_topic)

    # Start the streaming application
    app.run(dataframe=streaming_df)


if __name__ == "__main__":
    trade_to_ohlc(
        kafka_input_topic=config.kafka_input_topic_name,
        kafka_output_topic=config.kafka_output_topic_name,
        kafka_broker_address=config.kafka_broker_address,
        ohlc_window_seconds=config.ohlc_windows_seconds
    )
