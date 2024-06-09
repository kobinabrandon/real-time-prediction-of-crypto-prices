from quixstreams import Application


def trade_to_ohlc(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_output_topic: str,
    ohlc_window_seconds: int
) -> None:

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="trade_to_ohlc"
    )

    input_topic = app.topic(name=kafka_input_topic, value_serializer="json")
    output_topic = app.topic(name=kafka_output_topic, value_serializer="json")

    streaming_df = app.dataframe(topic=input_topic)