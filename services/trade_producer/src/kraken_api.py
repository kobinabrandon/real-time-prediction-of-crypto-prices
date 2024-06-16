import json
import requests

from loguru import logger
from websocket import create_connection


class KrakenWebsocketAPI:

    def __init__(self, product_id: str):
        self.websocket = None
        self.product_id = product_id
        self.url = "wss://ws.kraken.com/v2"
        self.is_done = False

    def connect(self):
        self.websocket = create_connection(url=self.url)
        logger.success("Connection established")
        return self.websocket

    def subscribe(self, product_id: str) -> None:
        logger.info(f"Subscribing to trades for {self.product_id}...")

        msg = {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [product_id],
                "snapshot": False
            }
        }

        try:
            # Send subscription request
            self.websocket.send(
                payload=json.dumps(msg)
            )
            logger.success("Subscription successful")

            # Skip two messages received as they contain no trade data
            _ = self.websocket.recv()
            _ = self.websocket.recv()

        except Exception as e:
            logger.error(f"Error subscribing to trades {e}")
            self.websocket.close()
            self.connect()

    def get_trades(self) -> list[dict]:

        self.websocket = self.connect()
        self.subscribe(product_id=self.product_id)
        try:
            message = self.websocket.recv()
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return []

        logger.success(f"Message received: {message}")
        if "heartbeat" in message:
            return []

        parsed_message = json.loads(message)

        trades = []
        for trade in parsed_message["data"]:
            trades.append(
                {
                    "product_id": self.product_id,
                    "price": trade["price"],
                    "volume": trade["qty"],
                    "timestamp": trade["timestamp"]
                }
            )

        return trades


class KrakenRestAPI:

    def __init__(self, product_id: list[str], from_ms: int, to_ms: int):
        """
        Initialisation of the Rest API
        :param product_id: the currency pair for which we want trades
        :param from_ms: the timestamp from which we want to find trades
        :param to_ms: the timestamp after which we no longer seek trades
        """
        self.product_id = product_id
        self.from_ms = from_ms
        self.to_ms = to_ms
        self.is_finished = None

    def get_trades(self) -> list[dict[str, float | str]]:
        """
        Make an HTTP request to the REST API for data between one timestamp and another, and extract
        the metrics of interest from the response. Then check whether the last timestamp in the
        received data is past the targeted end timestamp.

        :return:
        """
        payload = {}

        # The terminal time must be in seconds
        url = f"https://api.kraken.com/0/public/Trades?pair={self.product_id}&since={self.from_ms//1_000}"
        headers = {"Accept": "application/json"}
        response = requests.request(method="GET", url=url, headers=headers, data=payload)

        raw_data = json.loads(response.text)
        data_of_interest = [
            {
                "price": float(trade[0]),
                "volume": float(trade[1]),
                "time": float(trade[2]),
                "product_id": self.product_id
            } for trade in raw_data["result"][self.product_id]
        ]

        last_timestamp_ns = int(raw_data["result"]["last"])
        last_timestamp_ms = last_timestamp_ns//1_000_000

        if last_timestamp_ms >= self.to_ms:
            logger.success(f"Done collecting historical data")
            self.is_finished = True
        return data_of_interest
