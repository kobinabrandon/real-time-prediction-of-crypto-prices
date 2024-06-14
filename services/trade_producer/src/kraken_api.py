import json
import requests

from loguru import logger
from websocket import create_connection


class KrakenWebsocketAPI:

    def __init__(self, product_id: str):
        self.websocket = None
        self.product_id = product_id
        self.url = "wss://ws.kraken.com/v2"

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

    def __init__(self, product_ids: list[str], from_ms: int, to_ms: int):
        """
        Initialisation of the Rest API
        :param product_ids: list of product IDs for which we want trades
        :param from_ms: the timestamp from which we want to find trades
        :param to_ms: the timestamp after which we no longer seek trades
        """

        self.product_ids = product_ids
        self.from_ms = from_ms
        self.to_ms = to_ms

    @staticmethod
    def get_trades(self) -> list[dict]:
        payload = {}
        url = "https://api.kraken.com/0/public/Trades"
        headers = {"Accept": "application/json"}

        response = requests.request(method="GET", url=url, headers=headers, data=payload)

        print(response.text)
