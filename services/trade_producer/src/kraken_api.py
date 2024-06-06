import json

from loguru import logger 
from websocket import create_connection


class KrakenWebsocketTradeAPI():

    def __init__(self, product_id: str):
        self.product_id = product_id
        self.url = "wss://ws.kraken.com/v2"
        
    def connect(self):
        self._ws = create_connection(url=self.url)
        logger.success("Connection established")


    def subscribe(self) -> None:

        logger.info(f"Subscribing to trades for {self.product_id}")
        self.msg = {
            "method": "subscribe",
            "channel": "trade",
            "params": {
                "channel": "trade", 
                "symbol": [self.product_id], 
                "snapshot": False
            }
        }

        # Send subscription request
        self._ws.send(
            payload=json.dumps(self.msg)
        )
        logger.success("Subscription successful")

        # Skip two messages received as they contain no trade data
        _ = self._ws.recv()
        _ = self._ws.recv()

    def get_trades(self) -> list[dict]:
        
        self.connect()
        self.subscribe()

        message = self._ws.recv()
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



