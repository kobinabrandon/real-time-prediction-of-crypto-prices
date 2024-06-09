import json

from time import sleep
from loguru import logger 
from websocket import create_connection


class KrakenWebsocketTradeAPI():

    def __init__(self, product_id: str):
        self.product_id = product_id
        self.url = "wss://ws.kraken.com/v2"
        
    def connect(self):
        self._ws = create_connection(url=self.url)
        logger.success("Connection established")

        return self._ws

    def subscribe(self, product_id: str) -> None:

        logger.info(f"Subscribing to trades for {self.product_id}...")
        
        self.msg = {
            "method": "subscribe",
            "params": {
                "channel": "trade", 
                "symbol": [product_id], 
                "snapshot": False
            }
        }
         
        try:
            # Send subscription request
            self._ws.send(
                payload=json.dumps(self.msg)
            )
            logger.success("Subscription successful")

            # Skip two messages received as they contain no trade data
            _ = self._ws.recv()
            _ = self._ws.recv()

        except Exception as e:
            logger.error(f"Error subscribing to trades {e}")
            self._ws.close()
            self.connect()


    def get_trades(self) -> list[dict]:

        self._ws = self.connect()
        self.subscribe(product_id=self.product_id)
        
        try:
            message = self._ws.recv()

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

