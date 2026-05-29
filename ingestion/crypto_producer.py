import json
import logging
import os
import time
from datetime import datetime

from kafka import KafkaProducer
import websocket


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_prices")

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/!miniTicker@arr"

TRACKED_SYMBOLS = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
    "LTCUSDT", "TRXUSDT", "ATOMUSDT", "ETCUSDT", "XLMUSDT",
    "BCHUSDT", "FILUSDT", "APTUSDT", "NEARUSDT", "ARBUSDT",
    "OPUSDT", "INJUSDT", "SUIUSDT", "SEIUSDT", "AAVEUSDT",
    "UNIUSDT", "MKRUSDT", "RUNEUSDT", "GRTUSDT", "ALGOUSDT",
    "VETUSDT", "ICPUSDT", "EGLDUSDT", "SANDUSDT", "MANAUSDT",
    "AXSUSDT", "THETAUSDT", "EOSUSDT", "KAVAUSDT", "FLOWUSDT",
    "CHZUSDT", "CRVUSDT", "COMPUSDT", "SNXUSDT", "ZILUSDT",
    "ENJUSDT", "DYDXUSDT", "IMXUSDT", "APEUSDT", "GALAUSDT",
    "FTMUSDT", "HBARUSDT", "QNTUSDT", "LDOUSDT", "RNDRUSDT"
}


def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("Connected to Kafka producer")
            return producer

        except Exception as e:
            logger.warning(f"Kafka not ready yet: {e}")
            time.sleep(10)


producer = create_kafka_producer()


def get_base_asset(symbol):
    if symbol.endswith("USDT"):
        return symbol.replace("USDT", "")
    return symbol


def build_event(ticker):
    symbol = ticker.get("s")

    event_timestamp = datetime.utcnow().isoformat()
    binance_event_time = ticker.get("E")

    return {
        "event_id": f"{symbol}_{binance_event_time}",
        "event_timestamp": event_timestamp,
        "source": "binance_websocket",
        "symbol": symbol,
        "base_asset": get_base_asset(symbol),
        "quote_asset": "USDT",
        "price": float(ticker.get("c", 0)),
        "open_price": float(ticker.get("o", 0)),
        "high_price": float(ticker.get("h", 0)),
        "low_price": float(ticker.get("l", 0)),
        "volume": float(ticker.get("v", 0)),
        "quote_volume": float(ticker.get("q", 0)),
    }


def on_message(ws, message):
    try:
        tickers = json.loads(message)

        produced_count = 0

        for ticker in tickers:
            symbol = ticker.get("s")

            if symbol not in TRACKED_SYMBOLS:
                continue

            event = build_event(ticker)

            future = producer.send(KAFKA_TOPIC, value=event)
            future.get(timeout=10)

            produced_count += 1

        if produced_count > 0:
            logger.info(f"Produced {produced_count} Binance ticker events to Kafka")

    except Exception:
        logger.exception("Error processing Binance WebSocket message")


def on_error(ws, error):
    logger.error(f"Binance WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.warning(
        f"Binance WebSocket closed | status={close_status_code} | message={close_msg}"
    )


def on_open(ws):
    logger.info("Connected to Binance WebSocket stream")


logger.info("Binance WebSocket crypto producer started")


while True:
    try:
        ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        ws.run_forever(ping_interval=30, ping_timeout=10)

    except Exception:
        logger.exception("WebSocket producer crashed")

    logger.warning("Restarting Binance WebSocket producer in 10 seconds")
    time.sleep(10)