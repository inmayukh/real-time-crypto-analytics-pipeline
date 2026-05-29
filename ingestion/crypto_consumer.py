import json
import logging
import os
import time
from datetime import datetime

import clickhouse_connect
from kafka import KafkaConsumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_prices")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "crypto_analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin123")


events_processed = 0
events_inserted = 0
invalid_events = 0
failed_events = 0


def create_clickhouse_client():
    while True:
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DB,
            )
            logger.info("Connected to ClickHouse")
            return client

        except Exception as e:
            logger.warning(f"ClickHouse not ready yet: {e}")
            time.sleep(10)


def create_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                group_id="crypto_clickhouse_consumer_group",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            logger.info("Connected to Kafka consumer")
            return consumer

        except Exception as e:
            logger.warning(f"Kafka not ready yet: {e}")
            time.sleep(10)


client = create_clickhouse_client()
consumer = create_kafka_consumer()


def log_failed_event(event, error_message):
    client.insert(
        "failed_crypto_events",
        [[
            event.get("event_id", "unknown"),
            error_message,
            json.dumps(event),
        ]],
        column_names=[
            "event_id",
            "error_message",
            "raw_event",
        ],
    )


def log_consumer_metrics(status="running"):
    client.insert(
        "consumer_processing_metrics",
        [[
            events_processed,
            events_inserted,
            invalid_events,
            failed_events,
            status,
        ]],
        column_names=[
            "events_processed",
            "events_inserted",
            "invalid_events",
            "failed_events",
            "status",
        ],
    )


def is_valid_ticker_event(event):
    required_fields = [
        "event_id",
        "event_timestamp",
        "source",
        "symbol",
        "base_asset",
        "quote_asset",
        "price",
        "open_price",
        "high_price",
        "low_price",
        "volume",
        "quote_volume",
    ]

    for field in required_fields:
        if field not in event:
            return False, f"Missing required field: {field}"

    numeric_fields = [
        "price",
        "open_price",
        "high_price",
        "low_price",
        "volume",
        "quote_volume",
    ]

    for field in numeric_fields:
        if event.get(field) is None:
            return False, f"Missing numeric value: {field}"

        if float(event.get(field)) < 0:
            return False, f"Negative numeric value: {field}"

    if not event.get("symbol", "").endswith("USDT"):
        return False, "Only USDT pairs are supported"

    return True, None


logger.info("Binance WebSocket crypto consumer started")


for message in consumer:
    event = message.value
    events_processed += 1

    try:
        is_valid, error_message = is_valid_ticker_event(event)

        if not is_valid:
            invalid_events += 1

            logger.warning(
                f"Skipping invalid ticker event | "
                f"event_id={event.get('event_id')} | error={error_message}"
            )

            log_failed_event(event, error_message)
            log_consumer_metrics()
            continue

        row = [[
            event["event_id"],
            datetime.fromisoformat(event["event_timestamp"]),
            event["source"],
            event["symbol"],
            event["base_asset"],
            event["quote_asset"],
            float(event["price"]),
            float(event["open_price"]),
            float(event["high_price"]),
            float(event["low_price"]),
            float(event["volume"]),
            float(event["quote_volume"]),
        ]]

        client.insert(
            "raw_crypto_ticker_events",
            row,
            column_names=[
                "event_id",
                "event_timestamp",
                "source",
                "symbol",
                "base_asset",
                "quote_asset",
                "price",
                "open_price",
                "high_price",
                "low_price",
                "volume",
                "quote_volume",
            ],
        )

        events_inserted += 1

        logger.info(
            f"Inserted Binance ticker event into ClickHouse | "
            f"event_id={event.get('event_id')} | symbol={event.get('symbol')}"
        )

        log_consumer_metrics()

    except Exception as e:
        failed_events += 1
        error_message = str(e)

        logger.exception(
            f"Error processing ticker event | event_id={event.get('event_id')}"
        )

        try:
            log_failed_event(event, error_message)
            log_consumer_metrics(status="error")
        except Exception:
            logger.exception("Failed to log error event or consumer metrics")