import json
import logging
import os
import time
from datetime import datetime

import clickhouse_connect
import requests
from kafka import KafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:9092")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "crypto_analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin123")

URL = "https://api.coingecko.com/api/v3/simple/price"


api_calls = 0
successful_api_calls = 0
rate_limit_errors = 0
events_produced = 0
failed_api_calls = 0


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


client = create_clickhouse_client()
producer = create_kafka_producer()


def log_producer_metrics(status="running"):
    client.insert(
        "producer_api_metrics",
        [[
            api_calls,
            successful_api_calls,
            rate_limit_errors,
            events_produced,
            failed_api_calls,
            status,
        ]],
        column_names=[
            "api_calls",
            "successful_api_calls",
            "rate_limit_errors",
            "events_produced",
            "failed_api_calls",
            "status",
        ],
    )


logger.info("Crypto producer started")


while True:
    try:
        api_calls += 1

        response = requests.get(
            URL,
            params={
                "ids": "bitcoin,ethereum",
                "vs_currencies": "usd",
                "include_last_updated_at": "true",
            },
            timeout=10,
        )

        data = response.json()

        if response.status_code == 429 or "status" in data:
            rate_limit_errors += 1
            logger.warning(f"API error response received: {data}")
            log_producer_metrics(status="rate_limited")
            time.sleep(60)
            continue

        if "bitcoin" not in data or "ethereum" not in data:
            failed_api_calls += 1
            logger.warning(f"Unexpected API response: {data}")
            log_producer_metrics(status="invalid_response")
            time.sleep(60)
            continue

        successful_api_calls += 1

        event = {
            "event_id": str(datetime.utcnow().timestamp()),
            "event_timestamp": datetime.utcnow().isoformat(),
            "source": "coingecko_api",
            "prices": data,
        }

        future = producer.send("crypto_prices", value=event)
        future.get(timeout=10)

        events_produced += 1

        logger.info(f"Produced event to Kafka | event_id={event['event_id']}")

        log_producer_metrics()

        time.sleep(60)

    except Exception:
        failed_api_calls += 1
        logger.exception("Producer processing failed")

        try:
            log_producer_metrics(status="error")
        except Exception:
            logger.exception("Failed to log producer metrics")

        time.sleep(60)