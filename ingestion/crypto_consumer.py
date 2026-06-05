import json
import logging
import os
import time
import io
from datetime import datetime, timezone
from collections import defaultdict

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Kafka config
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_prices")

# S3 config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "crypto-pipeline-datalake-mayukh")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# Batching config
BATCH_SIZE = 500        # write to S3 every 500 events
BATCH_TIMEOUT = 60      # or every 60 seconds, whichever comes first

# Metrics
events_processed = 0
events_written_to_s3 = 0
invalid_events = 0
failed_events = 0


def create_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def create_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                group_id="crypto_s3_consumer_group",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            logger.info("Connected to Kafka consumer")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka not ready yet: {e}")
            time.sleep(10)


def is_valid_ticker_event(event):
    required_fields = [
        "event_id", "event_timestamp", "source", "symbol",
        "base_asset", "quote_asset", "price", "open_price",
        "high_price", "low_price", "volume", "quote_volume",
    ]
    for field in required_fields:
        if field not in event:
            return False, f"Missing required field: {field}"

    numeric_fields = ["price", "open_price", "high_price", "low_price", "volume", "quote_volume"]
    for field in numeric_fields:
        if event.get(field) is None or float(event.get(field)) < 0:
            return False, f"Invalid numeric value: {field}"

    if not event.get("symbol", "").endswith("USDT"):
        return False, "Only USDT pairs are supported"

    return True, None


def write_batch_to_s3(s3_client, batch):
    if not batch:
        return

    now = datetime.now(timezone.utc)
    s3_key = f"raw/{now.strftime('%Y-%m-%d')}/{now.strftime('%H')}/{now.strftime('%Y%m%d_%H%M%S')}.parquet"

    # Convert batch to PyArrow table
    table = pa.Table.from_pydict({
        "event_id":          [r["event_id"] for r in batch],
        "event_timestamp":   [r["event_timestamp"] for r in batch],
        "ingestion_timestamp": [now.isoformat() for _ in batch],
        "source":            [r["source"] for r in batch],
        "symbol":            [r["symbol"] for r in batch],
        "base_asset":        [r["base_asset"] for r in batch],
        "quote_asset":       [r["quote_asset"] for r in batch],
        "price":             [float(r["price"]) for r in batch],
        "open_price":        [float(r["open_price"]) for r in batch],
        "high_price":        [float(r["high_price"]) for r in batch],
        "low_price":         [float(r["low_price"]) for r in batch],
        "volume":            [float(r["volume"]) for r in batch],
        "quote_volume":      [float(r["quote_volume"]) for r in batch],
    })

    # Write to in-memory buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload to S3
    s3_client.put_object(
        Bucket=AWS_S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    logger.info(f"Written {len(batch)} events to s3://{AWS_S3_BUCKET}/{s3_key}")
    return s3_key


def main():
    global events_processed, events_written_to_s3, invalid_events, failed_events

    s3_client = create_s3_client()
    consumer = create_kafka_consumer()

    logger.info("Binance WebSocket crypto consumer started — writing to S3")

    batch = []
    last_flush_time = time.time()

    for message in consumer:
        event = message.value
        events_processed += 1

        try:
            is_valid, error_message = is_valid_ticker_event(event)

            if not is_valid:
                invalid_events += 1
                logger.warning(f"Invalid event | event_id={event.get('event_id')} | error={error_message}")
                continue

            batch.append(event)

            # Flush batch if size or time threshold reached
            time_since_flush = time.time() - last_flush_time
            if len(batch) >= BATCH_SIZE or time_since_flush >= BATCH_TIMEOUT:
                write_batch_to_s3(s3_client, batch)
                events_written_to_s3 += len(batch)
                logger.info(f"Flushed {len(batch)} events | total_written={events_written_to_s3}")
                batch = []
                last_flush_time = time.time()

        except Exception as e:
            failed_events += 1
            logger.exception(f"Error processing event | event_id={event.get('event_id')} | error={e}")


if __name__ == "__main__":
    main()