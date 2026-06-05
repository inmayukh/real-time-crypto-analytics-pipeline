import logging
import os
import io
import time
from datetime import datetime, timezone, timedelta

import boto3
import pyarrow.parquet as pq
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# S3 config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "crypto-pipeline-datalake-mayukh")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# ClickHouse config
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "crypto_analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "Admin1234")

# Loader config
LOAD_INTERVAL = 300     # run every 5 minutes
LOADED_FILES_KEY = "metadata/loaded_files.txt"  # tracks processed files in S3


def create_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


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


def get_loaded_files(s3_client):
    """Read the list of already-loaded S3 files."""
    try:
        response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=LOADED_FILES_KEY)
        content = response["Body"].read().decode("utf-8")
        return set(content.strip().split("\n")) if content.strip() else set()
    except s3_client.exceptions.NoSuchKey:
        return set()
    except Exception:
        return set()


def mark_file_as_loaded(s3_client, loaded_files, new_file):
    """Append new file to the loaded files tracker in S3."""
    loaded_files.add(new_file)
    content = "\n".join(loaded_files)
    s3_client.put_object(
        Bucket=AWS_S3_BUCKET,
        Key=LOADED_FILES_KEY,
        Body=content.encode("utf-8"),
    )


def list_new_s3_files(s3_client, loaded_files):
    """List all Parquet files in S3 that haven't been loaded yet."""
    new_files = []
    paginator = s3_client.get_paginator("list_objects_v2")

    # Only look at last 7 days of partitions
    for days_ago in range(8):
        date = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        prefix = f"raw/{date}/"

        for page in paginator.paginate(Bucket=AWS_S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet") and key not in loaded_files:
                    new_files.append(key)

    return sorted(new_files)


def load_parquet_to_clickhouse(s3_client, ch_client, s3_key):
    """Download a Parquet file from S3 and insert into ClickHouse."""
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
    buffer = io.BytesIO(response["Body"].read())
    table = pq.read_table(buffer)
    df = table.to_pandas()

    if df.empty:
        logger.warning(f"Empty file skipped: {s3_key}")
        return 0

    # Convert timestamps
    df["event_timestamp"] = df["event_timestamp"].apply(
        lambda x: datetime.fromisoformat(str(x).replace("Z", "+00:00")).replace(tzinfo=None)
    )
    df["ingestion_timestamp"] = df["ingestion_timestamp"].apply(
        lambda x: datetime.fromisoformat(str(x).replace("Z", "+00:00")).replace(tzinfo=None)
    )

    rows = df.values.tolist()
    ch_client.insert(
        "raw_crypto_ticker_events",
        rows,
        column_names=list(df.columns),
    )

    logger.info(f"Loaded {len(rows)} rows from {s3_key} into ClickHouse")
    return len(rows)


def main():
    s3_client = create_s3_client()
    ch_client = create_clickhouse_client()

    logger.info("S3 Loader started — loading Parquet files from S3 into ClickHouse every 5 minutes")

    while True:
        try:
            loaded_files = get_loaded_files(s3_client)
            new_files = list_new_s3_files(s3_client, loaded_files)

            if not new_files:
                logger.info("No new files to load")
            else:
                logger.info(f"Found {len(new_files)} new files to load")
                total_rows = 0

                for s3_key in new_files:
                    try:
                        rows = load_parquet_to_clickhouse(s3_client, ch_client, s3_key)
                        mark_file_as_loaded(s3_client, loaded_files, s3_key)
                        total_rows += rows
                    except Exception as e:
                        logger.exception(f"Failed to load {s3_key}: {e}")

                logger.info(f"Load complete — {total_rows} total rows inserted from {len(new_files)} files")

        except Exception as e:
            logger.exception(f"Loader cycle error: {e}")

        logger.info(f"Sleeping {LOAD_INTERVAL} seconds until next load cycle")
        time.sleep(LOAD_INTERVAL)


if __name__ == "__main__":
    main()