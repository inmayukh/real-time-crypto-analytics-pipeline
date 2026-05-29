from datetime import datetime, timedelta
import socket

import clickhouse_connect
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor


CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "admin123"
CLICKHOUSE_DATABASE = "crypto_analytics"

KAFKA_HOST = "kafka"
KAFKA_PORT = 9092


def task_failure_alert(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")

    print(
        f"""
        ALERT: Airflow task failed

        DAG: {dag_id}
        Task: {task_id}
        Execution Date: {execution_date}
        Log URL: {task_instance.log_url}
        """
    )


def is_port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except Exception:
        return False


def check_kafka_available():
    return is_port_open(KAFKA_HOST, KAFKA_PORT)


def check_clickhouse_available():
    return is_port_open(CLICKHOUSE_HOST, CLICKHOUSE_PORT)


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def has_recent_raw_events():
    client = get_clickhouse_client()

    result = client.query(
        """
        SELECT count()
        FROM crypto_analytics.raw_crypto_ticker_events
        WHERE ingestion_timestamp >= now() - INTERVAL 5 MINUTE
        """
    )

    recent_count = result.result_rows[0][0]

    print(f"Recent raw ticker events in last 5 minutes: {recent_count}")

    return recent_count > 0


def validate_pipeline_health():
    client = get_clickhouse_client()

    recent_raw_events = client.query(
        """
        SELECT count()
        FROM crypto_analytics.raw_crypto_ticker_events
        WHERE ingestion_timestamp >= now() - INTERVAL 5 MINUTE
        """
    ).result_rows[0][0]

    failed_events_recent = client.query(
        """
        SELECT count()
        FROM crypto_analytics.failed_crypto_events
        WHERE failed_at >= now() - INTERVAL 15 MINUTE
        """
    ).result_rows[0][0]

    unique_symbols_recent = client.query(
        """
        SELECT countDistinct(symbol)
        FROM crypto_analytics.raw_crypto_ticker_events
        WHERE ingestion_timestamp >= now() - INTERVAL 5 MINUTE
        """
    ).result_rows[0][0]

    print(f"Recent raw events: {recent_raw_events}")
    print(f"Recent failed events: {failed_events_recent}")
    print(f"Recent unique symbols: {unique_symbols_recent}")

    if recent_raw_events == 0:
        raise ValueError("ALERT: No raw ticker events received in the last 5 minutes.")

    if unique_symbols_recent < 10:
        raise ValueError(
            f"ALERT: Only {unique_symbols_recent} symbols received recently. Expected broader market coverage."
        )

    if failed_events_recent > 100:
        raise ValueError(
            f"ALERT: Too many failed events in the last 15 minutes: {failed_events_recent}"
        )

    print("Pipeline health validation passed.")


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
}


with DAG(
    dag_id="crypto_analytics_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    description="Orchestrates dbt transformations, tests, sensors, and health checks for crypto analytics pipeline",
) as dag:

    wait_for_kafka = PythonSensor(
        task_id="wait_for_kafka",
        python_callable=check_kafka_available,
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    wait_for_clickhouse = PythonSensor(
        task_id="wait_for_clickhouse",
        python_callable=check_clickhouse_available,
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    wait_for_recent_raw_events = PythonSensor(
        task_id="wait_for_recent_raw_events",
        python_callable=has_recent_raw_events,
        poke_interval=60,
        timeout=600,
        mode="poke",
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
        cd /opt/project/crypto_dbt_project &&
        dbt run --profiles-dir /opt/project/crypto_dbt_project/profiles
        """,
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="""
        cd /opt/project/crypto_dbt_project &&
        dbt test --profiles-dir /opt/project/crypto_dbt_project/profiles
        """,
    )

    validate_pipeline_health_task = PythonOperator(
        task_id="validate_pipeline_health",
        python_callable=validate_pipeline_health,
    )

    [wait_for_kafka, wait_for_clickhouse] >> wait_for_recent_raw_events
    wait_for_recent_raw_events >> run_dbt_models >> run_dbt_tests >> validate_pipeline_health_task