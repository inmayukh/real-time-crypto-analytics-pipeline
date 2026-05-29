from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    description="Run dbt transformations and tests for crypto analytics pipeline",
) as dag:

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
        cd /opt/project/crypto_dbt_project &&
        dbt run --profiles-dir /opt/project/crypto_dbt_project/profiles
        """
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="""
        cd /opt/project/crypto_dbt_project &&
        dbt test --profiles-dir /opt/project/crypto_dbt_project/profiles
        """
    )

    run_dbt_models >> run_dbt_tests