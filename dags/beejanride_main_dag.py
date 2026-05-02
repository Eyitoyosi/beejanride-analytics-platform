from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from datetime import datetime, timedelta
from utils.dbt_runner import run_dbt_command
from utils.alerts import success_alert, failure_alert
import os


default_args = {
    "owner": "beejanride-data",
    "retries": 1,
    "on_failure_callback": failure_alert,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="beejanride_elt_pipeline",
    description="BeejanRide main ELT pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["beejanride", "elt", "production"]
) as dag:

    trigger_airbyte = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id="airbyte_default",
        connection_id=os.getenv("AIRBYTE_CONNECTION_ID"),
        asynchronous=True
    )

    wait_airbyte = AirbyteJobSensor(
        task_id="wait_for_airbyte",
        airbyte_conn_id="airbyte_default",
        airbyte_job_id=trigger_airbyte.output
    )

    dbt_run_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=lambda: run_dbt_command("run --select staging")
    )

    dbt_test_staging = PythonOperator(
        task_id="dbt_test_staging",
        python_callable=lambda: run_dbt_command("test --select staging")
    )

    dbt_run_intermediate = PythonOperator(
        task_id="dbt_run_intermediate",
        python_callable=lambda: run_dbt_command("run --select intermediate")
    )

    dbt_run_marts = PythonOperator(
        task_id="dbt_run_marts",
        python_callable=lambda: run_dbt_command("run --select marts")
    )

    dbt_test_marts = PythonOperator(
        task_id="dbt_test_marts",
        python_callable=lambda: run_dbt_command("test --select marts")
    )

    dbt_snapshot = PythonOperator(
        task_id="dbt_snapshot",
        python_callable=lambda: run_dbt_command("snapshot")
    )

    success_task = PythonOperator(
        task_id="send_success_alert",
        python_callable=success_alert
    )

    trigger_airbyte >> wait_airbyte >> dbt_run_staging >> dbt_test_staging \
    >> dbt_run_intermediate >> dbt_run_marts >> dbt_test_marts \
    >> dbt_snapshot >> success_task