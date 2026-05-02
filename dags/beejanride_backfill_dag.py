from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.dbt_runner import run_dbt_command

with DAG(
    dag_id="beejanride_backfill",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    full_refresh = PythonOperator(
        task_id="dbt_full_refresh",
        python_callable=lambda: run_dbt_command("build --full-refresh")
    )