import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.smtp.operators.smtp import EmailOperator


sys.path.append("/opt/airflow/include")

from rocket_functions import fetch_launches, download_images


# Default arguments for the DAG
default_args = {
    "owner": "John Rambo",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="rocket_enthusiast_v2",
    start_date=datetime(2026, 3, 28),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["nasa", "space_devs", "email", "variables"],
)
def rocket_tracker_dag():

    # 1. Fetch the launch data
    @task
    def get_launches():
        launches = fetch_launches()
        print(f"Fetched {len(launches)} launches from API.")
        return launches

    # 2. Download images
    @task
    def download_pictures(launches):
        stats = download_images(launches)
        print(f"Download summary: {stats}")
        return stats

    # 3. Notify John in my logs
    @task
    def notify_john(stats: dict):
        print(f"Update for John: Processed {stats['total_processed']} launches.")
        print(f"Downloaded {stats['new_downloads']} new rocket images today!")

    # Task objects
    launch_data = get_launches()
    image_stats = download_pictures(launch_data)
    notify_task = notify_john(image_stats)

    # 4. Email notification task
    email_task = EmailOperator(
        task_id="send_email_notification",
        to=Variable.get("notification_email", default_var="harlahbee@gmail.com"),
        subject="🚀 Rocket Pipeline Completed",
        html_content="""
        <h3>Rocket Pipeline Finished</h3>
        <p>The rocket enthusiast pipeline has completed successfully.</p>
        <p>Check Airflow logs for details.</p>
        """,
    )

    # Explicit dependency using bitwise operator
    notify_task >> email_task


rocket_tracker_dag()