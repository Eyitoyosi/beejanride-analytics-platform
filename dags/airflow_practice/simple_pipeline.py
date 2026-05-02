from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

# define the functions for the tasks


def generate_random_number():
    numbers = list(range(1, 11))
    print(f"Generated random number: {numbers}")
    return numbers


def process_data(ti):
    # get data from previous task
    numbers = ti.xcom_pull(task_ids='generate_task')
    squared = [n**2 for n in numbers]  # square each number
    print("Squared numbers:", squared)
    return squared


def save_data(ti):
    squared = ti.xcom_pull(task_ids='process_task')

    with open('/tmp/processed_data.txt', 'w') as f:
        for num in squared:
            f.write(str(num) + "\n")

    print("Data saved to /tmp/processed_data.txt")


def notify():
    print("✅ Pipeline completed successfully!")


with DAG(
    dag_id="simple_pipeline",
    start_date=datetime(2026, 3, 22),
    schedule=None,  # run manually
    catchup=False
) as dag:

    generate_task = PythonOperator(
        task_id="generate_task",
        python_callable=generate_random_number
    )

    process_task = PythonOperator(
        task_id="process_task",
        python_callable=process_data
    )

    save_task = PythonOperator(
        task_id="save_task",
        python_callable=save_data
    )

    notify_task = PythonOperator(
        task_id="notify_task",
        python_callable=notify
    )

    generate_task >> process_task >> save_task >> notify_task
