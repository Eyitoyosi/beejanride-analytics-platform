from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
 
 
dag = DAG(
    dag_id="commit_weather",
    start_date=datetime(2026, 3, 10),
    schedule="@daily",
    catchup=True
)
 
 
def fecth_weather():
    print("Weather data fetched successfully")
 
def transform_weather():
    print("Weather data transformed")
 
def load_weather_data():
    print("Weather data loaded successfully")
 
 
fetch_data = PythonOperator(
    task_id="fecth_weather",
    python_callable=fecth_weather,
    dag=dag
)
 
 
transform_data = PythonOperator(
    task_id="transform_weather",
    python_callable=transform_weather,
    dag=dag
)
 
 
load_data = PythonOperator(
    task_id="load_weather_data",
    python_callable=load_weather_data,
    dag=dag
)
 
 
fetch_data >> transform_data >> load_data


