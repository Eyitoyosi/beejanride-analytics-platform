import sys
from airflow.sdk import DAG, task
from pendulum import datetime, duration
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

# Make include/ visible
sys.path.append("/opt/airflow/include")

# Importing  the callback function
from failure_callback import send_failure_email


# Default args
arg = {
    "on_failure_callback": send_failure_email,  
    "params": {
        "dag_owner": "Alani"   # 
    },
    "retries": 3,
    "retry_delay": duration(seconds=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": duration(hours=2)
}


with DAG(
    dag_id="commit_dependency",
    start_date=datetime(2026, 4, 7),
    schedule="@daily",
    tags=["Commit", "Engineering"],
    description="This is a DAG to practice dependency",
    doc_md="This is a DAG documentation",
    default_args=arg
) as dag:

    CRM_CHANGE_DATE = datetime(2026, 4, 20)

    start = EmptyOperator(task_id="start")

    @task(retries=2)
    def raise_error():
        # This forces failure so callback triggers
        raise KeyError("Simulated failure")

    get_old_customer_data = EmptyOperator(task_id="get_old_customer_data")
    clean_old_customer_data = EmptyOperator(task_id="clean_old_customer_data")

    get_new_customer_data = EmptyOperator(task_id="get_new_customer_data")
    clean_new_customer_data = EmptyOperator(task_id="clean_new_customer_data")

    def pick_crm_date(CRM_CHANGE_DATE, **context):
        if context["logical_date"] < CRM_CHANGE_DATE:
            return "get_old_customer_data"
        else:
            return "get_new_customer_data"

    pick_crm_data = BranchPythonOperator(
        task_id="pick_crm_data",
        python_callable=pick_crm_date,
        op_args=[CRM_CHANGE_DATE]
    )

    get_complaint_data = EmptyOperator(task_id="get_complaint_data")
    clean_complaint_data = EmptyOperator(task_id="clean_complaint_data")

    join_datasets = EmptyOperator(
        task_id="join_datasets",
        trigger_rule="none_failed"
    )

    train_ml = EmptyOperator(task_id="train_ml")
    deploy_ml = EmptyOperator(task_id="deploy_ml")

   
    start >> raise_error() >> [pick_crm_data, get_complaint_data]
    pick_crm_data >> [get_old_customer_data, get_new_customer_data]
    get_old_customer_data >> clean_old_customer_data
    get_new_customer_data >> clean_new_customer_data
    get_complaint_data >> clean_complaint_data
    [clean_old_customer_data, clean_new_customer_data, clean_complaint_data] >> join_datasets
    join_datasets >> train_ml >> deploy_ml