from airflow.utils.email import send_email
import os

ALERT_EMAIL = os.getenv("ALERT_EMAIL")


def success_alert(context):
    subject = f"✅ SUCCESS: DAG {context['dag'].dag_id}"
    
    body = f"""
    DAG: {context['dag'].dag_id}
    Run ID: {context.get('run_id')}
    Status: SUCCESS
    """

    send_email(to=ALERT_EMAIL, subject=subject, html_content=body)


def failure_alert(context):
    subject = f"❌ FAILURE: DAG {context['dag'].dag_id}"

    body = f"""
    DAG: {context['dag'].dag_id}
    Task Failed: {context['task_instance'].task_id}
    Execution Time: {context.get('execution_date')}
    Log URL: {context['task_instance'].log_url}
    """

    send_email(to=ALERT_EMAIL, subject=subject, html_content=body)