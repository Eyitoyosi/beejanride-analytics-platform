from airflow.providers.smtp.operators.smtp import EmailOperator


def send_failure_email(context):
    """
    What does this function do?
    This function will be triggered when a task fails. It extracts useful info from Airflow context and sends an email.
    """

    # Extract values from Airflow context
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id

    dag = context.get("dag")
    dag_owner = context.get("params", {}).get("dag_owner", "Unknown User")

    # Na for the beaitiful Email message
    subject = "🚨 Airflow Task Failure Alert"
    message = f"""
    Hi {dag_owner},<br><br>
    Your task with <b>task_id: {task_id}</b> has failed.<br><br>
    Please check your Airflow logs.<br><br>
    Regards,<br>
    Airflow
    """

    # oya let us use the EmailOperator 
    email = EmailOperator(
        task_id="failure_email_notification",
        to="harlahbee@gmail.com",  #Alter later don't hardcode
        subject=subject,
        html_content=message,
    )

    # Execute the email task manually
    email.execute(context=context)