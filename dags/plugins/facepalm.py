from slack_sdk.webhook import WebhookClient
from airflow.models import Variable

def send_slack_notification(context, status):
    """
    HERALD OF DOOM context['state']
    """
    slack_webhook = Variable.get("slack_webhook_url")
    webhook = WebhookClient(slack_webhook)

    dag_id = context['dag_run'].dag_id
    run_id = context['dag_run'].run_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    
    stat_dict = {'success': ':green-check-mark:', 'failed': ':no_entry_sign:'}
    message = f"""
    DAG  status {stat_dict[status]}
    - *DAG ID*: `{dag_id}`
    - *Run ID*: `{run_id}`
    - *Execution Date*: `{execution_date}`
    [View Logs]({log_url})
    """

    response = webhook.send(text=message)
    if response.status_code != 200:
        raise ValueError(f"Slack webhook failed with code {response.status_code}: {response.body}")