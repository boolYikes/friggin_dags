# The final ELT layer where DBT is utilized.
# This will demonstrate the TriggerDagRunOperator usage. (AFDBT_d3 -> AFDBT_d4)
# It also triggers the next DAG (AFDBT_d4 -> AFDBT_d5)
# use inference no + title + content as the composite key?
# profiles.yml must be moved here

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from plugins.facepalm import send_slack_notification

from datetime import datetime


# CONSTANTS
# Catchup is disabled as this one depends on the previous dag
# Some people seem to use global cursor.
hook = PostgresHook(postgres_conn_id='redshift_conn_id')
CONN = hook.get_connection('redshift_conn_id')
DBT_PROJECT_DIR = "/opt/airflow/dbt_elt"
ARGS = {
    'owner': 'airflow',
    'trigger_rule': 'all_success',
    'description':'Performs DBT-powered ELT.',
    'retries': 0,
    'env': {
        "DBT_USER": CONN.login,
        "DBT_PASSWORD": CONN.password,
        "DBT_SCHEMA": CONN.login,
        "DBT_DATABASE": CONN.schema,
        "DBT_HOST": CONN.host,
    }
}


# Helper functions
def success_callback(context):
    send_slack_notification(context, status="success")
    
def failure_callback(context):
    send_slack_notification(context, status="failed")


# DAG
with DAG(
    tags=['ELT', 'DBT'],
    dag_id='dbt_elt',
    default_args=ARGS,
    schedule_interval=None, # every sundie, 10 pm
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2024, 11, 20), # Full refresh. so it does not matter
    on_success_callback=success_callback,
    on_failure_callback=failure_callback
) as dag:
    
    # Tasks
    # /home/airflow/.local/bin/dbt [command] --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    ### T1
    # I don't have any dependencies. It's for extensibility
    dependencies = BashOperator(
        task_id='dbt_deps',
        bash_command=(
            f"""
                /home/airflow/.local/bin/dbt deps --profiles-dir '{DBT_PROJECT_DIR}' --project-dir '{DBT_PROJECT_DIR}'
            """
        )
    )

    ### T2
    # The actual run
    run_stuff = BashOperator(
        task_id='dbt_run',
        bash_command=(
            f"""
                /home/airflow/.local/bin/dbt run --profiles-dir '{DBT_PROJECT_DIR}' --project-dir '{DBT_PROJECT_DIR}'
            """
        )
    )

    ### T3
    # Tests
    test_stuff = BashOperator(
        task_id='dbt_test',
        bash_command=(
            f"""
                /home/airflow/.local/bin/dbt test --profiles-dir '{DBT_PROJECT_DIR}' --project-dir '{DBT_PROJECT_DIR}'
            """
        )
    )

    # ### T4
    # # Snapshot
    # make_snapshot = BashOperator(
    #     task_id='dbt_snapshot',
    #     bash_command=f'/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}'
    # )

    next_one = TriggerDagRunOperator(
        task_id='trigger',
        trigger_dag_id='to_google_sheet',
        wait_for_completion=False,
    )

    dependencies >> run_stuff >> test_stuff >> next_one # >> make_snapshot