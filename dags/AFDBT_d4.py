# The final ELT layer where DBT is utilized.
# This will demonstrate the TriggerDagRunOperator usage. (AFDBT_d3 -> AFDBT_d4)
# use inference no + title + content as the composite key?
# profiles.yml must be moved here

from airflow import DAG
from airflow.operators.bash import BashOperator

from plugins.getconn import get_redshift_connection, get_hooked
from datetime import datetime
import json


# CONSTANTS
# Catchup is disabled as this one depends on the previous dag
# Some people seem to use global cursor.
CURSOR = get_redshift_connection('redshift_conn_id')
CONN = json.loads(get_hooked('redshift_conn_id'))
DBT_PROJECT_DIR = "/opt/airflow/dbt_elt"
ARGS = {
    'owner': 'airflow',
    'trigger_rule': 'all_success',
    'description':'Performs DBT-powered ELT.',
    'retries': 0,
    'env': {
        "DBT_USER": CONN["user"],
        "DBT_PASSWORD": CONN["password"],
        "DBT_SCHEMA": CONN["user"],
        "DBT_DATABASE": CONN["dbname"],
        "DBT_PORT": CONN["port"],
        "DBT_HOST": CONN["host"],
    }
}


# DAG
with DAG(
    tags=['ELT', 'DBT'],
    dag_id='dbt_elt',
    default_args=ARGS,
    schedule_interval=None, # every sundie, 10 pm
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2024, 11, 20), # Full refresh. so it does not matter
) as dag:
    
    # Tasks
    # /home/airflow/.local/bin/dbt [command] --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    ### T1
    # I don't have any dependencies. It's for extensibility
    dependencies = BashOperator(
        task_id='dbt_deps',
        bash_command=f"dbt deps --profiles-dir '{DBT_PROJECT_DIR}' --project-dir '{DBT_PROJECT_DIR}'"
    )

    ### T2
    # The actual run
    run_stuff = BashOperator(
        task_id='dbt_run',
        bash_command=f"dbt run --profiles-dir '{DBT_PROJECT_DIR}' --project-dir '{DBT_PROJECT_DIR}'"
    )

    ### T3
    # Tests
    test_stuff = BashOperator(
        task_id='dbt_test',
        bash_command=f"dbt test --profiles-dir '{DBT_PROJECT_DIR}' --project-dir '{DBT_PROJECT_DIR}'"
    )

    # ### T4
    # # Snapshot
    # make_snapshot = BashOperator(
    #     task_id='dbt_snapshot',
    #     bash_command=f'/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}'
    # )

    dependencies >> run_stuff >> test_stuff # >> make_snapshot