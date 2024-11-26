from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

## TODO: Check proxy setup and querying yars

# Constants
default_args = {
    'owner': 'airflow',
}

# Tasks
@task
def get_proxy():
    pass

@task
def process_reddit_data():
    pass

@task
def load_reddit_to_redshift():
    pass

# DAG
