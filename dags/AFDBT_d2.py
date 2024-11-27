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
def install_dependencies():
    pass

@task
def get_proxy():
    return "address"

@task
def crawl_reddit(proxy):
    """
    Extraction
    """
    return "extracted"

@task
def process_reddit_data(extracted):
    """
    Transformation
    """
    return "transformed"

@task
def load_to_redshift(transformed):
    """
    Loading
    """
    pass

# DAG
with DAG(
    'reddit_pipeline',
    default_args=default_args,
    description='Crawl Reddit data and load to Redshift',
    schedule_interval='0 15 * * 0',
    start_date=datetime(2024, 11, 1), # change this on actual use
    catchup=True,
) as dag:
    
    install_dependencies()
    address = get_proxy()
    extracted = crawl_reddit(address)
    transformed = process_reddit_data(extracted)
    load_to_redshift(transformed)