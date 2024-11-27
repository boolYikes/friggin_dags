from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

## TODO: check commodity data columns
# Constants
default_args = {
    'owner': 'airflow',
}

# Tasks
@task
def extract_commodity_data():
    return "example"

@task
def load_to_redshift(schema, extracted):
    pass

# DAG
with DAG(
    'commidty_pipeline',
    default_args=default_args,
    description='Extract commodity prices and load to Redshift',
    schedule_interval='0 15 * * 0',
    start_date=datetime(2024, 11, 1), # change this on actual run
    catchup=True,
) as dag:
    
    extracted = extract_commodity_data()
    load_to_redshift("tunacome", extracted)
