from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

## TODO: Check proxy setup and querying yars

# Constants
CATCHUP = True
ARGS = {
    'owner': 'airflow',
    'trigger_rule': 'all_success',
    'description': 'Crawl Reddit data and load to Redshift',
    'retries': 0
}

# DAG
with DAG(
    dag_id='reddit_pipeline',
    default_args=ARGS,
    schedule_interval='0 15 * * 0', # every sundie, 3 pm
    max_active_runs=1,
    catchup=CATCHUP,
    start_date=datetime(2023, 1, 1), # change this on actual run
) as dag:
    
    # Tasks
    install_dependencies = BashOperator(
        task_id='dependencies',
        bash_command="""
            pip install -U git+https://github.com/boolYikes/ProxyBroker.git && \
            git clone https://github.com/datavorous/YARS.git
        """
    )

    def get_proxy():
        # get prox using the command > txt
        # load the txt in this block, parse it to produce addresses
        # return the addresses
        return "address"

    def crawl_reddit(proxy):
        """
        Extraction
        """
        # cd to src folder -> plugin loaded -> pass proxy param to YARS(proxy=) -> pass search key param -> go
        return "extracted"

    def process_reddit_data(extracted):
        """
        Transformation
        """
        return "transformed"

    def load_to_redshift(transformed):
        """
        Loading
        """
        pass

    install_dependencies