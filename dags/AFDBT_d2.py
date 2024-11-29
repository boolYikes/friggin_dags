from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import json, os, time

## TODO: Reddit queries <done>
## TODO: don't re-install dependencies if they pre-exist

# Constants
CATCHUP = True
ARGS = {
    'owner': 'airflow',
    'trigger_rule': 'all_success',
    'description': 'Crawl Reddit data and load to Redshift',
    'retries': 0
}


# Helper functions
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_conn_id')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def init_table(cur: object, schema: str, table: str, catchup: bool):
    if not catchup:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    # title, author, created_utc, num_comments, score, permalink, image_url, thumbnail_url
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            date date,
            title varchar(100),
            author varchar(50),
            n_comments int,
            score int,
            link varchar(200),
            image_url varchar(200),
            thumbnail_url varchar(200)
        );
    """)


# DAG: Crawling should be done after some time of the commodities price events to be meaningful
# I'll put 5 hourse later the day of the commodity crawling.
# Mind that the timeframe is subjective not to mention biased!
with DAG(
    dag_id='reddit_pipeline',
    default_args=ARGS,
    schedule_interval='0 20 * * 0', # every sundie, 8 pm
    max_active_runs=1,
    catchup=CATCHUP,
    start_date=datetime(2023, 1, 1), # change this on actual run
) as dag:
    
    ### T1
    # I could've merged T1 and T2 but I chose division of labor over simplicity
    # ## THIS IS NOW ASSIMILATED INTO THE DOCKERFILE
    # install_dependencies = BashOperator(
    #     task_id='dependencies',
    #     bash_command="""
    #         pip install -U git+https://github.com/boolYikes/ProxyBroker.git && \
    #         git clone https://github.com/datavorous/YARS.git
    #     """
    # )

    ### T2
    # stdout proxy using the shift operator
    # this changes from time to time so the txt should be removed afterwards
    get_proxy = BashOperator(
        task_id='get_proxy',
        bash_command="""
            proxybroker find --types HTTP HTTPS --lvl High --countries US --strict -l 10 > /tmp/proxy.txt
        """
    )
    
    ### T3
    # Read the output from T2,
    # parse it and return it to xcom
    # I won't be reusing this function so it's inside DAG.
    # Perhaps it's better to put it outside for readability?
    def proxybroker_parser():
        # proxybroker seems to be async. i'll find a better way
        # https://proxybroker.readthedocs.io/en/latest/
        time.sleep(30)
        proxies = []
        with open('/tmp/proxy.txt', 'rt') as f:
            data = f.read()
            splt = data.split('\n')
            for line in splt:
                if line:
                    # prot = 'https' if 'https' in line else 'http'
                    addr = line.split()[5].split('>')[0]
                    # proxies.append({prot: f'{prot}://{addr}'})
                    proxies.append(addr)
        return json.dumps(proxies)
    parse_proxy = PythonOperator(
        task_id='parse_proxy',
        python_callable=proxybroker_parser
    )

    ### T4
    # import plugin -> run the script with proxy param to YARS(proxy=) -> pass search key param -> go
    # the proxy needs to run with & cuz it doesn't have a detach mode
    # Opted not to use proxy: the plugin script's name will remain the same regardless.
    # echo '{{ ti.xcom_pull(task_ids="parse_proxy") }}' > /tmp/YARS/example/proxies.json && \
    # timeout 60s proxybroker serve --host 127.0.0.1 --http-allowed-codes --port 7531 --types HTTPS --lvl High & \
    reddit_extraction = BashOperator(
        task_id='crawl_reddit',
        bash_command=(
            """
            cd /tmp/YARS && \
            python /tmp/YARS/src/crawl_with_proxy.py '{{ ds }}' '{{ (execution_date + macros.timedelta(days=7)).strftime('%Y-%m-%d') }}'
            """
        )
    )

    ### T5
    # get the output json file, transform (need to merge them into one list)
    def process_reddit_data():
        """
        Transformation
        """
        basenames = list(filter(lambda x: '.json' in x, os.listdir('/tmp/YARS/example/')))
        combined = []

        for basename in basenames:
            fpath = os.path.join('/tmp/YARS/example/', basename)

            with open(fpath, 'r') as file:
                data = json.load(file)

                if isinstance(data, list):
                    combined.extend(data)

        return combined
    reddit_transformation = PythonOperator(
        task_id='reddit_transformation',
        python_callable=process_reddit_data,
    )

    ### T6
    # Loath it up
    def load_to_redshift(schema, table, **context):
        """
        Takes schema and table as arguments and loads transformed data to redshift dw
        """
        cur = get_redshift_connection()
        ti = context['ti']
        transformed = ti.xcom_pull(task_ids='reddit_transformation')
        
        try:
            cur.execute("BEGIN;")
            init_table(cur, schema, table, CATCHUP)
            # title, author, created_utc, num_comments, score, permalink, image_url, thumbnail_url
            for post in transformed:
                query = f"""
                    INSERT INTO {schema}.{table}
                    SELECT '{post["created_utc"]}', '{post["title"]}', '{post["author"]}', {post['num_comments']}, {post['score']}, '{post["permalink"]}', '{post["image_url"]}', '{post["thumbnail_url"]}'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {schema}.{table}
                        WHERE title = '{post["title"]}' AND date = '{post["created_utc"]}'
                    )
                """
                cur.execute(query)
                # reddit don't take day-offs so no weekends filtering
            cur.execute("COMMIT;")
        except Exception as e:
            print(e)
            cur.execute("ROLLBACK;")
            raise
    load = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        provide_context=True,
        op_args=['tunacome', 'afdbt_reddit']
    )

    ### T7
    # dependencies: clean up or not? 
    # proxies and search result jsons should be cleaned up fosho
    cleaner = BashOperator(
        task_id='cleaner',
        bash_command=("""
            rm -rf /tmp/proxies.txt && \
            rm -rf /tmp/YARS
        """)
    )

    get_proxy >> parse_proxy >> reddit_extraction >> reddit_transformation >> load >> cleaner