from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from plugins.getconn import get_redshift_connection

from datetime import datetime
import json, os, time, logging, re

## Just found out that reddit doesn't have a date-based filtering, but it can only fetch data by "[day|week|month|year] ago" units
## Hence the DAG has to run every week but run with a large limit at first
## TODO: Reddit queries <done>
## TODO: don't re-install dependencies if they pre-exist

# Constants
CATCHUP = False
ARGS = {
    'owner': 'airflow',
    'trigger_rule': 'all_success',
    'description': 'Crawl Reddit data and load to Redshift',
    'retries': 0
}


## Helper functions
# NOTE: Flippin Redshift enforces text type to varchar(256). That's BS
def init_table(cur: object, schema: str, table: str):
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                title text,
                content text,
                score int,
                num_comments int,
                created_utc timestamp,
                thumbnail_link text,
                category text,
                up_votes int,
                up_ratio float,
                subreddit text,
                author text,
                link text,
                search_key text,
                collected_on timestamp default GETDATE()
            );
        """)
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        cur.execute("COMMIT;")
    except Exception as e:
        print(e)
        cur.execute("ROLLBACK;")
        raise


# DAG: Crawling should be done after some time of the commodities price events to be meaningful
# I'll put 5 hourse later the day of the commodity crawling.
# Mind that the timeframe is subjective not to mention biased!
# NOTE: Set start_date to a week ago or you'll get banned from Reddit!
# NOTE: First run on 2024-11-30 7pm
with DAG(
    dag_id='reddit_pipeline',
    default_args=ARGS,
    schedule_interval='0 20 * * 0', # every sundie, 8 pm
    max_active_runs=1,
    catchup=CATCHUP,
    start_date=datetime(2024, 11, 20)
) as dag:
    
    ### T1
    # I could've merged T1 and T2 but I chose division of labor over simplicity
    # ## THIS IS NOW ASSIMILATED INTO THE DOCKERFILE
    # Now as a cleaner
    init_prereq = BashOperator(
        task_id='dependencies',
        bash_command="""
            if ls /tmp/YARS/example/*.json 1> /dev/null 2>&1; then
                rm /tmp/YARS/example/*.json
            fi
            if ls /tmp/YARS/example/*.txt 1> /dev/null 2>&1; then
                rm /tmp/YARS/example/*.txt
            fi
        """
        # bash_command="""
        #     pip install -U git+https://github.com/boolYikes/ProxyBroker.git && \
        #     git clone https://github.com/datavorous/YARS.git
        # """
    )

    ### T2
    # stdout proxy using the shift operator
    # this changes from time to time so the txt should be removed afterwards
    # not using proxy anymore. use it as a wait-between-requests
    def procrastinate():
        time.sleep(5)
    get_proxy = PythonOperator(
        task_id='get_proxy',
        python_callable=procrastinate
    )
    # get_proxy = BashOperator(
    #     task_id='get_proxy',
    #     bash_command="""
    #         proxybroker find --types HTTP HTTPS --lvl High --countries US --strict -l 10 > /tmp/proxy.txt
    #     """
    # )
    
    ### T3
    # Read the output from T2,
    # parse it and return it to xcom
    # I won't be reusing this function so it's inside DAG.
    # Perhaps it's better to put it outside for readability?
    def proxybroker_parser():
        # proxybroker seems to be async. i'll find a better way
        # https://proxybroker.readthedocs.io/en/latest/
        # this has become a procrastinator 
        time.sleep(5)
        # proxies = []
        # with open('/tmp/proxy.txt', 'rt') as f:
        #     data = f.read()
        #     splt = data.split('\n')
        #     for line in splt:
        #         if line:
        #             # prot = 'https' if 'https' in line else 'http'
        #             addr = line.split()[5].split('>')[0]
        #             # proxies.append({prot: f'{prot}://{addr}'})
        #             proxies.append(addr)
        # return json.dumps(proxies)
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
    # NOTE: Set the parameter to 'year' for the first run and then change it to 'week'
    reddit_extraction = BashOperator(
        task_id='crawl_reddit',
        bash_command=(
            """
            cd /tmp/YARS && \
            python /tmp/YARS/src/crawl_with_proxy.py week
            """
        )
    )

    ### T5
    # get the output json file, transform (need to merge them into one list, to be used in a insert query)
    # this can be improved with spark by avoiding handling large things directly
    # SO NOTHING WORKED EXCEPT FOR THE REGEX METHOD. THAT WAS A HASSLE
    def process_reddit_data(**context):
        """
        Transformation
        """
        basenames = list(filter(lambda x: '.json' in x, os.listdir('/tmp/YARS/example/')))
        payload = []

        for basename in basenames:
            fpath = os.path.join('/tmp/YARS/example/', basename)
            with open(fpath, 'r') as file:
                data = json.load(file)
                if isinstance(data, list):
                    for row in data:
                        payload.append(f"""(
                                    '{re.sub(r"[^a-zA-Z0-9\s]", "", row["title"])[:256]}', 
                                    '{re.sub(r"[^a-zA-Z0-9\s]", "", row["body"])[:256] if row["body"] else "null"}', 
                                    {row["score"]}, 
                                    {row["num_comments"]},
                                    '{datetime.fromtimestamp(row["created_utc"]).strftime("%Y-%m-%d %H:%M:%S")}',
                                    '{row["thumbnail_link"] if row["thumbnail_link"] else "null"}',
                                    '{row["category"] if row["category"] else "null"}',
                                    {row["up_votes"]},
                                    {row["up_ratio"]},
                                    '{row["subreddit"]}',
                                    '{row["author"]}',
                                    '{row["link"]}',
                                    '{row["search_key"]}'
                        )""")
        # apparently i can't just return a complex object to xcom
        context['ti'].xcom_push(key='payload_t5', value=json.dumps(payload))

    reddit_transformation = PythonOperator(
        task_id='reddit_transformation',
        python_callable=process_reddit_data,
        provide_context=True
    )

    ### T6
    # Loath it up
    def load_to_redshift(schema, table, **context):
        """
        Takes schema and table as arguments and loads transformed data to redshift dw
        """
        cur = get_redshift_connection('redshift_conn_id', True)
        ti = context['ti']
        transformed = json.loads(ti.xcom_pull(key='payload_t5'))
        
        ## for testing
        # import ast
        # transformed = ast.literal_eval(transformed)
        with open("/tmp/YARS/example/result_test.txt", "w") as file:
            json.dump(transformed, file, indent=4)
        
        init_table(cur, schema, table)

        try:
            cur.execute("BEGIN;")
            # title, author, created_utc, num_comments, score, permalink, image_url, thumbnail_url
            query = f"""INSERT 
                        INTO t (title, content, score, num_comments, created_utc, thumbnail_link, category, up_votes, up_ratio, subreddit, author, link, search_key) 
                        VALUES 
                        """ + ",".join(transformed)
            cur.execute(query)
            cur.execute("COMMIT;")
        except Exception as e:
            logging.basicConfig(
                filename="/tmp/YARS/example/whattheactualheck.txt",
                level=logging.INFO
            )
            logging.error(e)
            cur.execute("ROLLBACK;")
            raise

        # incremental 
        # created_utc alone is not unique in this dataset so + author
        # it seems inefficient on this dataset
        try:
            query = f"""
                DELETE FROM {schema}.{table};
                INSERT INTO {schema}.{table}
                SELECT title, content, score, num_comments, created_utc, thumbnail_link, category, up_votes, up_ratio, subreddit, author, link, search_key
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY CONCAT(created_utc, author) ORDER BY collected_on DESC) seq
                    FROM t
                )
                WHERE seq = 1;
            """
            cur.execute(query)
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
            if ls /tmp/proxies.txt 1> /dev/null 2>&1; then
                rm /tmp/proxies.txt
            fi
        """)
    )

    init_prereq >> get_proxy >> parse_proxy >> reddit_extraction >> reddit_transformation >> load >> cleaner