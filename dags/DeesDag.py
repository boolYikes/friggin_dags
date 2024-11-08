from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime, timedelta
import requests, logging, psycopg2

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract(url):
    logging.info('EXTRACTION COMPLETE.')
    return requests.get(url).json()

# official name, population, area
# every SAT 06:30 UTC
# make a repo

@task
def transform(json):
    countries = []
    for o in json:
        name, pop, area = o['name']['official'].replace("'", ""), o['population'], o['area']
        countries.append([name, pop, area])
    logging.info('TRANSFORM COMPLETE')
    return countries

@task
def load(schema, table, countries):
    cur = get_Redshift_connection()
    try:
        cur.execute('BEGIN;')
        cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    name varchar(50),
                    pop integer,
                    area float
                    );
                    """)
        cur.execute(f'DELETE FROM {schema}.{table};')
        for c in countries:
            name, pop, area = c[0], c[1], c[2]
            sql = f"INSERT INTO {schema}.{table} VALUES ('{name}', '{pop}', '{area}')"
            cur.execute(sql)
        cur.execute('COMMIT;')
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
        cur.execute('ROLLBACK;')
    logging.info('LOADING COMPLETE')

with DAG(
    dag_id='countryman_dee',
    start_date=datetime(2024, 1, 1),
    schedule='30 6 * * 6',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }
) as dag:
    url = Variable.get('countries_url')
    schema = 'tunacome'
    table = 'countries_basic_info'
    countries = transform(extract(url))
    load(schema, table, countries)
# no need to specify work order!