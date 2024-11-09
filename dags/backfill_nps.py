from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import psycopg2

"""
DAAAAAAAAAAAAAG
"""

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
LPATH = './intermed.csv'

with DAG( # catchup is not set since we're just testing out
    'mysql_to_redshift_backfill',
    default_args=default_args,
    description='Backfill data from MySQL to Redshift with dedupe',
    schedule='@daily',
    start_date=days_ago(1),
) as dag:
    
    @task
    def extract_data(execution_date=None):
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
        # the pseudo-bulk insert takes long so we're testing with a chunk of the dataset.
        sql = f"""SELECT * FROM prod.nps WHERE DATE(created_at) <= DATE('{execution_date}') LIMIT 100""" # if equals, only execute out-todays...i think
        df = mysql_hook.get_pandas_df(sql)
        df.to_csv(LPATH, index=False) # header is needed for iterrows to read col names. 

    @task
    def create_redshift_table():
        redshift_hook = PostgresHook(postgres_conn_id='redshift_dev_db')
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS tunacome.nps (
                id INT,
                created_at TIMESTAMP,
                score INT
            );
        """
        redshift_hook.run(create_table_sql)

    @task
    def create_staging_table(): # this is like a persisting temp table with idempotency as a bonus
        redshift_hook = PostgresHook(postgres_conn_id='redshift_dev_db')
        redshift_hook.run("CREATE TABLE IF NOT EXISTS tunacome.nps_staging (id INT, created_at TIMESTAMP, score INT);")
        redshift_hook.run("DELETE FROM tunacome.nps_staging;")
        create_staging_sql = """
            INSERT INTO tunacome.nps_staging
            SELECT * FROM tunacome.nps;
        """
        redshift_hook.run(create_staging_sql)

    @task # Apparently, copy bulk insert is only allowed from s3 and emr and dynamo. That sucks
    def bulk_insert_csv():
        try:
            df = pd.read_csv(LPATH)
            redshift_hook = PostgresHook(postgres_conn_id='redshift_dev_db')
            conn = redshift_hook.get_conn()
            cur = conn.cursor()

            for _, row in df.iterrows(): # this is super slow for 5M rows
                insert_sql = f"""
                    INSERT INTO tunacome.nps_staging (id, created_at, score)
                    VALUES ({row['id']}, '{row['created_at']}', {row['score']})
                """
                cur.execute(insert_sql)
            conn.commit()
        except Exception as e:
            print(f'Nope : {e}')
            if conn:
                conn.rollback()
            raise
        finally: # the closing orders matter
            if cur: cur.close()
            if conn: conn.close()

    @task # iu
    def dedupe_and_merge():
        redshift_hook = PostgresHook(postgres_conn_id='redshift_dev_db')
        dedupe_sql = """
            DELETE FROM tunacome.nps;
            INSERT INTO tunacome.nps
            SELECT id, created_at, score FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_at DESC) AS ord
                FROM tunacome.nps_staging
            )
            WHERE ord = 1;
        """
        redshift_hook.run(dedupe_sql)

    @task
    def cleanup():
        # potential cleanup crew
        pass

    extract_data() >> create_redshift_table() >> create_staging_table() >> bulk_insert_csv() >> dedupe_and_merge() >> cleanup()