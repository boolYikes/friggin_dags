# This DAG performs simple sentiment analyses by commencing title + content merging and feeding it to the model.
# This is assuming that title text also can be found in the content and duping text like this would add weights to the essence of the post.
# The next and final DAG will demonstrate sensor usage. (AFDBT_d3 -> AFDBT_d4)

# NOTE to self: is it efficient to put task decorators outside DAG or inside DAG?
# - and what of the cases where non-decorated tasks are placed in or outside DAG?
# - which case is prone to dupe execution?

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from plugins.getconn import get_redshift_connection
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from datetime import datetime, timedelta
import json, os, time, logging, re


# Constants
# CATCHUP is forced on this DAG
ARGS = {
    'owner': 'airflow',
    'trigger_rule': 'all_success',
    'description':'Merge Reddit titles and body content and classify semtiments',
    'retries': 0
}
MODEL_VERSION = 0.5
# Used in multiple tasks hence this. cuz it's a simpleton 😁
# Search suggests that it is not a safe approach
# This may lead to dupe sessions and transaction race conditions
# but It's fun ... 🙄
CURSOR = get_redshift_connection('redshift_conn_id')
SCHEMA = 'tunacome'
TABLE = 'afdbt_sentiment'


# Helper functions
def init_table(cur, schema, table):
    try:
        # uses title + created_utc as composite key
        # given id for updated inferrence model experiments later in dbt
        # getdate() is apparently server-specific date. alternative: current_timestamp
        # IDENTITY(1,1) is serial for redshift. how quaint
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                title text,
                content text,
                sentiment int default null,
                created_utc timestamp,
                inference_no bigint IDENTITY(1, 1),
                model_version text,
                inferred_on timestamp default GETDATE()
            )
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise


### T1
# Preparing for a full refresh
# Insert select columns to inference table, sift dupes by title + date composite
# Inference result defaults to null
# Old data will remain (not dropping table) for performance analysis
@task
def load_and_transfer(cur, schema, table):
    try:
        cur.execute("BEGIN;")
        init_table(cur, schema, table)
        cur.execute(f"""
            INSERT INTO {schema}.{table} (
                title, content, created_utc, model_version
            )
            SELECT red.title, red.content, red.created_utc, {MODEL_VERSION}
            FROM {schema}.afdbt_reddit red
            WHERE NOT EXISTS (
                SELECT 1
                FROM {schema}.{table} inf
                WHERE CONCAT(inf.title, inf.created_utc) = CONCAT(red.title, red.created_utc)
            );
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise
    
    # You have to send signal to the next task, if using decorator and no shift operator
    return "Yeet"

### T2
# Load up the table and infer
# honestly don't understand the necessity of loading it up again
# division of labor?
@task
def thinc(cur, schema, table, signal):
    try:
        cur.execute(f"""SELECT title, content, sentiment, inference_no FROM {schema}.{table}""")
        result = cur.fetchall()
        model = SentimentIntensityAnalyzer()
        
        # row[0] and row[1] will be title and content respectively
        inferred_records = []
        for row in result:
            # hasn't been inferred
            if row[2] == None:
                score = model.polarity_scores(row[0] + row[1])['compound']
                sentiment = 0
                if score >= 0.05: sentiment = 1
                elif score > -0.05: sentiment = 0
                else: sentiment = -1
                updated_row = (row[0], row[1], sentiment, row[3])
                inferred_records.append(updated_row)
            else:
                inferred_records.append(row)
    except Exception as e:
        logging.error(e)
        # it's only a getter
        # cur.execute("ROLLBACK;")
        raise

    return inferred_records
    
### T3
# Put the inferred result back in
@task
def in_you_go(cur, schema, table, inferred):
    try:
        for row in inferred: # sentiment is in row[2], id; row[3]
            cur.execute(f"""
                UPDATE {schema}.{table}
                SET sentiment = {row[2]}
                WHERE inference_no = {row[3]}
            """)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise


# D_irty A_vid G_eek
with DAG(
    dag_id='sentiment_pipeline',
    default_args=ARGS,
    schedule_interval='0 22 * * 0', # every sundie, 10 pm
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2024, 11, 20), # Full refresh. so it does not matter
) as dag:

    # T1
    groove = load_and_transfer(
        cur=CURSOR,
        schema=SCHEMA,
        table=TABLE
    )

    # T2
    inferred = thinc(
        cur=CURSOR,
        schema=SCHEMA,
        table=TABLE,
        signal=groove
    )

    # T3
    in_you_go(
        cur=CURSOR,
        schema=SCHEMA,
        table=TABLE,
        inferred=inferred
    )
