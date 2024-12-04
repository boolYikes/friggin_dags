# This DAG will demonstrate redering DB contents to google sheets
# 😁
from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.getconn import get_redshift_connection
from plugins.facepalm import send_slack_notification

from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import gspread
import logging


# Constants
ARGS = {
    'owner': 'airflow',
    'trigger_rule': 'all_success',
    'description':'Renders DB to google sheet.',
    'retries': 0,
}


# Helper functions
def success_callback(context):
    send_slack_notification(context, status="success")
    
def failure_callback(context):
    send_slack_notification(context, status="failed")


# Tasks
### T1
# Fetches the reddit analysis table
def fetch_analysis(schema, table, **ctxt):
    cur = get_redshift_connection('redshift_conn_id')
    ti = ctxt['ti']
    try:
        cur.execute("BEGIN;")
        cur.execute(f"SELECT * FROM {schema}.{table}")
        result = cur.fetchall()
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise
    ti.xcom_push(value=result, key='reddit')

### T2
# Fetches the commodities table
def fetch_commodities(schema, table, **ctxt):
    ti = ctxt['ti']
    try:
        cur = get_redshift_connection('redshift_conn_id')
        cur.execute("BEGIN;")
        cur.execute(f"SELECT * FROM {schema}.{table}")
        result = cur.fetchall()
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise
    ti.xcom_push(value=result, key='commodities')

### T3
# Render on google sheet
def write_to_google_sheet(**ctxt):
    ti = ctxt['ti']

    # G sheets api setup
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('/tmp/service_key.json', scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key("1X0oq7zvDNVSUPt9RjfO_BgSShJXNdwO5Z29a_JcziAM")

    ## Sheet manipulation
    # Analytics table on sheet 1
    reddit = ti.xcom_pull(task_ids='fetch_reddit', key='reddit')
    sheet = spreadsheet.sheet1 # the short hand for the first sheet, apprntly
    sheet.clear()
    # Add headers
    headers = ['date', 'search key', 'likes', 'comments', 'count', 'sentiment']
    sheet.append_row(headers)
    # add rows from the fetched data: bulk inserting minimizes request numbers (60 req per min per user)
    sheet.append_rows(list(map(lambda x: (datetime.strftime(x[0], '%Y-%m-%d'), x[1], x[2], x[3], x[4], x[5]), reddit)))

    # Repeat with the commodities table, on sheet 2
    commodities = ti.xcom_pull(task_ids='fetch_commodities', key='commodities')
    sheet = spreadsheet.get_worksheet_by_id('777879687')
    sheet.clear()
    # Add headers
    headers = ['date', 'name', 'open_value', 'high_value', 'low_value', 'close_value', 'volume']
    sheet.append_row(headers)
    # add rows from the fetched data
    sheet.append_rows(list(map(lambda x: (datetime.strftime(x[0], '%Y-%m-%d'), x[1], x[2], x[3], x[4], x[5], x[6]), commodities)))


# DAG
with DAG(
    tags=['Sheet', 'Visualization'],
    dag_id='to_google_sheet',
    default_args=ARGS,
    schedule_interval=None, # every sundie, 10 pm
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2024, 11, 20), # Full refresh. so it does not matter
    on_success_callback=success_callback,
    on_failure_callback=failure_callback
) as dag:
    
    t1 = PythonOperator(
        task_id='fetch_reddit',
        python_callable=fetch_analysis,
        provide_context=True,
        op_args=['tunacome', 'analytics_reddit_summary']
    )

    t2 = PythonOperator(
        task_id='fetch_commodities',
        python_callable=fetch_commodities,
        provide_context=True,
        op_args=['tunacome', 'afdbt_commodities']
    )

    t3 = PythonOperator(
        task_id='render_table',
        python_callable=write_to_google_sheet,
        provide_context=True
    )

    (t1 >> t2) >> t3