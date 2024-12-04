# TODO: Account for holidays (calendar api needed)
from airflow import DAG
from airflow.decorators import task

from plugins.getconn import get_redshift_connection
from plugins.facepalm import send_slack_notification

from datetime import datetime, timedelta
import yfinance as yf
import logging


# Constants
CATCHUP = True
ARGS = {
    'owner': 'airflow',
    'trigger_rule': 'all_success',
    'description':'Extract commodity prices and load to Redshift',
    'retries': 0
}
SYMBOLS = { # get open and volumes.
    "Oil": "CL=F",
    "Natrual Gas": "NG=F",
    "Gold": "GC=F",
    "Silver": "SI=F",
    "Copper": "HG=F",  
    "Gasoline": "RB=F",   
    "Diesel": "HO=F",
    "Corn": "ZC=F",
    "Coffee": "KC=F",
    "Sugar": "SB=F"
}


# Helper functions
def success_callback(context):
    send_slack_notification(context, status="success")

def failure_callback(context):
    send_slack_notification(context, status="failed")

def init_table(cur : object, schema : str, table : str, catchup : bool):
    if not catchup:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            date date,
            name varchar(20),
            open_value float,
            high_value float,
            low_value float,
            close_value float,
            volume bigint
        );
    """)

# Tasks
@task
def extract_commodity_data(start: str, end: str):
    """
    The first task that extracts data from the api.
    """
    extracted = []
    for name, symbol in SYMBOLS.items():
        ticket = yf.Ticker(symbol)
        data = ticket.history(start=start, end=end)
        for index, row in data.iterrows():
            extracted.append([
                index.strftime('%Y-%m-%d'),
                name,
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                row['Volume'],
            ])
    return extracted

@task
def load_to_redshift(schema : str, table : str, extracted : list):
    """
    The second task that loads data up to RS
    """
    cur = get_redshift_connection('redshift_conn_id', True)
    try:
        cur.execute("BEGIN;")
        init_table(cur, schema, table, CATCHUP)
        # extracted = ast.literal_eval(extracted) # not needed as I don't use xcom anymore
        for e in extracted:  # date, name, open, high, low, close, vol
            # the query is tripping me up
            query = f"""
                INSERT INTO {schema}.{table}
                SELECT '{e[0]}', '{e[1]}', ROUND({e[2]}, 2), ROUND({e[3]}, 2), ROUND({e[4]}, 2), ROUND({e[5]}, 2), {e[6]}
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}.{table}
                    WHERE name = '{e[1]}' AND date = '{e[0]}'
                )
            """
            cur.execute(query)
            d = datetime.strptime(e[0], "%Y-%m-%d")
            # if current exc day(actual, not logical) of the week is Friday,
            if d.weekday() == 4:
                # insert null fillers for that Saturday and Sunday
                query = f"""
                    INSERT INTO {schema}.{table}
                    VALUES ('{d + timedelta(days=1)}', '{e[1]}', null, null, null, null, null),
                    ('{d + timedelta(days=2)}', '{e[1]}', null, null, null, null, null)
                """
                cur.execute(query)
        cur.execute("COMMIT;")
    except Exception as e:
        print(e)
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise

# DAG
with DAG(
    dag_id='commidty_pipeline',
    default_args=ARGS,
    schedule_interval='0 15 * * 0', # every sundie, 3 pm
    max_active_runs=1,
    catchup=CATCHUP,
    start_date=datetime(2023, 1, 1), # change this on actual run
    on_success_callback=success_callback,
    on_failure_callback=failure_callback
) as dag:   

    # T1
    # Add 7 days from every execution point and that gives you the time frame
    # I think reducing 7 to certain number can help avoid some dupe records
    extracted = extract_commodity_data(
        start="{{ ds }}",
        end="{{ (execution_date + macros.timedelta(days=7)).strftime('%Y-%m-%d') }}"
    )

    # T2
    # I don't have to use xcom, that is a relief
    load_to_redshift("tunacome", "afdbt_commodities", extracted)
