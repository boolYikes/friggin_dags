from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import yfinance as yf
import logging

def get_Redshift_connection(autocommit=False): # false by default but i love them explicit
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    logging.info(f'\rDB connected.')
    return conn.cursor()


@task
def get_historical_prices(symbol):
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d %H:%M:%S')
        records.append(f"('{date}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Volume']})")
        logging.info(f'\rExtracting...)')
    logging.info(f'\rExtraction complete.')
    return records


def _upsert_ready(cur, schema, table):
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                date date,
                "open" float,
                high float,
                low float,
                close float,
                volume bigint,
                created_on timestamp default GETDATE()
            );"""
        )
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        cur.execute("COMMIT;")
        logging.info(f'\rTables ready.')
    except Exception as e:
        logging.error(f'Reflect upon your foolish mistake :P {e}')
        cur.execute("ROLLBACK;")
        raise

"""the thang :
copy original to temp table -> 
insert into the temp table (union) -> 
empty the original ->
insert only the rownum 1s from the temp table to the original ->
done n done
OH AND ONE MORE THING: BEGIN IS IMPLICIT IN NONE-AUTOCOMMIT MODE
"""
@task
def load(schema, table, records):
    logging.info(f'\rLoading started.')
    cur = get_Redshift_connection()
    # prep the tables
    _upsert_ready(cur, schema, table) # this is already in a transaction block
    try: # insert to temp 
        sql = f"""INSERT INTO t (date, "open", high, low, close, volume) VALUES """ + ",".join(records)
        cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
        logging.info(f'\rTemp table populated.')
    except Exception as e:
        print(e)
        cur.execute("ROLLBACK;")
        raise

    try: # clean up original, incremental update.. 
        iu_query = f"""
        DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table}
        SELECT date, "open", high, low, close, volume FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_on DESC) seq
            FROM t
        )
        WHERE seq = 1;
        """
        cur.execute(iu_query)
        cur.execute("COMMIT;")   # cur.execute("END;")
        logging.info(f'\rLoading complete.')
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise

with DAG(
    dag_id='UpdateSymbol_v3',
    start_date=datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    max_active_runs=1,
    schedule='10 18 * * *',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:

    results = get_historical_prices("AAPL")
    load("tunacome", "stock_info_v3", results)
