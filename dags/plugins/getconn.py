from airflow.providers.postgres.hooks.postgres import PostgresHook
import json


# Helper function    
def get_redshift_connection(conn_id, autocommit=True):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def get_hooked(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    payload = json.dumps({
        'user': conn.login,
        'host': conn.host,
        'password': conn.password,
        'dbname': conn.schema,
        'schema': conn.login,
        'port': conn.port
        })
    return payload