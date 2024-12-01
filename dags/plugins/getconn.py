from airflow.providers.postgres.hooks.postgres import PostgresHook

# Helper functions
def get_redshift_connection(conn_id, autocommit=True):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()