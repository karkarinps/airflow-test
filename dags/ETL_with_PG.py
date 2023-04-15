from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd


extr_path = "F://Git/airflow-test/dags/Orders.csv"
dest_path = "F://Git/airflow-test/dags/orders_1.csv"


def post_table_create():
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""create table if not exists orders (
                order_id VARCHAR,
                date date,
                product_name VARCHAR,
                quantity INT
            )
        """)
    cursor.close()
    conn.close()

def extract_pand_trans():
    df = pd.read_csv(extr_path, delimiter=',')
    df['date'] = df['date'].apply(lambda x: pd.to_datetime(x))
    df_1 = df.drop_duplicates()
    df_1.to_csv(dest_path, index=False)


def write_to_pg():
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    hook.bulk_load(table='orders', tmp_file=dest_path)


default_args = {
    'owner': 'me',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


ingestion_dag = DAG(
    'ETL_PG',
    default_args=default_args,
    description='Second ETL with PG and Pandas',
    start_date=days_ago(2),
    schedule_interval='0 0 * * *',
    catchup=False
)

task_1 = PythonOperator(
    task_id='CreateTable',
    python_callable=post_table_create,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='ExtrTransSave',
    python_callable=extract_pand_trans,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id='LoadToDB',
    python_callable=write_to_pg,
    dag=ingestion_dag,
)

task_1 >> task_2 >> task_3