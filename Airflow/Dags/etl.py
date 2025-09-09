from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os

DEFAULT_ARGS = {
    'owner': 'madhav',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# adjust paths according to how you mount into the container
ETL_PY = '/opt/etl'
CSV_DIR = '/opt/data'
SQL_DIR = '/opt/etl/../sql'  # if mounted adjust; or use /opt/etl/sql

with DAG(
    dag_id='madhav_elt_dag',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1_load = BashOperator(
        task_id='load_raw',
        bash_command=f'python {ETL_PY}/load_data.py --csv-dir {CSV_DIR} --pg-host db --pg-port 5432 --pg-db postgres --pg-user postgres --pg-pass postgres',
    )

    t2_dq = BashOperator(
        task_id='run_sql_dq',
        bash_command=f'python {ETL_PY}/run_sql_dq.py --pg-host db --pg-port 5432 --pg-db postgres --pg-user postgres --pg-pass postgres --sql-path /opt/etl/../sql/dq_checks.sql',
    )

    t3_transform = BashOperator(
        task_id='transform',
        bash_command=f'python {ETL_PY}/transform.py --pg-host db --pg-port 5432 --pg-db postgres --pg-user postgres --pg-pass postgres',
    )

    # optional: run any final SQL to create views
    t4_finalize = BashOperator(
        task_id='finalize_views',
        bash_command=f'psql -h db -U postgres -d postgres -c \"\\i /opt/etl/../sql/final_views.sql\"',
        env={'PGPASSWORD': 'postgres'}
    )

    t1_load >> t2_dq >> t3_transform >> t4_finalize
