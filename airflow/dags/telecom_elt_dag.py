from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.abspath("/opt/airflow/scripts"))

from extract_load import extract_and_load_to_staging
from transform import transform_and_load_to_warehouse

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'telecom_elt_pipeline',
        default_args=default_args,
        description='Hourly ELT pipeline for telecom customer churn data',
        schedule_interval='@hourly',  # Configurable: change to '@daily', '0 * * * *', etc.
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['elt', 'telecom', 'pii-anonymization'],
) as dag:

    extract_load_task = PythonOperator(
        task_id='extract_and_load_to_staging',
        python_callable=extract_and_load_to_staging,
        op_kwargs={
            'source_url': 'https://github.com/punitvnsit03/hginsight-telecom-elt-pipeline/blob/main/data/raw/customer_churn.csv',
            'local_path': '/opt/airflow/data/raw/customer_churn.csv'
        }
    )

    transform_task = PythonOperator(
        task_id='transform_and_load_to_warehouse',
        python_callable=transform_and_load_to_warehouse,
    )

    extract_load_task >> transform_task
