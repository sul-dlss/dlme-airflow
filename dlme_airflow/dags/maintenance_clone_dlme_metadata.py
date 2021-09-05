from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.models import Variable

# Our stuff
from task_groups.validate_dlme_metadata import build_validate_metadata_taskgroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [Variable.get("data_manager_email")],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'catchup': False,
}

with DAG(
    'maintenance.clone_dlme_metadata',
    default_args=default_args,
    description='Clone DLME-metadata into containers',
    schedule_interval='@yearly',
    start_date=datetime(2021, 8, 26),
    tags=['metadata'],
    catchup=False
) as dag:

    validate_dlme_metadata = build_validate_metadata_taskgroup(dag=dag)
