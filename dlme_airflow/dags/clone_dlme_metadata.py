import logging, os
from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from harvester.aub import harvest

# from airflow.models import Variable
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.contrib.sensors.bash_sensor import BashSensor
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'clone_dlme_metadata',
    default_args=default_args,
    description='Clone the dlme-metadata repository into the container',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['metadata'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='clone_dlme_metadata',
        bash_command='git clone --single-branch --branch main https://github.com/sul-dlss/dlme-metadata.git /opt/airflow/output'
    )
