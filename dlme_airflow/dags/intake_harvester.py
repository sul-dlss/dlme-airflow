from airflow import DAG
from datetime import datetime, timedelta

from harvester.csv import csv_harvester

from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provider": None,
}

with DAG(
    "intake-harvester",
    default_args=default_args,
    description="Intake Harvester DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 8, 17),
    tags=["csv"],
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id="csv_harvester",
        python_callable=csv_harvester,
        op_kwargs={"provider": "{{ dag_run.conf['provider'] }}"},
    )


t1
