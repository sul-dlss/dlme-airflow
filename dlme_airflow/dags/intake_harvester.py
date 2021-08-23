import intake
from airflow import DAG
from datetime import datetime, timedelta

from harvester.source_harvester import data_source_harvester

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from drivers.iiif_json import IIIfJsonSource

intake.source.register_driver("iiif_json", IIIfJsonSource)

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
    tags=["csv", "json"],
    catchup=False,
) as dag:
    t1 = BashOperator(task_id="make_working_dir", bash_command="mkdir -p /opt/airflow/working")
    t2 = PythonOperator(
        task_id="intake_harvester",
        python_callable=data_source_harvester,
        op_kwargs={"provider": "{{ dag_run.conf['provider'] }}"},
    )


t1 >> t2
