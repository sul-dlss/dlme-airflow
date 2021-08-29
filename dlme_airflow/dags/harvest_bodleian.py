import intake
import logging
import os
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.contrib.sensors.bash_sensor import BashSensor
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Our stuff
from task_groups.validate_dlme_metadata import build_validate_metadata_taskgroup
from task_groups.collect_metadata_changes import build_collect_metadata_changes_taskgroup
from task_groups.transform_and_index import build_transform_and_index_taskgroup
from harvester.source_harvester import data_source_harvester

from drivers.iiif_json import IIIfJsonSource

intake.source.register_driver("iiif_json", IIIfJsonSource)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [Variable.get("data_manager_email")],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    'catchup': False,
}

provider = 'bodleian'
home_directory = os.environ['AIRFLOW_HOME']
metadata_directory = f"{home_directory}/metadata/{provider}/"
working_directory = f"{home_directory}/working/{provider}/"

def new_metadata(**kwargs):
    task = kwargs['ti']
    changes = int(task.xcom_pull(task_ids='bodleian_record_changes'))
    deletes = int(task.xcom_pull(task_ids='bodleian_record_deletes'))
    additions = int(task.xcom_pull(task_ids='bodleian_record_additions'))
    total_changes = changes + deletes + additions
    
    if total_changes > 0:
        print(f"{total_changes}")
        return 'push_metadata_changes'
    
    return 'no_metadata_changes'


with DAG(
    f'harvest.{provider}',
    default_args=default_args,
    description=f'IIIF harvester for the {provider} collection(s)',
    schedule_interval='@yearly',
    start_date=datetime(2021, 8, 26),
    tags=['metadata', 'iiif'],
    catchup=False
) as dag:

    validate_dlme_metadata = build_validate_metadata_taskgroup(dag=dag)

    bodleian_harvester = PythonOperator(
        task_id="bodleian_intake_harvester",
        python_callable=data_source_harvester,
        op_kwargs={"provider": f"{provider}"},
    )

    # collect_metadata_changes = build_collect_metadata_changes_taskgroup(provider, dag=dag)

    # transform_and_index = build_transform_and_index_taskgroup(provider, dag=dag)
    
    validate_dlme_metadata >> bodleian_harvester # >> collect_metadata_changes # >> transform_and_index
