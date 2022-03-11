from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

# Our stuff
from task_groups.validate_dlme_metadata import build_validate_metadata_taskgroup
from task_groups.etl import build_provider_etl_taskgroup
from utils.driver_tag import fetch_driver

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


def create_dag(provider, default_args):
    dag = DAG(
        provider,
        default_args=default_args,
        schedule_interval='@once',
        start_date=datetime(2021, 9, 6),
        tags=fetch_driver(provider)
    )

    with dag:
        validate_dlme_metadata = build_validate_metadata_taskgroup(provider, dag)

        # A dummy operator is required as a transition point between task groups
        harvest_begin = DummyOperator(task_id='harvest_begin', trigger_rule='none_failed', dag=dag)
        harvest_complete = DummyOperator(task_id='harvest_complete', trigger_rule='none_failed', dag=dag)

        # TODO
        # post_harvest_begin = DummyOperator(task_id='post_harvest_begin', trigger_rule='none_failed', dag=dag)
        # post_harvest = build_post_havest_taskgroup(provider, dag)
        # post_harvest_complete = DummyOperator(task_id='post_harvest_complete', trigger_rule='none_failed', dag=dag)

        # TODO: A dummy operator is required as a transition point between task groups
        # post_harvest_complete = DummyOperator(task_id='post_harvest_complete', trigger_rule='none_failed', dag=dag)

        etl = build_provider_etl_taskgroup(provider, dag)

        validate_dlme_metadata >> harvest_begin >> etl >> harvest_complete

    return dag
