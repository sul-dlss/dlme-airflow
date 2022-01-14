from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

# Our stuff
from task_groups.validate_dlme_metadata import build_validate_metadata_taskgroup, build_sync_metadata_taskgroup
from task_groups.harvest import build_harvester_taskgroup
from task_groups.post_harvest import build_post_havest_taskgroup
from task_groups.detect_metadata_changes import build_detect_metadata_changes_taskgroup
from task_groups.transform import build_transform_taskgroup
from task_groups.index import build_index_taskgroup

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

        harvester = build_harvester_taskgroup(provider, dag)

        # A dummy operator is required as a transition point between task groups
        harvest_complete = DummyOperator(task_id='harvest_complete', trigger_rule='none_failed', dag=dag)
        transform_complete = DummyOperator(task_id='transform_complete', trigger_rule='none_failed', dag=dag)

        # TODO
        # post_harvest = build_post_havest_taskgroup(provider, dag)

        # TODO: A dummy operator is required as a transition point between task groups
        # post_harvest_complete = DummyOperator(task_id='post_harvest_complete', trigger_rule='none_failed', dag=dag)

        sync_dlme_metadata = build_sync_metadata_taskgroup(provider, dag)
        # TODO
        # collect_metadata_changes = build_detect_metadata_changes_taskgroup(provider, dag)

        transform = build_transform_taskgroup(provider, dag)
        index = build_index_taskgroup(provider, dag)
        # validate_dlme_metadata >> harvester >> harvest_complete >> post_harvest >> post_harvest_complete >> collect_metadata_changes
        # validate_dlme_metadata >> harvester >> harvest_complete >> collect_metadata_changes
        validate_dlme_metadata >> harvester >> harvest_complete >> sync_dlme_metadata >> transform >> transform_complete >> index

    return dag
