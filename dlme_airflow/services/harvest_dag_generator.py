from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.dummy import DummyOperator

# Our stuff
from task_groups.etl import build_provider_etl_taskgroup


def create_dag(provider, default_args):
    dag = DAG(
        provider.name,
        default_args=default_args,
        schedule_interval="@once",
        start_date=datetime(2021, 9, 6),
    )

    with dag:
        # A dummy operator is required as a transition point between task groups
        harvest_begin = DummyOperator(
            task_id="harvest_begin", trigger_rule="none_failed", dag=dag
        )
        harvest_complete = DummyOperator(
            task_id="harvest_complete", trigger_rule="none_failed", dag=dag
        )

        # TODO
        # post_harvest_begin = DummyOperator(task_id='post_harvest_begin', trigger_rule='none_failed', dag=dag)
        # post_harvest = build_post_havest_taskgroup(provider, dag)
        # post_harvest_complete = DummyOperator(task_id='post_harvest_complete', trigger_rule='none_failed', dag=dag)

        # TODO: A dummy operator is required as a transition point between task groups
        # post_harvest_complete = DummyOperator(task_id='post_harvest_complete', trigger_rule='none_failed', dag=dag)

        etl = build_provider_etl_taskgroup(provider, dag)

        harvest_begin >> etl >> harvest_complete

    return dag
