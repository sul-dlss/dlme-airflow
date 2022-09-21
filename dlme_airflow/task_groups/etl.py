import os
import logging

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

from dlme_airflow.tasks.harvest import build_harvester_task
from dlme_airflow.tasks.post_harvest import build_post_harvest_task
from dlme_airflow.tasks.transform import build_transform_task
from dlme_airflow.tasks.index import index_task
from dlme_airflow.tasks.harvest_report import build_harvest_report_task
from dlme_airflow.tasks.harvest_validator import build_validate_harvest_task
from dlme_airflow.tasks.send_harvest_report import build_send_harvest_report_task
from dlme_airflow.task_groups.validate_dlme_metadata import (
    build_sync_metadata_taskgroup,
)


def etl_tasks(provider, dag: DAG) -> list[TaskGroup]:
    task_array = []
    for collection in provider.collections:
        task_array.append(build_collection_etl_taskgroup(collection, dag))

    return task_array


def build_provider_etl_taskgroup(provider, dag: DAG) -> TaskGroup:
    with TaskGroup(
        group_id=f"{provider.name.upper()}_ETL", dag=dag
    ) as provider_etl_taskgroup:
        etl_tasks(provider, dag)

    return provider_etl_taskgroup


def build_collection_etl_taskgroup(collection, dag: DAG) -> TaskGroup:
    post_harvest = collection.catalog.metadata.get("post_harvest", None)

    with TaskGroup(
        group_id=f"{collection.name}_etl", dag=dag
    ) as collection_etl_taskgroup:
        harvest = build_harvester_task(collection, collection_etl_taskgroup, dag)
        sync = build_sync_metadata_taskgroup(collection, dag)
        transform = build_transform_task(collection, collection_etl_taskgroup, dag)
        index = index_task(collection, collection_etl_taskgroup, dag)

        etl_complete = DummyOperator(task_id="etl_complete", trigger_rule="none_failed")
        skip_load_data = DummyOperator(task_id="skip_load_data", trigger_rule="none_failed")
        load_data = DummyOperator(task_id="load_data", trigger_rule="none_failed")
        validate_harvest_task = build_validate_harvest_task(collection, collection_etl_taskgroup, dag)
        # validate_harvest_task = BranchPythonOperator(
        #     task_id="validate_harvest",
        #     task_group=collection_etl_taskgroup,
        #     python_callable=validate_harvest,
        #     op_kwargs = {
        #         "collection": collection
        #     }
        # )

        # harvest and sync with an optional post_harvest_task if catalog metadata wants it
        if post_harvest:
            logging.info(f"adding post harvest task for {collection.label()}")
            post_harvest_task = build_post_harvest_task(
                collection, collection_etl_taskgroup, dag
            )
            harvest >> validate_harvest_task >> [load_data, skip_load_data]
            load_data >> post_harvest_task >> sync
        else:
            harvest >> validate_harvest_task >> [load_data, skip_load_data]
            load_data >> sync

        # common tasks
        sync >> transform >> index
        skip_load_data >> etl_complete

        # add report unless the environment says not to
        if not os.getenv("SKIP_REPORT"):
            report = build_harvest_report_task(
                collection, collection_etl_taskgroup, dag
            )
            send_report = build_send_harvest_report_task(
                collection, collection_etl_taskgroup, dag
            )
            index >> report >> send_report >> etl_complete
        else:
            index >> etl_complete
            logging.info("skipping report generation in etl tasks")

    return collection_etl_taskgroup
