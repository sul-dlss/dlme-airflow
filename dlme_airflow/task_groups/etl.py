# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

import logging

# Operators and utils required from airflow
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from utils.catalog import catalog_for_provider
from tasks.harvest import build_havester_task
from tasks.post_harvest import build_post_havest_task
from tasks.transform import build_transform_task
from tasks.index import index_task
from tasks.harvest_report import build_harvest_report_task
from tasks.send_harvest_report import build_send_harvest_report_task
from task_groups.validate_dlme_metadata import build_sync_metadata_taskgroup

def etl_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_array = []
    source = catalog_for_provider(provider)
    logging.info(f'The source is {source}')
    try:
        collections = iter(list(source))
        for collection in collections:
            task_array.append(build_collection_etl_taskgroup(provider, collection, "/opt/dlme_airflow/utils/ifpo_get_thumbnail_urls.py", task_group, dag))
    except TypeError:
        return build_collection_etl_taskgroup(provider, None, "/opt/dlme_airflow/utils/ifpo_get_thumbnail_urls.py", task_group, dag)

    return task_array


def build_provider_etl_taskgroup(provider, dag: DAG) -> TaskGroup:

    with TaskGroup(group_id=f"{provider.upper()}_ETL", dag=dag) as provider_etl_taskgroup:
      etl_tasks(provider, provider_etl_taskgroup, dag)

    return provider_etl_taskgroup


def build_collection_etl_taskgroup(provider, collection, post_harvest, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    with TaskGroup(
        group_id=f"{collection}_etl",
        dag=dag) as collection_etl_taskgroup:
            harvest = build_havester_task(provider, collection, collection_etl_taskgroup, dag)  # Harvest
            post_harvest = build_post_havest_task(provider, collection, post_harvest, collection_etl_taskgroup, dag) # Post Harvest
            sync = build_sync_metadata_taskgroup(provider, collection, dag)
            transform = build_transform_task(provider, collection, collection, collection_etl_taskgroup, dag)  # Transform
            load = index_task(provider, collection, collection_etl_taskgroup, dag)  # Load / Index
            report = build_harvest_report_task(provider, collection, collection_etl_taskgroup, dag)  # Report
            send_report = build_send_harvest_report_task(provider, collection, collection_etl_taskgroup, dag)  # Send Report

            harvest >> post_harvest >> sync >> transform >> load >> report >> send_report

    return collection_etl_taskgroup
