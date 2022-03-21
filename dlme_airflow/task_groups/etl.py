# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.utils.task_group import TaskGroup

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
    try:
        collections = iter(list(source))
        for collection in collections:
            task_array.append(
                build_collection_etl_taskgroup(provider, collection, task_group, dag)
            )
    except TypeError:
        return build_collection_etl_taskgroup(provider, None, task_group, dag)

    return task_array


def build_provider_etl_taskgroup(provider, dag: DAG) -> TaskGroup:

    with TaskGroup(
        group_id=f"{provider.upper()}_ETL", dag=dag
    ) as provider_etl_taskgroup:
        etl_tasks(provider, provider_etl_taskgroup, dag)

    return provider_etl_taskgroup


def build_collection_etl_taskgroup(
    provider, collection, task_group: TaskGroup, dag: DAG
) -> TaskGroup:
    catalog_key = provider
    if collection:
        catalog_key = f"{catalog_key}.{collection}"

    # Move fetching of post harvest from metadata into the task group
    source = catalog_for_provider(catalog_key)
    post_harvest = source.metadata.get("post_harvest", None)
    data_path = source.metadata.get("data_path", catalog_key)

    with TaskGroup(group_id=f"{collection}_etl", dag=dag) as collection_etl_taskgroup:
        harvest = build_havester_task(
            provider, collection, collection_etl_taskgroup, dag
        )  # Harvest
        # sync = build_sync_metadata_taskgroup(provider, collection, dag)
        transform = build_transform_task(
            provider, collection, data_path, collection_etl_taskgroup, dag
        )  # Transform
        load = index_task(
            provider, collection, collection_etl_taskgroup, dag
        )  # Load / Index
        # report = build_harvest_report_task(
        #     provider, collection, collection_etl_taskgroup, dag
        # )  # Report
        # send_report = build_send_harvest_report_task(
        #     provider, collection, collection_etl_taskgroup, dag
        # )  # Send Report

        # If we fetch a post_harvest key from the catalog, build the post_harvest_task and include it in the flow
        if post_harvest:
            post_harvest_task = build_post_havest_task(
                provider, collection, post_harvest, collection_etl_taskgroup, dag
            )  # Post Harvest
            (harvest >> post_harvest_task >> transform >> load >> report >> send_report)
        else:
            # Else do not build a post_harvest task for this provider/collection
            harvest >> transform >> load  # >> report >> send_report

    return collection_etl_taskgroup
