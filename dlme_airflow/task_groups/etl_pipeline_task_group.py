# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

import logging

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from utils.catalog import catalog_for_provider
from tasks.extract import extract
from tasks.compare import compare
from tasks.transform import transform
from tasks.load import load

# def etl_pipeline_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
def etl_pipeline(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_group_array = []
    source = catalog_for_provider(provider)
    try:
        collections = iter(list(source))
        for collection in collections:
            collection_catalog = catalog_for_provider(f"{provider}.{collection}")
            with TaskGroup(group_id=collection) as collection_tg:
                provider_collection = f"{provider}.{collection}"
                extract_task = extract(provider_collection, collection_tg, dag)
                compare_task = compare(provider, collection, collection_tg, dag)
                complete = DummyOperator(task_id='complete', task_group=collection_tg)
                transform_task = transform(provider, collection, collection_catalog.metadata.get("data_path"), collection_tg, dag) 
                load_task = load(provider_collection, collection_tg, dag)
                etl_complete = DummyOperator(task_id='etl_complete')

                extract_task >> compare_task
                compare_task >> [complete, transform_task]
                transform_task >> load_task >> etl_complete
                complete >> etl_complete

                task_group_array.append(collection_tg)
        
    except TypeError:
        return extract(provider, task_group, dag)

    return task_group_array


def build_etl_pipeline_taskgroup(provider, dag: DAG) -> TaskGroup:
    with TaskGroup(
        group_id=f"{provider}.etl.pipeline"
    ) as etl_pipeline_taskgroup:
        etl_pipelines = etl_pipeline(provider, etl_pipeline_taskgroup, dag)
        etl_pipelines
    
    return etl_pipeline_taskgroup
