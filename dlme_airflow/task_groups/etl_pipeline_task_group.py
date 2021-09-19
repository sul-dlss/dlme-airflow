# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

import logging

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from utils.catalog import catalog_for_provider
from harvester.source_harvester import data_source_harvester


def build_havester_task(provider, collection, task_group: TaskGroup, dag: DAG):
    task_id = f"extract" if collection is None else f"extract.{collection}"

    logging.info(f"Building harvester task with id {task_id} for {provider}.{collection}")
    return PythonOperator(
        task_id=task_id,
        task_group=task_group,
        dag=dag,
        python_callable=data_source_harvester,
        op_kwargs={"provider": f"{provider}.{collection}"}
    )


# def etl_pipeline_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
def etl_pipeline(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_group_array = []
    source = catalog_for_provider(provider)
    try:
        collections = iter(list(source))
        for collection in collections:
            logging.info(f"Building task group for {provider}.{collection}")
            with TaskGroup(group_id=collection) as collection_tg:
                logging.info(f"Building tasks for task_group {provider}.{collection}")
                extract = build_havester_task(provider, collection, collection_tg, dag)

                extract
                task_group_array.append(collection_tg)
        
    except TypeError:
        return build_havester_task(provider, None, task_group, dag)

    return task_group_array


def build_etl_pipeline_taskgroup(provider, dag: DAG) -> TaskGroup:
    with TaskGroup(
        group_id=f"{provider}.etl.pipeline"
    ) as etl_pipeline_taskgroup:
        entry = DummyOperator(task_id='begin')
        etl_pipelines = etl_pipeline(provider, etl_pipeline_taskgroup, dag)
        exit = DummyOperator(task_id="complete")

        entry >> etl_pipelines >> exit
    
    return etl_pipeline_taskgroup
