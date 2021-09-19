# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.catalog import catalog_for_provider
from harvester.source_harvester import data_source_harvester


def build_havester_task(provider, collection, task_group: TaskGroup, dag: DAG):
    task_id = f"extract" if collection is None else f"extract.{collection}"

    return PythonOperator(
        task_id=task_id,
        task_group=task_group,
        dag=dag,
        python_callable=data_source_harvester,
        op_kwargs={"provider": f"{provider}.{collection}"}
    )


def harvester_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_array = []
    source = catalog_for_provider(provider)
    try:
        collections = iter(list(source))
        for collection in collections:
            task_array.append(build_havester_task(provider, collection, task_group, dag))
    except TypeError:
        return build_havester_task(provider, None, task_group, dag)

    return task_array


def build_harvester_taskgroup(provider, dag: DAG) -> TaskGroup:
    iiif_harvester_taskgroup = TaskGroup(group_id=f"{provider}.etl.pipeline")

    return harvester_tasks(provider, iiif_harvester_taskgroup, dag)
