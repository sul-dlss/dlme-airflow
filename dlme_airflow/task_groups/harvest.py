# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.catalog import catalog_for_provider
from harvester.source_harvester import data_source_harvester


def build_havester_task(provider, task_group: TaskGroup, dag: DAG):
    return PythonOperator(
        task_id=f"{provider}_intake_harvester",
        task_group=task_group,
        dag=dag,
        python_callable=data_source_harvester,
        op_kwargs={"provider": f"{provider}"}
    )


def harvester_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_array = []
    source = catalog_for_provider(provider)
    try:
        collections = iter(list(source))
        for collection in collections:
            task_array.append(build_havester_task(f"{provider}.{collection}", task_group, dag))
    except TypeError:
        return build_havester_task(f"{provider}", task_group, dag)

    return task_array


def build_harvester_taskgroup(provider, dag: DAG) -> TaskGroup:
    iiif_harvester_taskgroup = TaskGroup(group_id="metadata_harvester")

    return harvester_tasks(provider, iiif_harvester_taskgroup, dag)
