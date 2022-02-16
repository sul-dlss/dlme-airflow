# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.catalog import catalog_for_provider
from harvester.source_harvester import data_source_harvester


def build_havester_task(provider, collection, task_group: TaskGroup, dag: DAG):
    if collection:
        label = f"{provider}_{collection}"
        args = {"provider": provider, "collection": collection}
    else:
        label = f"{provider}_{collection}"
        args = {"provider": provider, "collection": None}

    return PythonOperator(
        task_id=f"{label}_harvest",
        task_group=task_group,
        dag=dag,
        python_callable=data_source_harvester,
        op_kwargs=args
    )
