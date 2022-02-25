# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.catalog import catalog_for_provider
from harvester.source_post_harvester import data_source_post_harvester


def build_post_havest_task(provider, collection, post_harvest, task_group: TaskGroup, dag: DAG):
    if collection:
        label = f"{provider}_{collection}"
        if post_harvest:
            args = {"provider": provider, "collection": collection, post_harvest: "post_harvest"}
        else:
            args = {"provider": provider, "collection": collection, post_harvest: None}
    else:
        label = f"{provider}"
        if post_harvest:
            args = {"provider": provider, "collection": None, post_harvest: "post_harvest"}
        else:
            args = {"provider": provider, "collection": None, post_harvest: None}

    return PythonOperator(
        task_id=f"{label}_post_harvest",
        task_group=task_group,
        dag=dag,
        python_callable=data_source_post_harvester,
        op_kwargs=args
    )