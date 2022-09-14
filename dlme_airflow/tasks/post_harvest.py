# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# While it looks like they're not used these are called dynamically by run_post_harvest
from dlme_airflow.utils.add_thumbnails import add_thumbnails
from dlme_airflow.utils.qnl_merge_records import (
    merge_records,
)
from dlme_airflow.utils.yale_babylon_remove_non_relevant_records import (
    remove_non_relevant,
)

# The method name/include must match the value in metadata.post_harvest from the catalog


def run_post_harvest(**kwargs):
    post_harvest_func = globals()[kwargs["post_harvest"]]

    post_harvest_func(
        **kwargs
    )  # This dynamically calls the function defined by metadata.post_harvest in the catalog


def build_post_harvest_task(collection, task_group: TaskGroup, dag: DAG):
    return PythonOperator(
        task_id=f"{collection.label()}_post_harvest",
        task_group=task_group,
        dag=dag,
        python_callable=run_post_harvest,
        op_kwargs={
            "collection": collection,
            "post_harvest": collection.catalog.metadata.get("post_harvest"),
        },
    )
