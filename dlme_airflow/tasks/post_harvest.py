# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# The method name/include must match the value in metadata.post_harvest from the catalog
from utils.yale_babylon_remove_non_relevant_records import yale_remove_non_relevant
from utils.dasi_get_thumbnail_urls import dasi_add_thumbnail_urls


def run_post_harvest(**kwargs):
    post_harvest_func = globals()[kwargs["post_harvest"]]

    post_harvest_func(
        **kwargs
    )  # This dynamically calls the function defined by metadata.post_harvest in the catalog


def build_post_havest_task(
    provider, collection, post_harvest, task_group: TaskGroup, dag: DAG
):
    if collection:
        label = f"{provider}_{collection}"
        args = {
            "provider": provider,
            "collection": collection,
            "post_harvest": post_harvest,
        }
    else:
        label = f"{provider}"
        args = {"provider": provider, "collection": None, "post_harvest": post_harvest}

    return PythonOperator(
        task_id=f"{label}_post_harvest",
        task_group=task_group,
        dag=dag,
        python_callable=run_post_harvest,
        op_kwargs=args,
    )
