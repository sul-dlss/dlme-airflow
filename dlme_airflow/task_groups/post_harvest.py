# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.catalog import catalog_for_provider
from tasks.fetch_post_harvest import fetch_post_harvest


def build_post_havest_task(provider, task_group: TaskGroup, dag: DAG):
    return PythonOperator(
        task_id=f"{provider}_post_harvest",
        task_group=task_group,
        dag=dag,
        python_callable=fetch_post_harvest,
        op_kwargs={"provider": provider}
    )

def post_harvest_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_array = []
    source = catalog_for_provider(provider)

    try:
        collections = list(source).__iter__()
        for collection in collections:
            task_array.append(build_post_havest_task(f"{provider}.{collection}", task_group, dag))
    except TypeError:
        return build_post_havest_task(f"{provider}", task_group, dag)

    return task_array

def build_post_havest_taskgroup(provider, dag: DAG) -> TaskGroup:
    post_harvest_taskgroup = TaskGroup(group_id="post_harvest")

    return post_harvest_tasks(provider, post_harvest_taskgroup, dag)
