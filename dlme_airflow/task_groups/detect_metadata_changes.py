# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.catalog import catalog_for_provider
from tasks.check_equality import check_equity


def build_detect_changes_task(provider, task_group: TaskGroup, dag: DAG):
    compare_dataframes = PythonOperator(
        task_id=f"{provider}_compare_dataframes",
        task_group=task_group,
        dag=dag,
        python_callable=check_equity,
        op_kwargs={"provider": provider}
    )

    return compare_dataframes


def inspect_dataframe_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_array = []
    source = catalog_for_provider(provider)

    try:
        collections = list(source).__iter__()
        for collection in collections:
            task_array.append(build_detect_changes_task(f"{provider}.{collection}", task_group, dag))
    except TypeError:
        return build_detect_changes_task(f"{provider}", task_group, dag)

    return task_array


def build_detect_metadata_changes_taskgroup(provider, dag: DAG) -> TaskGroup:
    detect_metadata_changes_taskgroup = TaskGroup(group_id="detect_metadata_changes")

    return inspect_dataframe_tasks(provider, detect_metadata_changes_taskgroup, dag)
