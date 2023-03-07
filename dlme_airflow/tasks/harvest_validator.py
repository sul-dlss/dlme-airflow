import pandas as pd

from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup

from dlme_airflow.utils.dataframe import dataframe_from_file


def validate_harvest(task_instance, task, **kwargs):
    task_prefix = ".".join(task.task_id.split(".")[:-1])
    collection = kwargs["collection"]
    current_harvest = pd.read_csv(
        task_instance.xcom_pull(task_ids=task.upstream_task_ids)[0]
    )
    previous_harvest = dataframe_from_file(collection)

    if current_harvest.equals(previous_harvest):
        return f"{task_prefix}.skip_load_data"
    else:
        return f"{task_prefix}.load_data"


def build_validate_harvest_task(collection, task_group: TaskGroup, dag: DAG):
    return BranchPythonOperator(
        task_id=f"{collection.label()}_validate_harvest",
        dag=dag,
        task_group=task_group,
        python_callable=validate_harvest,
        op_kwargs={"collection": collection},
    )
