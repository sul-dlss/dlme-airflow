# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dlme_airflow.utils.filter_dataframe import filter_dataframe


def build_filter_data_task(collection, task_group: TaskGroup, dag: DAG):
    return PythonOperator(
        task_id=f"{collection.label()}_filter_data",
        task_group=task_group,
        dag=dag,
        python_callable=filter_dataframe,
        op_kwargs={
            "collection": collection,
        },
    )
