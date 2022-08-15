# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from harvester.source_harvester import data_source_harvester


def build_havester_task(collection, task_group: TaskGroup, dag: DAG):
    return PythonOperator(
        task_id=f"{collection.label()}_harvest",
        task_group=task_group,
        dag=dag,
        python_callable=data_source_harvester,
        op_kwargs={"collection": collection},
    )
