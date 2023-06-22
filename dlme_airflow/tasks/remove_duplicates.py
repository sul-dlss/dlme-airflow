import os
import pandas as pd
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def run_remove_duplicates(**kwargs):
    coll = kwargs["collection"]
    data_path = coll.data_path()
    working_csv = coll.datafile("csv")

    if os.path.isfile(working_csv):
        df = pd.read_csv(working_csv)
        # Filter out duplicate records and over write the csv
        df = df.drop_duplicates(subset="identifier")
        df.to_csv(working_csv)

    return working_csv


def build_remove_duplicates_task(collection, task_group: TaskGroup, dag: DAG):
    return PythonOperator(
        task_id=f"{collection.label()}_remove_duplicates",
        task_group=task_group,
        dag=dag,
        python_callable=run_remove_duplicates,
        op_kwargs={
            "collection": collection
        },
    )
