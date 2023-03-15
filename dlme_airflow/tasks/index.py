import os
import requests
import json

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def index_collection(**kwargs):
    api_endpoint = os.environ.get("API_ENDPOINT")
    token = os.environ.get("API_TOKEN")
    collection = kwargs["collection"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-type": "application/json",
    }
    payload = {
        "url": collection.intermediate_representation_location(),
    }
    response = requests.post(api_endpoint, data=json.dumps(payload), headers=headers)
    return response.json()["message"]


def index_task(collection, task_group: TaskGroup, dag: DAG) -> PythonOperator:
    return PythonOperator(
        task_id=f"index_{collection.label()}",
        task_group=task_group,
        dag=dag,
        python_callable=index_collection,
        op_kwargs={
            "collection": collection,
        },
    )
