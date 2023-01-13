import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup


def build_transform_task(collection, task_group: TaskGroup, dag: DAG):
    return DockerOperator(
        task_id=f"transform_{collection.label()}",
        task_group=task_group,
        image='suldlss/dlme-transform:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            "DATA_PATH": collection.data_path()
        },
        docker_url="tcp://docker-proxy:2375",
        network_mode="bridge",
        dag=dag,
    )
