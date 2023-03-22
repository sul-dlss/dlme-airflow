import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount


def build_transform_task(collection, task_group: TaskGroup, dag: DAG):
    return DockerOperator(
        task_id=f"transform_{collection.label()}",
        task_group=task_group,
        image=os.environ.get("TRANSFORM_IMAGE"),
        force_pull=True,
        api_version="auto",
        auto_remove=True,
        environment={
            "DATA_PATH": collection.data_path(),
            "SOURCE_DATA": os.environ.get("SOURCE_DATA_PATH"),
        },
        docker_url="tcp://docker-proxy:2375",
        network_mode="bridge",
        dag=dag,
        mounts=[
            Mount(
                source=os.environ.get("METADATA_OUTPUT_PATH"),
                target="/opt/airflow/metadata",
                type="bind",
            ),
            Mount(
                source=os.environ.get("SOURCE_DATA_PATH"),
                target="/opt/airflow/working",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
    )
