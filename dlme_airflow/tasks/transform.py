import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.amazon.aws.operators.ecs import EcsOperator
from airflow.utils.task_group import TaskGroup


def build_transform_task(collection, task_group: TaskGroup, dag: DAG):
    return EcsOperator(
        task_id=f"transform_{collection.label()}",
        task_group=task_group,
        aws_conn_id="aws_conn",
        cluster="dlme-dev",
        task_definition="dlme-transform",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "dlme-transform",
                    "environment": [
                        {"name": "DATA_PATH", "value": collection.data_path()},
                    ],
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "securityGroups": [
                    os.environ.get("SECURITY_GROUP_ID", os.getenv("ECS_SECURITY_GROUP"))
                ],
                "subnets": [os.environ.get("SUBNET_ID", os.getenv("ECS_SUBNET"))],
            },
        },
        dag=dag,
    )
