import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.utils.task_group import TaskGroup


def transform(provider, task_group: TaskGroup, dag: DAG) -> ECSOperator:
    return ECSOperator(
        task_id="transform",
        aws_conn_id="aws_ecs",
        cluster="dlme-dev",
        task_definition="dlme-transform",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    'name': 'dlme-transform',
                    'environment': [
                        {
                            'name': 'DATA_PATH',
                            'value': provider
                        },
                    ],
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "securityGroups": [os.environ.get("SECURITY_GROUP_ID", "sg-00a3f19fea401ad4c")],
                "subnets": [os.environ.get("SUBNET_ID", "subnet-05a755dca83416be5")],
            },
        },
        task_group=task_group,
        dag=dag
    )
