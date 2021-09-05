import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.utils.task_group import TaskGroup


def build_transform_and_index_taskgroup(provider, dag: DAG) -> TaskGroup:
    transform_and_index_taskgroup = TaskGroup(group_id="transform_and_index")

    transform = ECSOperator(
        task_id=f"transform_{provider}",
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
        task_group=transform_and_index_taskgroup,
        dag=dag
    )

    index = ECSOperator(
        task_id=f"index_{provider}",
        aws_conn_id="aws_ecs",
        cluster="dlme-dev",
        task_definition="dlme-index-from-s3",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    'name': 'dlme-index-from-s3',
                    'environment': [{
                        'name': 'S3_FETCH_URL',
                        'value': f"https://dlme-metadata-dev.s3.us-west-2.amazonaws.com/output-{provider}.ndjson"
                    }]
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "securityGroups": [os.environ.get("SECURITY_GROUP_ID", "sg-00a3f19fea401ad4c")],
                "subnets": [os.environ.get("SUBNET_ID", "subnet-05a755dca83416be5")],
            },
        },
        task_group=transform_and_index_taskgroup,
        dag=dag
    )

    transform >> index

    return transform_and_index_taskgroup
