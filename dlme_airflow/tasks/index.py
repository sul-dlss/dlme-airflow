import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.utils.task_group import TaskGroup
from utils.catalog import catalog_for_provider


def index_task(provider, collection, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    if collection:
        data_path = f"{provider}-{collection}"
    else:
        data_path = provider

    return ECSOperator(
        task_id=f"index_{provider}_{collection}",
        task_group=task_group,
        aws_conn_id="aws_conn",
        cluster="dlme-dev",
        task_definition="dlme-index-from-s3",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    'name': 'dlme-index-from-s3',
                    'environment': [{
                        'name': 'S3_FETCH_URL',
                        'value': f"https://dlme-metadata-dev.s3.us-west-2.amazonaws.com/output/output-{data_path}.ndjson"
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
        dag=dag
    )
