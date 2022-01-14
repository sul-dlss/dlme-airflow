import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.utils.task_group import TaskGroup
from utils.catalog import catalog_for_provider


def build_index_task(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    return ECSOperator(
        task_id=f"index_{provider}",
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
                        'value': f"https://dlme-metadata-dev.s3.us-west-2.amazonaws.com/output/output-{provider}.ndjson"
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
        task_group=task_group,
        dag=dag
    )


def index_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_array = []
    source = catalog_for_provider(provider)

    try:
        collections = iter(list(source))
        for collection in collections:
            coll_label = f"{provider}-{collection}"
            task_array.append(build_index_task(coll_label, task_group, dag))
    except:
        return build_index_task(provider, task_group, dag)

    return task_array


def build_index_taskgroup(provider, dag: DAG) -> TaskGroup:
    index_taskgroup = TaskGroup(group_id="index")

    return index_tasks(provider,index_taskgroup, dag)
