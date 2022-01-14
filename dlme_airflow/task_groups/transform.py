import os
import logging

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.utils.task_group import TaskGroup
from utils.catalog import catalog_for_provider


def build_transform_task(provider, data_path, task_group: TaskGroup, dag: DAG):
     return ECSOperator(
        task_id=f"transform_{provider}",
        aws_conn_id="aws_conn",
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
                            'value': data_path
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
 

def transform_tasks(provider, task_group: TaskGroup, dag: DAG) -> TaskGroup:
    task_array = []
    source = catalog_for_provider(provider)

    try:
        collections = iter(list(source))
        for collection in collections:
            coll_label = f"{provider}.{collection}"
            data_path = catalog_for_provider(coll_label).metadata.get('data_path', f"{provider}/{collection}")
            task_array.append(build_transform_task(coll_label, data_path, task_group, dag))
    except TypeError:
        data_path = source.metadata.get('data_path', f"{provider}")
        return build_transform_task(f"{provider}", data_path, task_group, dag)

    return task_array


def build_transform_taskgroup(provider, dag: DAG) -> TaskGroup:
    transform_taskgroup = TaskGroup(group_id="transform")

    return transform_tasks(provider, transform_taskgroup, dag)
