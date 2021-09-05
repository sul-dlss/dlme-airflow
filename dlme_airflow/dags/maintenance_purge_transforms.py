import os
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.models import Variable

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [Variable.get("data_manager_email")],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'catchup': False,
}


with DAG(
    'maintenance.purge_transforms',
    default_args=default_args,
    description='Example dag for triggering the DLME Transform ECS Task',
    schedule_interval='@yearly',
    start_date=datetime(2021, 8, 24),
    tags=['metadata'],
    catchup=False,
) as dag:

    trigger_transform = ECSOperator(
        task_id="trigger_purge_transform_task",
        aws_conn_id="aws_ecs",
        cluster="dlme-dev",
        task_definition="dlme-purge-transforms",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    'name': 'dlme-purge-transforms'
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "securityGroups": [os.environ.get("SECURITY_GROUP_ID", "sg-00a3f19fea401ad4c")],
                "subnets": [os.environ.get("SUBNET_ID", "subnet-05a755dca83416be5")],
            },
        },
    )
