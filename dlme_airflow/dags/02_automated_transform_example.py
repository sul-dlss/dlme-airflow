import logging
import os
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.contrib.sensors.bash_sensor import BashSensor
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
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
    '02_automated_transform_example',
    default_args=default_args,
    description='Example dag for triggering the DLME Transform ECS Task',
    schedule_interval='@weekly',
    start_date=datetime(2021, 8, 17),
    tags=['metadata'],
    catchup=False,
) as dag:

    trigger_transform = ECSOperator(
        task_id="trigger_transform",
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
                            'value': 'stanford'
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
    )