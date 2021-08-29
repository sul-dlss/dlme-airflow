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

# home_directory = os.environ['AIRFLOW_HOME']
# metadata_directory = os.environ['AIRFLOW_HOME']+"/metadata/"
# git_branch = Variable.get("git_branch", default_var='main')
# git_repo = Variable.get("git_repo_metadata")


# def validate_metadata_folder(**kwargs):
#     logging.info("validate_git_dags_folder STARTED")
#     if not os.path.exists(metadata_directory):
#         os.makedirs(metadata_directory)
#     if len(os.listdir(metadata_directory)) == 0:
#         return 'clone_metadata'
#     return 'pull_metadata'


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
        # awslogs_group="/ecs/hello-world",
        # awslogs_stream_prefix="prefix_b/hello-world-container",  # prefix with container name
    )