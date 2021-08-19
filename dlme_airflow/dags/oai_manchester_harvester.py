import logging
import os
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
# from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable

from harvester.oai import oai_harvester
from harvester.utils.metadata import compare_metadata, validate_metadata_folder

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

provider = 'manchester'
home_directory = os.environ['AIRFLOW_HOME']
metadata_directory = os.environ['AIRFLOW_HOME']+"/metadata/"
working_directory = os.environ['AIRFLOW_HOME']+"/working/"
git_branch = Variable.get("git_branch", default_var='main')
git_repo = Variable.get("git_repo_metadata")

with DAG(
    f"oai_{provider}_harvester",
    default_args=default_args,
    description=f"{provider.upper()} OAI Harvester DAG",
    schedule_interval='@monthly',
    start_date=datetime(2021, 8, 18),
    tags=["example"],
    catchup=False,
) as dag:

    """ Validates if the git folder is empty or not """
    validate_git_folder = BranchPythonOperator(task_id='validate_metadata_folder',
                                               python_callable=validate_metadata_folder)

    """ If the git folder is empty, clone the repo """
    bash_command_clone = f"git clone --depth 1 --single-branch --branch {git_branch} {git_repo} {metadata_directory}"
    logging.info(f"bash command sent to server: {bash_command_clone}")
    git_clone = BashOperator(task_id='clone_metadata', bash_command=bash_command_clone)

    """ If the git folder is not empty, pull the latest changes """
    bash_command_pull = f"git -C {metadata_directory} pull origin {git_branch}"
    git_pull = BashOperator(task_id='pull_metadata', bash_command=bash_command_pull)

    """ Dummy operator (DO NOT DELETE, IT WOULD BREAK THE FLOW) """
    finished_pulling = DummyOperator(task_id='finished_pulling', dag=dag, trigger_rule='none_failed')

    oai_harvest = oai_harvester(provider)

    """ Compares source metadata and newly harvested metadata """
    compare_metadata_task = BranchPythonOperator(task_id='compare_metadata',
                                                 python_callable=compare_metadata,
                                                 op_kwargs={"provider": provider})

    """ Dummy operator (DO NOT DELETE, IT WOULD BREAK THE FLOW) """
    nothing_to_do = DummyOperator(task_id='equal', dag=dag)

    """ If the harvest has new metdata """
    bash_copy_data_command = f"cp -R {working_directory}/{provider} {metadata_directory}/{provider}"
    logging.info(f"bash command sent to server: {bash_copy_data_command}")
    copy_metadata = BashOperator(task_id='copy_metadata', bash_command=bash_copy_data_command)

    bash_git_commit_command = f"cd {metadata_directory} && git commit -am 'Airflow harvested metadata update for {provider}'"
    logging.info(f"bash command sent to server: {bash_git_commit_command}")
    commit_metadata = BashOperator(task_id='commit_metadata', bash_command=bash_git_commit_command)

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
                            'value': provider,
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
        # awslogs_group="/ecs/hello-world",
        # awslogs_stream_prefix="prefix_b/hello-world-container",  # prefix with container name
    )

    notify_data_manager = EmailOperator(
        task_id='notify_data_manager_update',
        to=Variable.get("data_manager_email"),
        subject=f"DLME-AIRFLOW: {provider} metadata updated and transformed.",
        html_content='Up-to-date metadata fetched and transformed'
    )

    """ Dummy operator (DO NOT DELETE, IT WOULD BREAK THE FLOW) """
    done = DummyOperator(task_id=f"monthly_{provider}_harvest_complete", dag=dag, trigger_rule='none_failed')

validate_git_folder >> [git_clone, git_pull] >> finished_pulling >> oai_harvest >> compare_metadata_task >> [nothing_to_do, copy_metadata]
copy_metadata >> commit_metadata >> trigger_transform >> notify_data_manager >> done
nothing_to_do >> done
