import logging
import os
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable


home_directory = os.environ['AIRFLOW_HOME']
metadata_directory = os.environ['AIRFLOW_HOME']+"/metadata/"
git_branch = Variable.get("git_branch", default_var='main')
git_repo = Variable.get("git_repo_metadata")


def validate_metadata_folder(**kwargs):
    logging.info("validate_git_dags_folder STARTED")
    if not os.path.exists(metadata_directory):
        os.makedirs(metadata_directory)
    if len(os.listdir(metadata_directory)) == 0:
        return 'clone_metadata'
    return 'pull_metadata'

def ValidateDlmeMetadata(parent_dag, child_dag, args):
    validate_dlme_metdata_subdag = DAG(
        f'{parent_dag}.{child_dag}',
        default_args = args,
        description=f'Validate DLME-Metdata is present and up to date',
        schedule_interval='@yearly',
        start_date=datetime(2021, 8, 26)
    )
    with validate_dlme_metdata_subdag:

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
        finished_pulling = DummyOperator(task_id='finished_pulling', dag=validate_dlme_metdata_subdag, trigger_rule='none_failed')

    validate_git_folder >> [git_clone, git_pull] >> finished_pulling

    return validate_dlme_metdata_subdag