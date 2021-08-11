import logging
import os
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
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


with DAG(
    '01_clone_dlme_metadata',
    default_args=default_args,
    description='Clone and verify dlme-metadata in the container',
    schedule_interval='@hourly',
    start_date=datetime(2021, 8, 10),
    tags=['metadata'],
    catchup=False,
) as dag:
    sensor_dir_exists_cmd = """
    # Check if the folder exists
    if [ ! -d {metadata_directory} ]; then
      echo "{metadata_directory} does not exist. Will create it and git clone repo"
      exit 0
    fi
    # check if there are changes in branch
    cd {metadata_directory}
    git fetch
    local_hash=`git rev-parse {branch}`
    remote_hash=`git rev-parse origin/{branch}`
    if [ $local_hash != $remote_hash ]
    then
        echo "{branch} is not updated... will pull recent changes"
        exit 0
    else
        echo "Everything is updated with branch {branch}"
        exit 1
    fi
    """.format(branch=git_branch, metadata_directory=metadata_directory)
    sensor_dir_exists = BashSensor(task_id='dlme_metadata_changes',
                                   bash_command=sensor_dir_exists_cmd,
                                   poke_interval=60,
                                   timeout=60*50)

    """ Validates if the git folder is empty or not """
    validate_git_folder = BranchPythonOperator(task_id='validate_metadata_folder',
                                               python_callable=validate_metadata_folder)

    """ If the git folder is empty, clone the repo """
    bash_command_clone = "git clone --depth 1 --single-branch --branch {} {} {}".format(git_branch, git_repo, metadata_directory)
    logging.info("bash command sent to server: {}".format(bash_command_clone))
    git_clone = BashOperator(task_id='clone_metadata', bash_command=bash_command_clone)

    """ If the git folder is not empty, pull the latest changes """
    bash_command_pull = "git -C {} pull origin {}".format(metadata_directory, git_branch)
    git_pull = BashOperator(task_id='pull_metadata', bash_command=bash_command_pull)

    """ Dummy operator (DO NOT DELETE, IT WOULD BREAK THE FLOW) """
    finished_pulling = DummyOperator(task_id='finished_pulling', dag=dag, trigger_rule='none_failed')

    notify_data_manager = EmailOperator(
        task_id='notify_data_manager_update',
        to=Variable.get("data_manager_email"),
        subject='DLME-AIRFLOW: Metadata refresh complete.',
        html_content='Up-to-date metadata fetched'
    )

sensor_dir_exists >> validate_git_folder >> [git_clone, git_pull] >> finished_pulling >> notify_data_manager
