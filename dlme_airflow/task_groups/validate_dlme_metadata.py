import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


home_directory = os.environ['AIRFLOW_HOME']
metadata_directory = os.environ['AIRFLOW_HOME']+"/metadata/"
git_branch = Variable.get("git_branch", default_var='intake')
git_repo = Variable.get("git_repo_metadata")

# Task Configuration
task_group_prefix = 'validate_metadata'
git_user_email = 'aaron.collier@stanford.edu'
git_user_name = 'Aaron Collier'


def validate_metadata_folder(**kwargs):
    if not os.path.exists(metadata_directory):
        return f"{task_group_prefix}.clone_metadata"

    if len(os.listdir(metadata_directory)) == 0:
        return f"{task_group_prefix}.clone_metadata"

    return f"{task_group_prefix}.pull_metadata"


def build_validate_metadata_taskgroup(dag: DAG) -> TaskGroup:
    validate_metadata_taskgroup = TaskGroup(group_id=task_group_prefix)

    bash_command_configure = f"git config --global user.email \"{git_user_email}\" && git config --global user.name \"{git_user_name}\""
    configure_git = BashOperator(
        task_id='configure_git',
        bash_command=bash_command_configure,
        task_group=validate_metadata_taskgroup,
        dag=dag)

    """ Validates if the git folder is empty or not """
    validate_git_folder = BranchPythonOperator(
        task_id=f"{task_group_prefix}_folder",
        python_callable=validate_metadata_folder,
        task_group=validate_metadata_taskgroup,
        dag=dag)

    """ If the git folder is empty, clone the repo """
    bash_command_clone = f"git clone --single-branch --branch {git_branch} {git_repo} {metadata_directory}"
    git_clone = BashOperator(
        task_id='clone_metadata',
        bash_command=bash_command_clone,
        task_group=validate_metadata_taskgroup,
        dag=dag)

    """ If the git folder is not empty, pull the latest changes """
    bash_command_pull = f"git -C {metadata_directory} pull origin {git_branch}"
    git_pull = BashOperator(
        task_id='pull_metadata',
        bash_command=bash_command_pull,
        task_group=validate_metadata_taskgroup,
        dag=dag)

    """ Dummy operator (DO NOT DELETE, IT WOULD BREAK THE FLOW) """
    finished_pulling = DummyOperator(
        task_id='finished_pulling',
        trigger_rule='none_failed',
        task_group=validate_metadata_taskgroup,
        dag=dag)

    configure_git >> validate_git_folder >> [git_clone, git_pull] >> finished_pulling

    return validate_metadata_taskgroup
