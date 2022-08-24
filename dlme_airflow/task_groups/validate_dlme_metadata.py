import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup

# AWS Credentials
dev_role_arn = os.getenv("DEV_ROLE_ARN")

home_directory = os.getenv("AIRFLOW_HOME", "/opt/airflow")
metadata_directory = f"{home_directory}/metadata/"
working_directory = f"{home_directory}/working/"
s3_data = os.getenv("S3_BUCKET")

# Task Configuration
task_group_prefix = "validate_metadata"


def require_credentials(**kwargs):
    prefix = kwargs.get("task_group", task_group_prefix)
    if os.getenv("AWS_ACCESS_KEY_ID"):
        return f"{prefix}.assume_role"

    return f"{prefix}.sync_metadata"


def build_sync_metadata_taskgroup(collection, dag: DAG) -> TaskGroup:
    task_group_prefix = f"{collection.provider.name.upper()}_ETL.{collection.name}_etl.sync_{collection.name}_metadata"

    with TaskGroup(
        group_id=f"sync_{collection.name}_metadata"
    ) as sync_metadata_taskgroup:
        are_credentials_required = BranchPythonOperator(
            task_id="verify_aws_credentials",
            task_group=sync_metadata_taskgroup,
            python_callable=require_credentials,
            op_kwargs={"task_group": task_group_prefix},
            dag=dag,
        )

        bash_assume_role = f"""
        temp_role=$(aws sts assume-role --role-session-name \"DevelopersRole\" --role-arn {dev_role_arn}) && \
        export AWS_ACCESS_KEY_ID=$(echo $temp_role | jq .Credentials.AccessKeyId | xargs) && \
        export AWS_SECRET_ACCESS_KEY=$(echo $temp_role | jq .Credentials.SecretAccessKey | xargs) && \
        export AWS_SESSION_TOKEN=$(echo $temp_role | jq .Credentials.SessionToken | xargs) && \
        aws s3 sync {working_directory}/{collection.data_path()} {s3_data}/{collection.data_path()} --delete
        """
        aws_assume_role = BashOperator(
            task_id="assume_role",
            bash_command=bash_assume_role,
            task_group=sync_metadata_taskgroup,
            dag=dag,
        )

        bash_sync_s3 = f"aws s3 sync {working_directory}/{collection.data_path()} {s3_data}/{collection.data_path()} --delete"
        sync_metadata = BashOperator(
            task_id="sync_metadata",
            bash_command=bash_sync_s3,
            task_group=sync_metadata_taskgroup,
            dag=dag,
        )

        """ Dummy operator (DO NOT DELETE, IT WOULD BREAK THE FLOW) """
        finished_sync = DummyOperator(
            task_id="finished_metadata_sync",
            trigger_rule="none_failed",
            task_group=sync_metadata_taskgroup,
            dag=dag,
        )

        are_credentials_required >> [aws_assume_role, sync_metadata] >> finished_sync

    return sync_metadata_taskgroup
