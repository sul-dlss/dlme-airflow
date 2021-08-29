import os
from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.task_group import TaskGroup


def new_metadata(**kwargs):
    task = kwargs['ti']
    changes = int(task.xcom_pull(task_ids='bodleian_record_changes'))
    deletes = int(task.xcom_pull(task_ids='bodleian_record_deletes'))
    additions = int(task.xcom_pull(task_ids='bodleian_record_additions'))
    total_changes = changes + deletes + additions
    
    if total_changes > 0:
        print(f"{total_changes}")
        return 'collect_metadata_changes.commit_and_push_changes'
    
    return 'collect_metadata_changes.no_metadata_changes'


def build_collect_metadata_changes_taskgroup(provider, dag: DAG) -> TaskGroup:
    home_directory = os.environ['AIRFLOW_HOME']
    metadata_directory = f"{home_directory}/metadata/{provider}/"

    collect_metadata_changes_taskgroup = TaskGroup(group_id="collect_metadata_changes")

    git_count_modifications_cmd = f"cd {metadata_directory} && git status --porcelain | grep \"^ M\" | wc -l"
    count_metadata_modifications = BashOperator(
        task_id='count_metadata_modifications',
        bash_command=git_count_modifications_cmd,
        task_group=collect_metadata_changes_taskgroup,
        dag=dag
    )

    get_count_deletes_cmd = f"cd {metadata_directory} && git status --porcelain | grep \"^ D\" | wc -l"
    count_metadata_deletes = BashOperator(
        task_id='count_metadata_deletes',
        bash_command=get_count_deletes_cmd,
        task_group=collect_metadata_changes_taskgroup,
        dag=dag
    )

    get_count_additions_cmd = f"cd {metadata_directory} && git status --porcelain | grep \"^??\" | wc -l"
    count_metadata_additions = BashOperator(
        task_id='count_metadata_additions',
        bash_command=get_count_additions_cmd,
        task_group=collect_metadata_changes_taskgroup,
        dag=dag
    )

    delect_metadata_updates = BranchPythonOperator(
        task_id='detect_metadata_updates',
        python_callable=new_metadata,
        task_group=collect_metadata_changes_taskgroup,
        dag=dag
    )

    git_push_cmd = f"cd {metadata_directory} && git add . && git commit -m 'Automated harvest update: {provider}' && git push"
    commit_and_push_changes = BashOperator(task_id='commit_and_push_changes',
        bash_command=git_push_cmd,
        task_group=collect_metadata_changes_taskgroup,
        dag=dag
    )

    no_changes_detected = DummyOperator(task_id='no_metadata_changes', task_group=collect_metadata_changes_taskgroup, dag=dag, trigger_rule='none_failed')
    done = DummyOperator(task_id='done', task_group=collect_metadata_changes_taskgroup, dag=dag, trigger_rule='none_failed')

    count_metadata_modifications >> count_metadata_deletes >> count_metadata_additions >> delect_metadata_updates >> [commit_and_push_changes, no_changes_detected] >> done
    
    return collect_metadata_changes_taskgroup