from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [Variable.get("data_manager_email")],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}
with DAG(
    'clone_dlme_metadata',
    default_args      = default_args,
    description       = 'Clone and verify dlme-metadata in the container',
    schedule_interval = '@weekly',
    start_date        = days_ago(0),
    tags              = ['metadata'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    verify_metadata = BashOperator(
        task_id='verify_dlme_metadata',
        bash_command='cd /opt/airflow/output && /opt/dlme_airflow/helpers/verify_branch.sh remotes/origin/main'
    )

    fetch_metadata = BashOperator(
        task_id='clone_dlme_metadata',
        bash_command='rm -rf /opt/airflow/output && git clone --single-branch --branch main https://github.com/sul-dlss/dlme-metadata.git /opt/airflow/output',
        trigger_rule=TriggerRule.ALL_FAILED
    )

    notify_data_manager_cloned = EmailOperator(
        task_id='notify_data_manager_update',
        to=Variable.get("data_manager_email"),
        subject='DLME-AIRFLOW: Weekly metadata refresh complete.',
        html_content='Up-to-date metadata fetched'
    )

    notify_data_manager_noop = EmailOperator(
        task_id='notify_data_manager_no_update',
        to=Variable.get("data_manager_email"),
        subject='DLME-AIRFLOW: Weekly metadata verified up-to-date.',
        html_content='Metadata verified up to date.',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

# Start with the verify_metadata task, if that fails clone and notify the data manager
# Otherwise notify the data manager that metadata is up to date.
verify_metadata >> [fetch_metadata, notify_data_manager_noop]
fetch_metadata >> notify_data_manager_cloned

verify_metadata.doc_md = dedent(
"""\
    #### Task: verify_metadata
    This tasks compares the current (local) git hash to the upstream hash on main.

    If the local copy of dlme-metadata is up to date, the data managers is notified that
    no changes where pulled.

    If the local copy of dlme-metadata differs from the upstream main branch it is cloned into
    /opt/airflow/output.
"""
)