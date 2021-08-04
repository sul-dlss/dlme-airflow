from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from harvester.aims import oai
from harvester.copydir import copydir

# def print_world():
#     print('HELLO WORLD')
#     return 'HELLO WORLD'

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
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
with DAG(
    "aims_harvester",
    default_args=default_args,
    description="AIMS Harvester DAG",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["example"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(task_id="aims_harvest", python_callable=oai)

    t2 = PythonOperator(
        task_id="aims_copy", python_callable=copydir, op_kwargs={"provider": "aims"}
    )

t1 >> t2
