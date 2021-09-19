# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from tasks.check_equality import check_equity

def compare(provider, task_group: TaskGroup, dag: DAG):
    compare_dataframes = PythonOperator(
        task_id="compare",
        task_group=task_group,
        dag=dag,
        python_callable=check_equity,
        op_kwargs={"provider": provider}
    )

    return compare_dataframes