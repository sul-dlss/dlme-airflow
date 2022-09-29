from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email

from dlme_airflow.utils.catalog import catalog_for_provider


def email_callback(task_instance, task, **kwargs):
    subject = f"ETL Report for {kwargs.get('collection').data_path()}"
    content = task_instance.xcom_pull(task_ids=task.upstream_task_ids)[0]

    send_email(
        to=["dlme-monitoring@lists.stanford.edu"],
        subject=subject,
        html_content=content,
    )


def build_send_harvest_report_task(collection, task_group: TaskGroup, dag: DAG):
    return PythonOperator(
        task_id=f"{collection.label()}_harvest_send_report",
        dag=dag,
        task_group=task_group,
        python_callable=email_callback,
        op_kwargs={"collection": collection},
        trigger_rule="none_failed",
    )


def send_harvest_report_tasks(provider, task_group: TaskGroup, dag: DAG):
    task_array = []
    source = catalog_for_provider(provider)

    try:
        collections = list(source).__iter__()
        for collection in collections:
            send_report_task = build_send_harvest_report_task(
                collection, task_group, dag
            )
            task_array.append(send_report_task)
    except TypeError:
        return build_send_harvest_report_task("", task_group, dag)

    return task_array


def build_send_harvest_report_taskgroup(provider, dag: DAG) -> TaskGroup:
    send_harvest_report_taskgroup = TaskGroup(group_id="send_harvest_report")

    return send_harvest_report_tasks(provider, send_harvest_report_taskgroup, dag)
