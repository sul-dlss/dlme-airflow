#!/usr/bin/python
from datetime import date

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email

from utils.catalog import catalog_for_provider


def email_callback(**kwargs):
    provider_id = kwargs.get('provider')
    collection_id = kwargs.get('collection', None)

    if collection_id:
        data_path = f"{provider_id}/{collection_id}"
        file_path = f"{provider_id}_{collection_id}"
    else:
        data_path = provider_id
        file_path = provider_id

    subject = f"ETL Report for {data_path}"
    report_file = f"/tmp/report_{file_path}_{date.today()}.html"

    with open(report_file) as f:
        content = f.read()
    send_email(
        to=[
            'amcollie@stanford.edu'
        ],
        subject=subject,
        html_content=content,
    )


def build_send_harvest_report_task(provider, collection, task_group: TaskGroup, dag: DAG):
    if collection:
        label = f"{provider}_{collection}"
        args = {
            "provider": provider,
            "collection": collection
        }
    else:
        label = provider
        args = {
            "provider": provider,
        }

    return PythonOperator(
        task_id=f"{provider}_{collection}_harvest_send_report",
        dag=dag,
        task_group=task_group,
        python_callable=email_callback,
        op_kwargs=args,
        trigger_rule='none_failed'
    )


def send_harvest_report_tasks(provider, task_group: TaskGroup, dag: DAG):
    task_array = []
    source = catalog_for_provider(provider)

    try:
        collections = list(source).__iter__()
        for collection in collections:
            send_report_task = build_send_harvest_report_task(provider, collection, task_group, dag)
            task_array.append(send_report_task)
    except TypeError:
        return build_send_harvest_report_task(f"{provider}", "", task_group, dag)

    return task_array


def build_send_harvest_report_taskgroup(provider, dag: DAG) -> TaskGroup:
    send_harvest_report_taskgroup = TaskGroup(group_id="send_harvest_report")

    return send_harvest_report_tasks(provider, send_harvest_report_taskgroup, dag)
