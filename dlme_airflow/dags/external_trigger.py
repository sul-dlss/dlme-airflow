import json

import pika
from airflow import utils
from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='external_trigger',
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        'start_date': utils.dates.days_ago(1),
    },
    schedule_interval='@once', # ='*/1 * * * *',
)


def consume_message(**kwargs):
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='airflow-test', durable=True)

    method_frame, header_frame, body = channel.basic_get(queue='airflow-test')
    if body:
        print(f"BODY: {body}")
        json_params = json.loads(body)
        print(f"JSON PARAMS: {json_params}")
        kwargs['ti'].xcom_push(key='job_params', value=json.dumps(json_params['params']))
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        connection.close()
        print("Got message ? {}".format(body))
        return True
    else:
        return False


router = PythonOperator(
    task_id='router',
    python_callable=consume_message,
    dag=dag,
    provide_context=True,
    depends_on_past=True
)

task_trash = DummyOperator(
    task_id='task_trash',
    dag=dag
)

router >> task_trash