from datetime import datetime
from datetime import timedelta
import logging

# Operators and utils required from airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

import intake

# Our stuff
from drivers.iiif_json import IiifJsonSource
from drivers.feed import FeedSource
from drivers.oai_xml import OaiXmlSource
from drivers.xml import XmlSource
from drivers.sequential_csv import SequentialCsvSource
from models.provider import Provider
from task_groups.etl import build_provider_etl_taskgroup
from utils.catalog import fetch_catalog


_harvest_dags = dict()


def harvest_dags():
    return _harvest_dags


def default_dag_args():
    """
    These args will get passed on to each operator.
    You can override them on a per-task basis during operator initialization.
    """
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "email": [Variable.get("data_manager_email")],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(seconds=60),
        "catchup": False,
    }


def create_dag(provider, default_args):
    dag = DAG(
        provider.name,
        default_args=default_args,
        schedule_interval="@once",
        start_date=datetime(2021, 9, 6),
    )

    with dag:
        # A dummy operator is required as a transition point between task groups
        harvest_begin = DummyOperator(
            task_id="harvest_begin", trigger_rule="none_failed", dag=dag
        )
        harvest_complete = DummyOperator(
            task_id="harvest_complete", trigger_rule="none_failed", dag=dag
        )

        # TODO
        # post_harvest_begin = DummyOperator(task_id='post_harvest_begin', trigger_rule='none_failed', dag=dag)
        # post_harvest = build_post_havest_taskgroup(provider, dag)
        # post_harvest_complete = DummyOperator(task_id='post_harvest_complete', trigger_rule='none_failed', dag=dag)

        # TODO: A dummy operator is required as a transition point between task groups
        # post_harvest_complete = DummyOperator(task_id='post_harvest_complete', trigger_rule='none_failed', dag=dag)

        etl = build_provider_etl_taskgroup(provider, dag)

        harvest_begin >> etl >> harvest_complete

    return dag


def register_drivers():
    intake.source.register_driver("iiif_json", IiifJsonSource)
    intake.source.register_driver("oai_xml", OaiXmlSource)
    intake.source.register_driver("feed", FeedSource)
    intake.source.register_driver("xml", XmlSource)
    intake.source.register_driver("sequential_csv", SequentialCsvSource)


def create_provider_dags():
    for provider in iter(list(fetch_catalog())):
        current_provider = Provider(provider)
        logging.info(f"Creating DAG for {current_provider.name}")
        globals()[provider] = create_dag(current_provider, default_dag_args())
        _harvest_dags[provider] = globals()[provider]

    logging.info(f"_harvest_dags={_harvest_dags}")
