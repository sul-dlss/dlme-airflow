import os
import sys
import logging

from typing import Union
from datetime import datetime
from datetime import timedelta

# Operators and utils required from airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

# Our stuff
from dlme_airflow.models.provider import Provider, Collection
from dlme_airflow.utils.catalog import fetch_catalog
from dlme_airflow.task_groups.etl import (
    build_provider_etl_taskgroup,
    build_collection_etl_taskgroup,
)


_harvest_dags: dict[str, DAG] = dict()


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
    }


def create_dags(provider) -> list[DAG]:
    """Returns a list of DAGs for a given provider."""
    dags = []
    if provider.catalog.metadata.get("separate_dags"):
        # create a DAG for each collection, which can be useful if you want to
        # schedule them separately
        for collection in provider.collections:
            dags.append(assemble_dag(collection))
    else:
        # create a single DAG for the provider and all its collections which
        # will all run in parallel
        dags.append(assemble_dag(provider))
    return dags


def assemble_dag(source: Union[Provider, Collection]):
    """Returns a DAG for either a Provider or a Collection."""
    default_args = default_dag_args()
    default_schedule = os.getenv("DEFAULT_DAG_SCHEDULE", "@daily")

    schedule = source.catalog.metadata.get("schedule", default_schedule)
    start_date = datetime(2022, 9, 6)
    dag_id = source.label()

    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False,
    ) as dag:
        harvest_begin = DummyOperator(
            task_id="harvest_begin", trigger_rule="none_failed"
        )
        harvest_complete = DummyOperator(
            task_id="harvest_complete", trigger_rule="none_failed"
        )
        if type(source) == Provider:
            etl = build_provider_etl_taskgroup(source, dag)
        elif type(source) == Collection:
            etl = build_collection_etl_taskgroup(source, dag)
        else:
            raise Exception("source must be a Provider or a Collection")
        harvest_begin >> etl >> harvest_complete

    return dag


def create_provider_dags(module_name=None):
    """Create all the DAGs optionally in a given module namespace so that
    Airflow can discover them.
    """
    for provider in fetch_catalog():
        current_provider = Provider(provider)
        logging.info(f"Creating DAG for {current_provider.name}")

        dags = create_dags(current_provider)
        for dag in dags:
            if module_name is None:
                globals()[dag.dag_id] = dag
            else:
                setattr(sys.modules[module_name], dag.dag_id, dag)
            _harvest_dags[dag.dag_id] = dag

    logging.info(f"_harvest_dags={_harvest_dags}")
