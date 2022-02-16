import intake
import logging
from datetime import timedelta

# Operators and utils required from airflow
from airflow.models import Variable

from drivers.iiif_json import IIIfJsonSource
from drivers.feed import FeedSource
from drivers.oai_xml import OAIXmlSource
from utils.catalog import fetch_catalog
from services.harvest_dag_generator import create_dag

intake.source.register_driver("iiif_json", IIIfJsonSource)
intake.source.register_driver("oai_xml", OAIXmlSource)
intake.source.register_driver("feed", FeedSource)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [Variable.get("data_manager_email")],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    'catchup': False,
}

catalog = fetch_catalog()

try:
    collections = iter(list(catalog))
except TypeError:
    collections = [catalog]

for provider in collections:
    logging.info(f"Creating DAG for {provider}")
    globals()[provider] = create_dag(provider, default_args)
