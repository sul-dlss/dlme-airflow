import intake
import logging

from datetime import timedelta

from airflow.models import Variable

from drivers.iiif_json import IiifJsonSource
from drivers.feed import FeedSource
from drivers.oai_xml import OaiXmlSource
from drivers.xml import XmlSource
from drivers.sequential_csv import SequentialCsvSource
from utils.catalog import fetch_catalog
from services.harvest_dag_generator import create_dag
from models.provider import Provider

intake.source.register_driver("iiif_json", IiifJsonSource)
intake.source.register_driver("oai_xml", OaiXmlSource)
intake.source.register_driver("feed", FeedSource)
intake.source.register_driver("xml", XmlSource)
intake.source.register_driver("sequential_csv", SequentialCsvSource)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [Variable.get("data_manager_email")],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=60),
    "catchup": False,
}

for provider in iter(list(fetch_catalog())):
    current_provider = Provider(provider)
    logging.info(f"Creating DAG for {current_provider.name}")
    globals()[provider] = create_dag(current_provider, default_args)
