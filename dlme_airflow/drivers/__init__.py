import intake

from dlme_airflow.drivers.iiif_json import IiifJsonSource
from dlme_airflow.drivers.feed import FeedSource
from dlme_airflow.drivers.oai_xml import OaiXmlSource
from dlme_airflow.drivers.xml import XmlSource
from dlme_airflow.drivers.sequential_csv import SequentialCsvSource


def register_drivers():
    intake.source.register_driver("iiif_json", IiifJsonSource)
    intake.source.register_driver("oai_xml", OaiXmlSource)
    intake.source.register_driver("feed", FeedSource)
    intake.source.register_driver("xml", XmlSource)
    intake.source.register_driver("sequential_csv", SequentialCsvSource)
