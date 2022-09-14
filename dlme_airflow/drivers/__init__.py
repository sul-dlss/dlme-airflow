import intake

from dlme_airflow.drivers.iiif_json import IiifJsonSource
from dlme_airflow.drivers.oai_xml import OaiXmlSource
from dlme_airflow.drivers.xml import XmlSource
from dlme_airflow.drivers.sequential_csv import SequentialCsvSource
from dlme_airflow.drivers.json import JsonSource


def register_drivers():
    intake.source.register_driver("iiif_json", IiifJsonSource)
    intake.source.register_driver("oai_xml", OaiXmlSource)
    intake.source.register_driver("xml", XmlSource)
    intake.source.register_driver("sequential_csv", SequentialCsvSource)
    intake.source.register_driver("custom_json", JsonSource)
