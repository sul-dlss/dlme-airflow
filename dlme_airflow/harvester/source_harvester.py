from dlme_airflow.utils.dataframe import dataframe_to_file
from dlme_airflow.utils.catalog import catalog_for_provider

COLLECTION = "collection"
PROVIDER = "provider"


def data_source_harvester(**kwargs):
    """Intake source harvester, takes a provider (for nested catalog use the catalog
    name.source, i.e. bodleian.arabic), generates a Pandas DataFrame, runs
    validations, and saves a copy of the DataFrame.

    @param -- provider
    """

    dataframe_to_file(source, kwargs[COLLECTION].provider.name, kwargs[COLLECTION],name)
