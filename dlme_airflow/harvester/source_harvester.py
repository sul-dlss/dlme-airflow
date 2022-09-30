from dlme_airflow.utils.dataframe import dataframe_to_file

COLLECTION = "collection"
PROVIDER = "provider"


def data_source_harvester(**kwargs):
    """Intake source harvester, takes a provider (for nested catalog use the catalog
    name.source, i.e. bodleian.arabic), generates a Pandas DataFrame, runs
    validations, and saves a copy of the DataFrame.

    @param -- provider
    """

    collection = kwargs.get("collection")
    # dataframe_to_file(source, collection.provider.name, collection)
    return dataframe_to_file(collection)
