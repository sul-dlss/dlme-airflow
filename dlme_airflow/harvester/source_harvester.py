import logging

from dlme_airflow.utils.dataframe import dataframe_to_file
from dlme_airflow.utils.catalog import catalog_for_provider

COLLECTION = "collection"
PROVIDER = "provider"


def provider_key(**kwargs):
    if PROVIDER not in kwargs:
        return None

    if COLLECTION not in kwargs:
        return kwargs[PROVIDER]

    return f"{kwargs[PROVIDER]}.{kwargs[COLLECTION]}"


def data_source_harvester(**kwargs):
    """Intake source harvester, takes a provider (for nested catalog use the catalog
    name.source, i.e. bodleian.arabic), generates a Pandas DataFrame, runs
    validations, and saves a copy of the DataFrame.

    @param -- provider
    """

    if PROVIDER not in kwargs:
        raise ValueError("Missing provider argument.")

    source_provider = provider_key(**kwargs)

    source = catalog_for_provider(source_provider)
    logging.info(f"SOURCE_PROVIDER = {source_provider}")
    logging.info(f"source = {source}")

    try:
        logging.info("trying")
        has_sources = iter(list(source))
        logging.info(f"source = {has_sources}")
        for collection in has_sources:
            data_source_harvester(provider=kwargs[PROVIDER], collection=collection)
    except TypeError:
        dataframe_to_file(source, kwargs[PROVIDER], kwargs.get(COLLECTION, None))
