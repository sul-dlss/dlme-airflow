import logging

from utils.dataframe import dataframe_to_file
from utils.catalog import catalog_for_provider

COLLECTION = 'collection'
PROVIDER = 'provider'


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
        raise ValueError('Missing provider argument.')

    source_provider = provider_key(**kwargs)

    source = catalog_for_provider(source_provider)
    logging.info(f"SOURCE_PROVIDER = {source_provider}")

    try:
        has_sources = iter(list(source))
        for collection in has_sources:
            data_source_harvester(provider=kwargs[PROVIDER], collection=collection)
    except TypeError:
        dataframe_to_file(source, kwargs[PROVIDER], kwargs.get(COLLECTION, None))
