from utils.dataframe import dataframe_to_file
from utils.catalog import catalog_for_provider

COLLECTION = 'collection'
PROVIDER = 'provider'


def provider_key(**kwargs):
    if COLLECTION not in kwargs:
        return kwargs[PROVIDER]
    
    return f"{kwargs[PROVIDER]}.{kwargs[COLLECTION]}"


def data_source_harvester(**kwargs): # (provider: str, collection: str):
    """Intake source harvester, takes a provider (for nested catalog use the catalog
    name.source, i.e. bodleian.arabic), generates a Pandas DataFrame, runs
    validations, and saves a copy of the DataFrame.

    @param -- provider
    """

    source_provider = provider_key(**kwargs)

    source = catalog_for_provider(source_provider)

    try:
        has_sources = iter(list(source))
        for collection in has_sources:
            data_source_harvester(provider=source_provider, collection=collection)
    except:
        dataframe_to_file(source)
