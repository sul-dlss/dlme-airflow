import logging

from utils.catalog import catalog_for_provider
from harvester.source_harvester import provider_key

COLLECTION = "collection"
PROVIDER = "provider"


def data_source_post_harvester(**kwargs):
    """Intake source post harvester, takes a provider (for nested catalog use the catalog
    name.source, i.e. bodleian.arabic), and fetches any existing post harvest scripts in the
    catalog.

    @param -- provider
    """

    logging.info("data_source_post_harvester function is executing.")

    if PROVIDER not in kwargs:
        raise ValueError("Missing provider argument.")

    source_provider = provider_key(**kwargs)
    logging.info("Build data_source_post_harvester task.")

    source = catalog_for_provider(source_provider)
    logging.info(f"SOURCE_PROVIDER = {source_provider}")
    logging.info(f"source = {source}")
    data_source_post_harvester(
        provider=kwargs[PROVIDER],
        collection=kwargs.get(COLLECTION, None),
        post_harvest="/opt/dlme_airflow/utils/ifpo_get_thumbnail_urls.py",
    )
