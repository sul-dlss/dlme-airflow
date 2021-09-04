import intake
import logging
import os


from harvester.validations import check_equality


def catalog_for_provider(provider):
    catalog_file = os.getenv("CATALOG_SOURCE", "catalogs/catalog.yaml")
    logging.info(f"\tLoading catalog file {catalog_file}")
    catalog = intake.open_catalog(catalog_file)
    return getattr(catalog(), provider)  # Raises Attribute error for missing provider
