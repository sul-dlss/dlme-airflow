import intake
import os
from intake.catalog.exceptions import ValidationError


def catalog_file():
    return os.getenv("CATALOG_SOURCE", "catalogs/catalog.yaml")


def fetch_catalog():
    try:
        catalog = intake.open_catalog(catalog_file())
    except ValidationError:
        return []
    
    return catalog


def catalog_for_provider(provider):
    return getattr(fetch_catalog(), provider)  # Raises Attribute error for missing provider
