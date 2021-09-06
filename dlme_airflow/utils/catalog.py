import intake
import logging
import os


def catalog_file():
    return os.getenv("CATALOG_SOURCE", "catalogs/catalog.yaml")

def fetch_catalog():
    return intake.open_catalog(catalog_file())

def catalog_for_provider(provider):
    return getattr(fetch_catalog(), provider)  # Raises Attribute error for missing provider
