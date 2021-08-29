import logging
import pathlib
import time
import os
import json
import intake
import pandas as pd


from harvester.validations import check_equality
from harvester.iiif_harvester import fetch

COLLECTION = 'collection'
PROVIDER = 'provider'

def catalog():
    catalog_file = os.getenv("CATALOG_SOURCE", "catalogs/catalog.yaml")
    logging.info(f"\tLoading catalog file {catalog_file}")
    return intake.open_catalog(catalog_file)


def output_path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


# TODO: If not files are found / dir is empty / etc, this raising an error.
#       We should handle this error more cleanly.
def get_local_dataframe(driver: str, data_file_path: pathlib.Path) -> pd.DataFrame:
    """"Returns existing DLME metadata as a Panda dataframe based on the
    type of driver.

    @param -- driver The registered DataSource name
    @param -- existing_dir
    """
    for driver_type in ["csv", "json"]:
        if driver.endswith(driver_type):
            read_func = getattr(pd, f"read_{driver_type}")
            # for data_file_path in dir.glob(f"*.{driver_type}"):
            return read_func(data_file_path)


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

    logging.info(f"\tStarting harvest of {source_provider}")
    source = getattr(catalog(), source_provider)  # Raises Attribute error for missing provider

    try:
        has_sources = iter(list(source))
        for collection in has_sources:
            data_source_harvester(provider=source_provider, collection=collection)
    except:
        working_directory = output_path(source.metadata.get("working_directory"))
        persist_to_csv = f"{working_directory}/data.csv"
        source_df = source.read()
        source_df.to_csv(persist_to_csv, index=False)
        logging.info(f"Source columns: {source_df.columns}")
        logging.info(f"Source count: {len(source_df)}")
        # source_df.set_index('id').to_json(persist_to_json, orient='index')
        persisted_df = get_local_dataframe('csv', pathlib.Path(persist_to_csv))
        current_df = get_local_dataframe('csv', pathlib.Path(source.metadata.get("current_metadata")))
        logging.info(f"Current columns: {current_df.columns}")
        logging.info(f"Count count: {len(current_df)}")
        check_equality(current_df, persisted_df)

        # IF csv dataframes are equal - DONE
        # IF csv dataframes are not equal 
        #   compare lengths
        #   compare columns
