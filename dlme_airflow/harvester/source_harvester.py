import logging
import pathlib
import time
import os

import intake
import pandas as pd


from harvester.validations import check_equality


def catalog():
    catalog_file = os.getenv("CATALOG_SOURCE", "catalogs/catalog.yaml")
    return intake.open_catalog(catalog_file)


def get_existing_df(driver: str, existing_dir: pathlib.Path) -> pd.DataFrame:
    """"Returns existing DLME metadata as a Panda dataframe based on the
    type of driver.

    @param -- driver The registered DataSource name
    @param -- existing_dir
    """
    raw_data = []
    for driver_type in ["csv", "json"]:
        if driver.endswith(driver_type):
            read_func = getattr(pd, f"read_{driver_type}")
            for data_file_path in existing_dir.glob(f"*.{driver_type}"):
                try:
                    file_df = read_func(data_file_path)
                except ValueError:
                    file_df = pd.DataFrame()
                raw_data.append(file_df)
    return pd.concat(raw_data)


def data_source_harvester(provider: str):
    """Intake source harvester, takes a provider (for nested catalog use the catalog
    name.source, i.e. bodleian.arabic), generates a Pandas DataFrame, runs
    validations, and saves a copy of the DataFrame.

    @param -- provider
    """
    logging.info(f"Starting harvest of {provider}")
    source = getattr(catalog(), provider)  # Raises Attribute error for missing provider
    source_df = source.read()
    existing_directory = pathlib.Path(source.metadata.get("current_directory"))
    existing_df = get_existing_df(source._entry._driver, existing_directory)
    if len(existing_df) < 1:
        logging.error(f"Empty existing dataframe for {existing_directory}")
    logging.info(f"{provider} start check_equality")
    check_equality(existing_df, source_df)
    logging.info(f"{provider} finished check equality")
    working_dir = source.metadata.get("working_directory", "/opt/airflow/working/")
    source_df_path = f"{working_dir}{provider}-{time.time()}.csv"
    logging.info(f"Persisting to {source_df_path}")
    source_df.to_csv(source_df_path)
