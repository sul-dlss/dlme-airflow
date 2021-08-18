import logging
import pathlib

# import time

import intake
import pandas as pd

from harvester.validations import check_equality

catalog = intake.open_catalog("catalog.yaml")


def iiif_json_harvester(provider: str):
    """IIIf JSON harvester, takes a provider (for nested catalog use the catalog
    name.source, i.e. bodleian.arabic) extracts and

    @param -- provider
    """
    logging.info(f"Started iiif JSON harvest for {provider}")
    if provider not in catalog:
        raise ValueError(f"{provider} not found in catalog")
    source = getattr(catalog, provider)
    source_df = source.read()
    existing_directory = pathlib.Path(source.metadata.current_directory)
    existing_df = pd.concat(
        [pd.read_json(json_path) for json_path in existing_directory.glob("*.json")]
    )
    logging.info(f"{provider} start check_equality")
    check_equality(existing_df, source_df)
    logging.info(f"{provider} finished check equality")
    # Pattern for persisting DataFrame to the local container, we could
    # get the directory from the config
    # source_df_path = f"/opt/airflow/{provider}-{time.time()}.csv"
    # logging.info(f"persisting to {source_df_path}")
    # source_df.to_csv(source_df_path)
