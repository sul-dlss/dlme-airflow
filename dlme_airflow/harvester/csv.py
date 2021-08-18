import logging

import intake
import pandas as pd

from harvester.validations import check_equality

catalog = intake.open_catalog("catalog.yaml")


def csv_harvester(provider: str):
    """CSV Harvester takes a YAML configuration file, loads a csv file from a
    URL or filesystem, and returns the result as a Pandas Dataframe

    @param provider -- Data provider
    """
    logging.info(f"Started csv harvest for {provider}")
    if provider not in catalog:
        raise ValueError(f"{provider} not found in catalog")
    csv_source = getattr(catalog, provider)
    csv_df = csv_source.read()
    existing_df = pd.concat(
        [pd.read_csv(r) for r in csv_source.metadata.get("current")]
    )
    logging.info(f"{provider} start check_equality")
    check_equality(existing_df, csv_df)
    logging.info(f"{provider} finished check equality")
