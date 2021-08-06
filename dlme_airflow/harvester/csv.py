import time

import intake
import pandas as pd

catalog = intake.open_catalog("catalog.yaml")


def csv_harvester(provider: str):
    """CSV Harvester takes a YAML configuration file, loads a csv file from a
    URL or filesystem, and returns the result as a Pandas Dataframe

    @param provider -- Data provider
    """
    if provider not in catalog:
        raise ValueError(f"{provider} not found in catalog")
    csv_source = getattr(catalog, provider)
    csv_df = csv_source.read()
    existing_df = pd.concat([pd.read_csv(r) for r in csv_source.metadata.get('current')])
    if csv_df.equals(existing_df) is False:
        raise ValueError(f"Harvested {provider} differs from existing")
    return csv_source
