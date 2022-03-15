# /bin/python
import logging
import os
import requests
import pandas as pd

from utils.catalog import catalog_for_provider


def get_thumbnail_url(id):
    id = id.split(":")[-1]
    url = f"https://medihal.archives-ouvertes.fr/IFPOIMAGES/{id}"
    try:
        req = requests.get(url)
        return f"{req.url}/large"
    except:  # noqa: E722
        logging.info(f"Unable to connect to: {url}")


def add_thumbnail_urls(**kwargs):
    # Fetch working directory path from catalog and read file into Pandas dataframe
    catalog = catalog_for_provider("ifpo")
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = catalog.metadata.get("data_path", "ifpo/photographs")
    working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    df = pd.read_csv(working_csv)
    df["thumbnail"] = df.apply(lambda row: get_thumbnail_url(row["id"]), axis=1)
    df.to_csv(working_csv)
