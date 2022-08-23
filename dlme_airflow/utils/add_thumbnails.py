# /bin/python
import os
import pandas
import logging

from typing import Optional
from dlme_airflow.utils.schema import get_schema
from dlme_airflow.utils.catalog import catalog_for_provider


def add_thumbnails(**kwargs) -> None:
    "Add a thumbnail column based on the value in the url column"
    provider = catalog_for_provider(kwargs["provider"])
    coll = provider[kwargs["collection"]]

    # get the name for the harvested csv file
    data_path = coll.metadata.get("data_path")
    if data_path is None:
        raise Exception(
            f"unable to find data_path for provider={kwargs['provider']} collection={kwargs['collection']}"
        )

    # ensure that the harvested csv is present on the filesystem
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    if not os.path.isfile(working_csv):
        raise Exception(f"unable to locate working CSV data: {working_csv}")

    # add a thumbnail column and save it
    df = pandas.read_csv(working_csv)
    df["thumbnail"] = df.url.apply(get_thumbnail)
    df.to_csv(working_csv)


def get_thumbnail(url) -> Optional[str]:
    logging.info(f"getting thumbnail for {url}")
    schema = get_schema(url)
    if schema:
        return schema.get("thumbnailUrl")
    else:
        return None
