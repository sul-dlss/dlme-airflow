# /bin/python
import os
import pandas
import logging
import json

from typing import Optional
from dlme_airflow.utils.schema import get_schema


def add_thumbnails(**kwargs) -> None:
    """Add a thumbnail column based on the value in the url column"""
    coll = kwargs["collection"]
    data_path = coll.data_path()
    if data_path is None:
        raise Exception(f"unable to find data_path for collection={coll}")

    # ensure that the working csv is present on the filesystem
    working_csv = coll.datafile("csv")
    if not os.path.isfile(working_csv):
        raise Exception(f"unable to locate working CSV data: {working_csv}")

    # add a thumbnail column and save it
    df = pandas.read_csv(working_csv)
    df["thumbnail"] = df.emuIRN.apply(get_thumbnail)
    df.to_csv(working_csv)


def get_thumbnail(id) -> Optional[str]:
    url = f"https://www.penn.museum/collections/object/{id}"
    logging.info(f"getting thumbnail for {url}")
    try:
        schema = get_schema(url)
        if schema:
            return schema.get("thumbnailUrl")
        else:
            return None
    except json.JSONDecodeError as e:
        logging.error(f"JSONDecodeError occurred: %s for record {url}", e)
