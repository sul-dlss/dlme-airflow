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

    # ensure that the working json is present on the filesystem
    working_json = coll.datafile("json")
    if not os.path.isfile(working_json):
        raise Exception(f"unable to locate working json data: {working_json}")

    # add a thumbnail column and save it
    df = pandas.read_json(working_json)
    df["thumbnail"] = df.emuIRN.apply(get_thumbnail)
    df.to_json(working_json)


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
