# /bin/python
import os
import pandas

from itertools import chain


def merge_records(**kwargs):
    """Called by the Airflow workflow to merge records in multiple languages"""
    coll = kwargs["collection"]
    json_path = coll.datafile(format="json")
    if os.path.isfile(json_path):
        df = pandas.read_json(json_path, orient="records")
        df = merge_df(df)
        df.to_json(json_path, orient="records", force_ascii=False)
    return json_path


def merge_df(df) -> pandas.DataFrame:
    """Takes a DataFrame and returns a new DataFrame where row values have been
    merged using the location_shelfLocator column.
    """

    # put the id in a list so the groupby/agg doesn't split the string into a list
    df.id = df.id.apply(lambda id: [id])

    # replace NaN values with empty string to avoid warnings
    df = df.fillna("")

    # this pandas hocus-pocus groups by the call number, and then aggregate the
    # results while preserving the lists that are present
    df = df.groupby(
        by=lambda i: df.iloc[i].location_shelfLocator[0], as_index=False
    ).agg(lambda x: list(chain(*x)))

    # convert the id back into a string by using the first id value present in the list
    df.id = df.id.apply(lambda l: l[0])

    return df
