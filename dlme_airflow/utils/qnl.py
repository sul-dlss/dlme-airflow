# /bin/python
import os
import pandas
from dlme_airflow.utils.read_df import read_datafile_with_lists

from itertools import chain


def merge_records(**kwargs):
    """Called by the Airflow workflow to merge records in multiple languages"""
    collection = kwargs["collection"]
    data_file = collection.datafile("json")
    if os.path.isfile(data_file):
        df = read_datafile_with_lists(data_file)
        df = merge_df(df)
        df.to_json(data_file, orient="records", force_ascii=False)

    return data_file


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
    df.id = df.id.apply(lambda source_record: source_record[0])

    return df
