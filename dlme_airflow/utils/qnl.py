# /bin/python
import os
import pandas

from itertools import chain
from dlme_airflow.utils.catalog import get_working_csv


def merge_records(**kwargs):
    """Called by the Airflow workflow to merge records in multiple languages"""
    coll = kwargs["collection"]
    data_path = coll.data_path()
    working_csv = get_working_csv(data_path)
    if os.path.isfile(working_csv):
        df = read_csv_with_lists(working_csv)
        df = merge_df(df)
        df.to_csv(working_csv)

    return working_csv


def read_csv_with_lists(path) -> pandas.DataFrame:
    """Reads a CSV and returns a Pandas DataFrame after having converted
    lists serialized as strings back into lists again.
    """
    df = pandas.read_csv(path)
    df = df.applymap(lambda v: eval(v) if type(v) == str else None)
    return df


def merge_df(df) -> pandas.DataFrame:
    """Takes a DataFrame and returns a new DataFrame where row values have been
    merged using the location_shelfLocator column.
    """
    df_filled = df.fillna("")
    df_merged = df_filled.groupby(
        by=lambda i: df.iloc[i].location_shelfLocator[0], as_index=False
    ).agg(lambda x: list(chain(*x)))
    return df_merged
