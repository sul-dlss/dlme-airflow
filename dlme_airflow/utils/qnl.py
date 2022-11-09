# /bin/python
import os
import pandas

from itertools import chain


def merge_records(**kwargs):
    coll = kwargs["collection"]
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = coll.data_path()
    working_csv = os.path.join(root_dir, "metadata", data_path, "data.csv")
    df = pandas.read_csv(working_csv)
    df = merge_df(df)
    df.to_csv(working_csv)


def merge_df(df) -> pandas.DataFrame:
    df_filled = df.fillna("")
    df_merged = df_filled.groupby("location_shelfLocator", as_index=False).agg(
        lambda x: list(chain(*x))
    )
    return df_merged
