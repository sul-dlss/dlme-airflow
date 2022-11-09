# /bin/python
import os
import numpy as np
import pandas as pd


def merge_records(**kwargs):
    coll = kwargs["collection"]
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = coll.data_path()
    working_csv = os.path.join(root_dir, "metadata", data_path, "data.csv")
    df = pd.read_csv(working_csv)
    df = merge_df(df)
    df.to_csv(working_csv)


def merge_lists(value):
    value = [x.replace("\n", "") for x in value]
    return (value[0] + value[1]).replace("][", ", ") if len(value) > 1 else value


def merge_df(df) -> pd.DataFrame:
    df_filled = df.fillna("NOT PROVIDED")
    df_merged = df_filled.groupby("location_shelfLocator", as_index=False).agg(
        lambda x: np.unique(x)
    )
    df_merged = df_merged.applymap(merge_lists)
    return df_merged
