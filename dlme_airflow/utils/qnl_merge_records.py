# /bin/python
import os
import pandas as pd


def merge_records(**kwargs):
    coll = kwargs["collection"]
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = coll.data_path()
    working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    df = pd.read_csv(working_csv)

    # merge rows with the same shelf locator
    df_merged = df.groupby("location_shelfLocator").agg(lambda x: x.tolist())

    df_merged.to_csv(working_csv)
