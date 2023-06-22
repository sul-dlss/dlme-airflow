# /bin/python
import os
import pandas as pd


def remove_ymdi(**kwargs):
    coll = kwargs["collection"]
    working_csv = coll.datafile("csv")

    if os.path.isfile(working_csv):
        df = pd.read_csv(working_csv)
        filter_df(df)
        df.to_csv(working_csv)

    return working_csv


def filter_df(df):
    # Filter out ymdi records and overwrite the csv
    df = df[
        ~df["member-of-collections"].str.contains(
            "Yemeni Manuscript Digitization Initiative"
        )
    ]

    return df
