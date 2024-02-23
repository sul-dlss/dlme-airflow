# /bin/python
import os
from dlme_airflow.utils.read_df import read_datafile_with_lists


def remove_ymdi(**kwargs):
    """Called by the Airflow workflow to merge records in multiple languages"""
    collection = kwargs["collection"]
    data_file = collection.datafile("json")
    if os.path.isfile(data_file):
        df = read_datafile_with_lists(data_file)
        df = filter_df(df)
        df.to_json(data_file, orient="records", force_ascii=False)

    return data_file


def filter_df(df):
    # Filter out ymdi records and overwrite the csv
    if type(df["member-of-collections"][0]) == str:
        df = df[
            ~df["member-of-collections"].str.contains(
                "Yemeni Manuscript Digitization Initiative"
            )
        ]
    else:
        df = df[
            ~df["member-of-collections"].map(
                set(["Yemeni Manuscript Digitization Initiative"]).issubset
            )
        ]

    return df
