# /bin/python
import os
from dlme_airflow.utils.read_df import read_datafile_with_lists


# Objects from these countries will be suppressed
NON_RELEVANT_COUNTRIES = [
    "Canada",
    "China",
    "Ecuador",
    "Guatemala",
    "Greece",
    "Honduras",
    "Italy",
    "Korea",
    "Malaysia",
    "Mexico",
    "Peru",
    "South Africa",
    "USA",
]


def remove_babylonian_non_relevant(**kwargs):
    """Called by the Airflow workflow to merge records in multiple languages"""
    collection = kwargs["collection"]
    data_file = collection.datafile("json")
    if os.path.isfile(data_file):
        df = read_datafile_with_lists(data_file)
        df = filter_df(df)
        df.to_json(data_file, orient="records", force_ascii=False)

    return data_file


def filter_df(df):
    df = df[~df["geographic_country"].isin(NON_RELEVANT_COUNTRIES)]

    return df
