# /bin/python
import os
import pandas as pd

from dlme_airflow.utils.catalog import get_working_csv

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
    coll = kwargs["collection"]
    data_path = coll.data_path()
    working_csv = get_working_csv(data_path)

    if os.path.isfile(working_csv):
        df = pd.read_csv(working_csv)
        # Filter out non relevant records and over write the csv
        df = df[~df["geographic_country"].isin(NON_RELEVANT_COUNTRIES)]
        df.to_csv(working_csv)

    return working_csv
