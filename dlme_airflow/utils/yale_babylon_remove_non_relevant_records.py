# /bin/python
import os
import pandas as pd

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


def remove_non_relevant(**kwargs):
    coll = kwargs["collection"]
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = coll.catalog.metadata.get("data_path")
    working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    df = pd.read_csv(working_csv)
    # Filter out non relevant records and over write the csv
    df = df[~df["geographic_country"].isin(NON_RELEVANT_COUNTRIES)]

    df.to_csv(working_csv)
