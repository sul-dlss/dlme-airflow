# /bin/python
import os
import pandas as pd

from utils.catalog import catalog_for_provider

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


def yale_remove_non_relevant(**kwargs):
    # Fetch working directory path from catalog and read file into Pandas dataframe
    catalog = catalog_for_provider("yale")
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = catalog.metadata.get("data_path", "yale/babylon")
    working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    df = pd.read_csv(working_csv)
    # Filter out non relevant records and over write the csv
    df = df[~df["geographic_country"].isin(NON_RELEVANT_COUNTRIES)]

    df.to_csv(working_csv)
