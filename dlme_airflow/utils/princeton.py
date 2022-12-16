# /bin/python
import os
import pandas as pd

def remove_ymdi(**kwargs):
    coll = kwargs["collection"]
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = coll.data_path()
    working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    df = pd.read_csv(working_csv)
    # Filter out ymdi records and over write the csv
    df = df[~df["member-of-collections"].str.contains("Yemeni Manuscript Digitization Initiative")]

    df.to_csv(working_csv)
