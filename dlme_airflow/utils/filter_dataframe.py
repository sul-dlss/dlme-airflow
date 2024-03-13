# /bin/python
import os
import pandas as pd
from dlme_airflow.utils.read_df import read_datafile_with_lists


def filter_by_field(df, field, permitted_values):
    """Filter records by field values."""
    return df[df[field].isin(permitted_values)]


def filter_dataframe(**kwargs):
    """Called by the Airflow workflow to apply field filters to a DataFrame."""
    collection = kwargs["collection"]
    data_file = collection.datafile("json")
    filters = collection.filters()

    if os.path.isfile(data_file):
        df = read_datafile_with_lists(data_file)
        for field, permitted_values in filters.items():
            df = filter_by_field(df, field, permitted_values)
    
        df.to_json(data_file, orient="records", force_ascii=False)

    return data_file