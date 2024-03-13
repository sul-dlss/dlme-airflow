# /bin/python
import os
from dlme_airflow.utils.read_df import read_datafile_with_lists


def filter_by_field(df, field, filters):
    """Filter records by field values."""
    if "include" in filters:
        df = df[df[field].isin(filters["include"])]

    if "exclude" in filters:
        df = df[~df[field].isin(filters["exclude"])]

    return df


def filter_dataframe(**kwargs):
    """Called by the Airflow workflow to apply field filters to a DataFrame."""
    collection = kwargs["collection"]
    data_file = collection.datafile("json")
    filters = collection.filters()

    if os.path.isfile(data_file):
        df = read_datafile_with_lists(data_file)
        for field, filters in filters.items():
            df = filter_by_field(df, field, filters)

        df.to_json(data_file, orient="records", force_ascii=False)

    return data_file
