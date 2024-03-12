# /bin/python
import os
import pandas as pd
from dlme_airflow.utils.read_df import read_datafile_with_lists


def filter_by_field(df, field, permitted_values):
    """Filter records by field values."""
    return df[df[field].isin(permitted_values)]
