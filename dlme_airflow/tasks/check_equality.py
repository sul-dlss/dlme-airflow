import logging
import os
from utils.dataframe import dataframe_from_file
from utils.catalog import catalog_for_provider


def check_equity(provider):
    source = catalog_for_provider(provider)
    root_dir = os.path.dirname(os.path.abspath('metadata'))
    data_path = source.metadata.get('data_path', provider)

    working_csv = os.path.join(root_dir, 'working', data_path, 'data.csv')
    working_df = dataframe_from_file('csv', working_csv)

    current_metadata = os.path.join(root_dir, 'metadata', data_path, 'data.csv')
    current_df = dataframe_from_file('csv', current_metadata)

    return current_df.equals(working_df)  # We are returning the result of this check to auto-store in a xcom variable
