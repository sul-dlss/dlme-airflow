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
    if not os.path.exists(current_metadata):
        return 'sync_metadata'
    
    current_df = dataframe_from_file('csv', current_metadata)

    if current_df.equals(working_df):
        return 'equal_metadata'
    
    return 'sync_metadata'
