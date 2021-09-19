from utils.dataframe import dataframe_from_file
from utils.catalog import catalog_for_provider


def check_equity(provider):
    source = catalog_for_provider(provider)
    working_csv = f"{source.metadata.get('working_directory')}/data.csv"

    working_df = dataframe_from_file('csv', working_csv)
    current_df = dataframe_from_file('csv', source.metadata.get("current_metadata"))

    if current_df.equals(working_df):
        return 'complete'
    
    return 'transform'
