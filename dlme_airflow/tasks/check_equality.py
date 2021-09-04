from utils.dataframe import dataframe_from_file
from utils.catalog import catalog_for_provider


def check_equity(provider): # (provider: str, collection: str):
    source = catalog_for_provider(provider)
    working_csv = f"{source.metadata.get('working_directory')}/data.csv"

    working_df = dataframe_from_file('csv', working_csv)
    current_df = dataframe_from_file('csv', source.metadata.get("current_metadata"))
    
    return current_df.equals(working_df) # We are returning the result of this check to auto-store in a xcom variable
