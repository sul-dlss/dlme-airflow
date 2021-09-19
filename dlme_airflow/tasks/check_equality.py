from utils.dataframe import dataframe_from_file
from utils.catalog import catalog_for_provider


def check_equity(provider, collection):
    source = catalog_for_provider(f"{provider}.{collection}")
    working_csv = f"/opt/airflow/working/{source.metadata.get('data_path')}/data.csv"
    current_csv = f"/opt/airflow/metadata/{source.metadata.get('data_path')}/data.csv"

    working_df = dataframe_from_file('csv', working_csv)
    current_df = dataframe_from_file('csv', current_csv)

    if current_df.equals(working_df):
        return f"{provider}.etl.pipeline.{collection}.complete"
    
    return f"{provider}.etl.pipeline.{collection}.transform"
