from dlme_airflow.utils.dataframe import dataframe_to_file

COLLECTION = "collection"
PROVIDER = "provider"


def data_source_harvester(task_instance, **kwargs):
    """Intake source harvester, takes a provider (for nested catalog use the catalog
    name.source, i.e. bodleian.arabic), generates a Pandas DataFrame, runs
    validations, and saves a copy of the DataFrame.

    @param -- provider
    """

    collection = kwargs.get("collection")
    # dataframe_to_file(source, collection.provider.name, collection)
    df_and_csv = dataframe_to_file(collection)
    df = df_and_csv["source_df"]
    working_csv = df_and_csv["working_csv"]
    dataframe_stats = {"record_count": df.shape[0]}
    task_instance.xcom_push(key="dataframe_stats", value=dataframe_stats)
    return working_csv
