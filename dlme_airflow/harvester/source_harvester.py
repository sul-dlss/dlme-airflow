from dlme_airflow.utils.dataframe import dataframe_to_file

COLLECTION = "collection"
PROVIDER = "provider"


def data_source_harvester(task_instance, **kwargs):
    """Intake source harvester, takes a Collection object, generates
    a Pandas DataFrame from a harvest of it, and saves a copy of the
    DataFrame.  Also, pushes some basic stats about the dataframe for
    use in e.g. downstream validation of transform output.
    """

    collection = kwargs.get("collection")
    df_and_csv = dataframe_to_file(collection)
    df = df_and_csv["source_df"]
    working_csv = df_and_csv["working_csv"]
    dataframe_stats = {"record_count": df.shape[0]}
    task_instance.xcom_push(key="dataframe_stats", value=dataframe_stats)
    return working_csv
