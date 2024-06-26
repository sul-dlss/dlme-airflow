from dlme_airflow.utils.dataframe import dataframe_to_file

from airflow.utils.state import DagRunState

COLLECTION = "collection"
PROVIDER = "provider"


def data_source_harvester(task_instance, **kwargs):
    """Intake source harvester, takes a Collection object, generates
    a Pandas DataFrame from a harvest of it, and saves a copy of the
    DataFrame.  Also, pushes some basic stats about the dataframe for
    use in e.g. downstream validation of transform output.
    """

    last_dagrun = task_instance.get_previous_dagrun(DagRunState.SUCCESS)
    if last_dagrun is not None:
        last_harvest_start_date = last_dagrun.start_date
    else:
        last_harvest_start_date = None

    collection = kwargs.get("collection")

    df_and_csv = dataframe_to_file(collection, last_harvest_start_date)
    df = df_and_csv["source_df"]
    working_csv = df_and_csv["working_csv"]
    dataframe_stats = {"record_count": df.shape[0]}
    task_instance.xcom_push(key="dataframe_stats", value=dataframe_stats)

    assert len(df) > 0, f"DataFrame for {collection.name} is empty"

    return working_csv
