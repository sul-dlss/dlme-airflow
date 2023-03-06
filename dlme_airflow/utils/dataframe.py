import os
import pandas as pd


def datafile_for_collection(collection):
    working_csv = os.path.join(
        os.path.abspath("working"), collection.data_path(), "data.csv"
    )

    return working_csv


# TODO: If not files are found / dir is empty / etc, this raising an error.
#       We should handle this error more cleanly.
def dataframe_from_file(collection) -> pd.DataFrame:
    """Returns existing DLME metadata as a Pandas dataframe from S3

    @param -- collection
    """
    return pd.read_csv(datafile_for_collection(collection))


# TODO: An Error is thrown on line 22 if working_directory is not found in
#       the metadata. Need to handle this error.
def dataframe_to_file(collection):
    working_csv = datafile_for_collection(collection)
    os.makedirs(os.path.dirname(working_csv), exist_ok=True)

    unique_id = (
        collection.catalog.metadata.get("fields")
        .get("id")
        .get("name_in_dataframe", "id")
    )
    source_df = collection.catalog.read().drop_duplicates(
        subset=[unique_id], keep="first"
    )
    source_df.to_csv(working_csv, index=False)

    return {"working_csv": working_csv, "source_df": source_df}
