import os
import pandas as pd
import boto3


# TODO: If not files are found / dir is empty / etc, this raising an error.
#       We should handle this error more cleanly.
def dataframe_from_s3(collection) -> pd.DataFrame:
    """Returns existing DLME metadata as a Pandas dataframe from S3

    @param -- data_path
    """
    data_path = collection.data_path()
    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET")
    key = f"{data_path}/data.csv"
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


# TODO: An Error is thrown on line 22 if working_directory is not found in
#       the metadata. Need to handle this error.
def dataframe_to_file(collection):
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = collection.data_path()

    working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    working_directory = os.path.join(root_dir, "working", data_path)
    os.makedirs(working_directory, exist_ok=True)

    unique_id = (
        collection.catalog.metadata.get("fields")
        .get("id")
        .get("name_in_dataframe", "id")
    )
    source_df = collection.catalog.read().drop_duplicates(
        subset=[unique_id], keep="first"
    )
    source_df.to_csv(working_csv, index=False)
