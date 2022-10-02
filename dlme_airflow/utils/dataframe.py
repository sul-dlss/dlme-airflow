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
    # TODO: This needs to take creds. See: https://stackoverflow.com/a/45982080/7600626
    #    -- see how to make this work with localstack so we don't have to hammer S3 for local dev.
    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET").replace("s3://", "")
    key = f"metadata/{data_path}/data.csv"
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


# TODO: An Error is thrown on line 22 if working_directory is not found in
#       the metadata. Need to handle this error.
def dataframe_to_file(collection):
    working_csv = os.path.join(
        os.path.abspath("metadata"), collection.data_path(), "data.csv"
    )
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

    return working_csv
