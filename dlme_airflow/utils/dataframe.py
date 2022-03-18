import boto3
import logging
import os
import pandas as pd


# TODO: If not files are found / dir is empty / etc, this raising an error.
#       We should handle this error more cleanly.
def dataframe_from_file(driver: str, data_file_path: str) -> pd.DataFrame:
    """ "Returns existing DLME metadata as a Panda dataframe based on the
    type of driver.

    @param -- driver The registered DataSource name
    @param -- existing_dir
    """
    for driver_type in ["csv", "json"]:
        if driver.endswith(driver_type):
            read_func = getattr(pd, f"read_{driver_type}")
            return read_func(data_file_path)


# TODO: An Error is thrown on line 22 if working_directory is not found in
#       the metadata. Need to handle this error.
def dataframe_to_file(dataframe, provider, collection):
    session = boto3.Session()
    s3 = session.resource("s3")

    # root_dir = os.path.dirname(os.path.abspath("metadata"))
    if collection:
        default_data_path = f"{provider}/{collection}"
    else:
        default_data_path = provider

    data_path = dataframe.metadata.get("data_path", default_data_path)

    # working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    s3_object = s3.Object("dlme_metadata", os.path.join(data_path, "data.csv"))

    # working_directory = os.path.join(root_dir, "working", data_path)
    # os.makedirs(working_directory, exist_ok=True)

    unique_id = (
        dataframe.metadata.get("fields").get("id").get("name_in_dataframe", "id")
    )
    source_df = dataframe.read().drop_duplicates(subset=[unique_id], keep="first")
    # source_df.to_csv(working_csv, index=False)
    result = s3_object.put(Body=source_df.to_csv)
    res = result.get("ResponseMetadata")
    logging.info(f"S3 Status Response: {res.get('HTTPStatusCode')}")
