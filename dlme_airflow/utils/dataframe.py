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
    s3 = s3_resource_builder()

    # root_dir = os.path.dirname(os.path.abspath("metadata"))
    if collection:
        default_data_path = f"{provider}/{collection}"
    else:
        default_data_path = provider

    data_path = dataframe.metadata.get("data_path", default_data_path)

    s3_object = s3.Object(
        "dlme-metadata-dev", os.path.join("metadata", data_path, "data.csv")
    )

    unique_id = (
        dataframe.metadata.get("fields").get("id").get("name_in_dataframe", "id")
    )
    source_df = dataframe.read().drop_duplicates(subset=[unique_id], keep="first")

    try:
        result = s3_object.put(Body=source_df.to_csv())
        res = result.get("ResponseMetadata")
        logging.info(f"S3 Status Response: {res.get('HTTPStatusCode')}")
    except Exception as err:
        logging.info(f"{err}")


def s3_resource_builder():
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_ID"),
        )
        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(
            RoleArn=os.getenv("DEV_ROLE_ARN"), RoleSessionName="DevelopersRole"
        )
        credentials = assumed_role_object["Credentials"]

        return session.resource(
            "s3",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
    else:
        return boto3.Session().resource("s3")
