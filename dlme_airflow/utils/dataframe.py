import os
import pandas as pd


# TODO: If not files are found / dir is empty / etc, this raising an error.
#       We should handle this error more cleanly.
def dataframe_from_file(driver: str, data_file_path: str) -> pd.DataFrame:
    """"Returns existing DLME metadata as a Panda dataframe based on the
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
def dataframe_to_file(dataframe, provider):
    root_dir = os.path.dirname(os.path.abspath('metadata'))
    data_path = dataframe.metadata.get('data_path', provider)

    working_csv = os.path.join(root_dir, 'working', data_path, 'data.csv')
    working_directory = os.path.join(root_dir, 'working', data_path)
    os.makedirs(working_directory, exist_ok=True)

    source_df = dataframe.read()
    source_df.to_csv(working_csv, index=False)
