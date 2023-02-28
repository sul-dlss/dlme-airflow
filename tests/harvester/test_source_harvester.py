import datetime
import pandas as pd

from pytest_mock import MockerFixture
from dlme_airflow.harvester.source_harvester import data_source_harvester
from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider

last_harvest_start_date = datetime.datetime.now()


class MockTaskInstance:
    def __init__(self):
        self.xcom_dict = {}

    def xcom_push(self, key: str, value):
        self.xcom_dict[key] = value

    def get_previous_dagrun(self, state):
        return MockDagRun()


class MockDagRun:
    def __init__(self):
        self.start_date = last_harvest_start_date


def test_source_harvester():
    assert data_source_harvester


def test_dataframe_retrieval_and_profiling(mocker: MockerFixture):
    collection = Collection(Provider("aims"), "aims")
    mock_task_instance = MockTaskInstance()

    column_headers = ["id", "source"]
    rows = [
        ["1", ["Rare Books and Special Collections Library"]],
        ["2", ["Rare Books and Special Collections Library"]],
    ]

    mock_df = pd.DataFrame(data=rows, columns=column_headers)
    mock_csv_path = "/metadata/data_path/data.csv"

    patched_dataframe_to_file = mocker.patch(
        "dlme_airflow.harvester.source_harvester.dataframe_to_file",
        return_value={"source_df": mock_df, "working_csv": mock_csv_path},
    )
    data_source_harvester(mock_task_instance, collection=collection)

    # ensure that dataframe_to_file gets the last_harvest_start_date
    patched_dataframe_to_file.assert_any_call(collection, last_harvest_start_date)
    assert mock_task_instance.xcom_dict["dataframe_stats"] == {"record_count": 2}
