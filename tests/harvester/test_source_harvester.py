from pytest_mock import MockerFixture

import pandas as pd

from dlme_airflow.harvester.source_harvester import data_source_harvester
from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider


class MockTaskInstance:
    def __init__(self):
        self.xcom_dict = {}

    def xcom_push(self, key: str, value):
        self.xcom_dict[key] = value


def test_source_harvester():
    assert data_source_harvester


def test_dataframe_retrieval_and_profiling(mocker: MockerFixture):
    collection = Collection(Provider("aims"), "aims")
    mock_task_instance = MockTaskInstance()

    column_headers = ["id", "source"]
    rows = [
        column_headers,
        ["1", ["Rare Books and Special Collections Library"]],
        ["2", ["Rare Books and Special Collections Library"]],
    ]

    mock_df = pd.DataFrame(rows)
    mock_csv_path = "/metadata/data_path/data.csv"

    patched_dataframe_to_file = mocker.patch(
        "dlme_airflow.harvester.source_harvester.dataframe_to_file",
        return_value={"source_df": mock_df, "working_csv": mock_csv_path},
    )
    data_source_harvester(mock_task_instance, collection=collection)

    patched_dataframe_to_file.assert_any_call(collection)
    assert mock_task_instance.xcom_dict["dataframe_stats"] == {"record_count": 2}
