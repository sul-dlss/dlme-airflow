import boto3
import botocore.session
import io
import os
import pytest
import mock

from botocore.response import StreamingBody
from botocore.stub import Stubber
from dlme_airflow.tasks.harvest_validator import validate_harvest
from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider


@pytest.fixture
def mock_boto3(monkeypatch):
    def mock_client(_type):
        expected_message = (
            b"id,title\n1,Example 1\n2,Example 2\n3,Example 3\n4,Example 4"
        )

        raw_stream = StreamingBody(io.BytesIO(expected_message), len(expected_message))

        response = {"Body": raw_stream}

        expected_params = {
            "Bucket": "test-bucket",
            "Key": "metadata/aims/data.csv",
        }

        s3 = botocore.session.get_session().create_client("s3")
        stubber = Stubber(s3)

        stubber.add_response("get_object", response, expected_params)
        stubber.activate()

        return s3

    monkeypatch.setattr(boto3, "client", mock_client)


# @pytest.fixture
# def mock_collection(monkeypatch):
#     def mock_data_path(self):
#         return "provider/collection"

#     monkeypatch.setattr(Collection, "data_path", mock_data_path)


def test_validate_harvest_skip_load_data(mock_boto3):
    os.environ["S3_BUCKET"] = "test-bucket"
    test_provider = Provider("aims")
    test_collection = Collection(test_provider, "aims")
    mock_task_instance = mock.Mock()
    mock_task = mock.Mock()
    mock_task.task_id = "AIMS_ETL.aims_etl.aims_aims_validate_harvest"
    mock_task_instance.xcom_pull.return_value = [
        os.path.join(os.path.abspath("tests"), "data", "csv", "example1.csv")
    ]
    mock_task.upstream_task_ids = ["task_id_1", "task_id_2"]
    next_task = validate_harvest(
        mock_task_instance, mock_task, collection=test_collection
    )

    assert next_task == "AIMS_ETL.aims_etl.skip_load_data"


def test_validate_harvest_load_data(mock_boto3):
    os.environ["S3_BUCKET"] = "test-bucket"
    test_provider = Provider("aims")
    test_collection = Collection(test_provider, "aims")
    mock_task_instance = mock.Mock()
    mock_task = mock.Mock()
    mock_task.task_id = "AIMS_ETL.aims_etl.aims_aims_validate_harvest"
    mock_task_instance.xcom_pull.return_value = [
        os.path.join(os.path.abspath("tests"), "data", "csv", "example2.csv")
    ]
    mock_task.upstream_task_ids = ["task_id_1", "task_id_2"]
    next_task = validate_harvest(
        mock_task_instance, mock_task, collection=test_collection
    )

    assert next_task == "AIMS_ETL.aims_etl.load_data"
