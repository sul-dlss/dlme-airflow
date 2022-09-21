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
from mock import patch
from airflow.models.taskinstance import TaskInstance
from airflow.models.baseoperator import BaseOperator

@pytest.fixture
def mock_boto3(monkeypatch):
    def mock_client(_type):
        expected_message = b"column1,column2,column3\n1,2,3\n4,5,6"

        raw_stream = StreamingBody(io.BytesIO(expected_message), len(expected_message))

        response = {"Body": raw_stream}

        expected_params = {
            "Bucket": "test-bucket",
            "Key": "metadata/provider/collection/data.csv",
        }

        s3 = botocore.session.get_session().create_client("s3")
        stubber = Stubber(s3)

        stubber.add_response("get_object", response, expected_params)
        stubber.activate()

        return s3

    monkeypatch.setattr(boto3, "client", mock_client)


@pytest.fixture
def mock_provider(monkeypatch):
    monkeypatch.setattr(Provider, "name", "provider")


@pytest.fixture
def mock_collection(monkeypatch, mock_provider):
    monkeypatch.setattr(Collection, "provider", mock_provider)
    monkeypatch.setattr(Collection, "name", "collection")
    monkeypatch.setattr(Collection, "data_path", "provider/collection")

def test_validate_harvest(mock_boto3, mock_collection):
    # with mock.patch("dlme_airflow.models.provider.Provider") as mock:, new_callable=mock.PropertyMock, return_value="my value"):
    # @mock.patch('Provider.name', return_value="provider")
    # @mock.patch('Collection.name', return_value="collection")
    os.environ["S3_BUCKET"] = "test-bucket"
    mock_task_instance = mock.Mock()
    mock_task = mock.Mock()
    mock_task_instance.xcom_pull.return_value = [os.path.join(os.path.abspath("tests"), "data", "csv", "example1.csv")]
    mock_task.upstream_task_ids = ["task_id_1", "task_id_2"]
    with patch("dlme_airflow.models.provider.Provider.name", new_callable=mock.PropertyMock, return_value="provider") as mock_provider_name:
    # with patch("dlme_airflow.models.provider.Provider.name", new_callable=mock.PropertyMock, return_value="provider") as mock_provider_name:
    #   with patch("dlme_airflow.models.provider.Provider") as mock_provider:
    #       mock_provider.name.return_value = "provider"
    #       with patch("dlme_airflow.models.collection.Collection") as mock_collection:
    #         mock_collection.data_path.return_value = "provider/collection"
    #         mock_collection.provider.return_value = mock_provider
    #         mock_collection.name.return_value = "collection"
    next_task = validate_harvest(mock_task_instance, mock_task, collection = mock_collection)

    assert next_task == "something.something.something"

    # mock_collection = mock.Mock()
    # mock_provider = mock.Mock()
    # mock_provider.name.return_value = "provider"
    # mock_collection.data_path.return_value = "provider/collection"
    # mock_collection.provider.return_value = mock_provider
    # mock_collection.name.return_value = "collection"
