import boto3
import botocore.session
import io
import os
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber
from dlme_airflow.utils.dataframe import dataframe_from_s3
from mock import patch


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


def test_dataframe_from_s3(mock_boto3):
    with patch("dlme_airflow.models.collection.Collection") as mock:
        collection = mock.return_value
        collection.data_path.return_value = "provider/collection"
        os.environ["S3_BUCKET"] = "test-bucket"

        df = dataframe_from_s3(collection)
        assert df.shape == (2, 3)
        assert not (df.to_numpy().flatten() - [1, 2, 3, 4, 5, 6]).all()
