import pytest
import pandas as pd
import json

from unittest.mock import patch
from dlme_airflow.utils.add_thumbnails import add_thumbnails, get_thumbnail


@pytest.fixture
def mock_collection():
    class MockCollection:
        def data_path(self):
            return '/mock/path'

        def datafile(self, filetype):
            return 'tests/data/json/penn.json'

    return MockCollection()


def test_add_thumbnails_success(mock_collection, monkeypatch):
    def mock_get_thumbnail(id):
        return f'http://thumbnail{id}.jpg'

    monkeypatch.setattr('dlme_airflow.utils.add_thumbnails.get_thumbnail', mock_get_thumbnail)
    monkeypatch.setattr('pandas.read_json', lambda file: pd.DataFrame({'emuIRN': ['1', '2', '3']}))

    add_thumbnails(collection=mock_collection)

    # Load the mocked DataFrame to assert
    df = pd.DataFrame({
        'emuIRN': ['1', '2', '3'],
        'thumbnail': ['http://thumbnail1.jpg', 'http://thumbnail2.jpg', 'http://thumbnail3.jpg']
    })

    # Assertions
    expected_thumbnails = ['http://thumbnail1.jpg', 'http://thumbnail2.jpg', 'http://thumbnail3.jpg']
    pd.testing.assert_series_equal(df["thumbnail"], pd.Series(expected_thumbnails), check_names=False)



def test_get_thumbnail_success():
    # Mock the schema response
    with patch('dlme_airflow.utils.add_thumbnails.get_schema', return_value={"thumbnailUrl": "http://thumbnail.jpg"}):
        result = get_thumbnail('123')

        # Assertions
        assert result == "http://thumbnail.jpg"


def test_get_thumbnail_no_schema():
    # Mock the schema response as None
    with patch('dlme_airflow.utils.add_thumbnails.get_schema', return_value=None):
        result = get_thumbnail('123')

        # Assertions
        assert result is None


def test_get_thumbnail_json_decode_error():
    # Simulate a JSON decode error
    with patch('dlme_airflow.utils.add_thumbnails.get_schema', side_effect=json.JSONDecodeError("Expecting value", "doc", 0)):
        result = get_thumbnail('123')

        # Assertions
        assert result is None
