import pytest

from dlme_airflow.utils.dataframe import dataframe_from_file
from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider


# This mock prevents writing to the fixture CSV when testing
@pytest.fixture
def mock_collection_datafile(monkeypatch):
    def mock_datafile(_, format):
        match format:
            case "json":
                return "tests/data/json/example1.json"
            case _:
                return "tests/data/csv/example1.csv"

    monkeypatch.setattr(
        "dlme_airflow.models.collection.Collection.datafile", mock_datafile
    )


def test_dataframe_from_csv_file(mock_collection_datafile):
    provider = Provider("aub")
    collection = Collection(provider, "aladab")

    assert dataframe_from_file(collection)["id"].count() == 4


def test_dataframe_from_json_file(mock_collection_datafile):
    provider = Provider("aub")
    collection = Collection(provider, "aladab")

    assert dataframe_from_file(collection, "json")["id"].count() == 2
