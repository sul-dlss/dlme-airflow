import pandas
import pytest

from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider
from dlme_airflow.utils.dataframe import dataframe_from_file, dataframe_to_file


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


class FakeCatalog:
    def __init__(self, df, metadata):
        self.df = df
        self.metadata = metadata

    def read(self):
        return self.df


class FakeCollection:
    def __init__(self, tmp_path, metadata):
        self.tmp_path = tmp_path
        df = pandas.DataFrame(
            {
                "id": ["a", "b", "c", "d"],
                "section": ["Egyptian", "Asian", "Egyptian", "Babylonian"],
            }
        )
        self.catalog = FakeCatalog(df, metadata)

    def datafile(self, format):
        return str(self.tmp_path / f"data.{format}")

    def filters(self):
        return self.catalog.metadata.get("filters", {})


FILTERS = {"section": {"include": ["Egyptian"]}}


def test_dataframe_to_file_filters_on_harvest(tmp_path):
    collection = FakeCollection(
        tmp_path,
        {"fields": {"id": {}}, "filters": FILTERS, "filter_on_harvest": True},
    )

    result = dataframe_to_file(collection)

    assert list(result["source_df"]["id"]) == ["a", "c"]
    assert list(pandas.read_json(tmp_path / "data.json")["id"]) == ["a", "c"]


def test_dataframe_to_file_leaves_filtering_to_filter_task_by_default(tmp_path):
    collection = FakeCollection(tmp_path, {"fields": {"id": {}}, "filters": FILTERS})

    result = dataframe_to_file(collection)

    assert len(result["source_df"]) == 4
