import pytest
import pandas
import shutil

from dlme_airflow.utils.filter_dataframe import filter_dataframe
from dlme_airflow.models.provider import Provider
from dlme_airflow.models.collection import Collection


@pytest.fixture
def mock_collection_datafile(monkeypatch, tmp_path):
    def mock_datafile(_self, format):
        tmp_file = f"{tmp_path}/walters.json"
        shutil.copy("tests/data/json/walters/unfiltered.json", tmp_file)
        return tmp_file

    monkeypatch.setattr(Collection, "datafile", mock_datafile)


def test_filter_walters_mena(mocker, mock_collection_datafile, tmp_path):
    provider = Provider("walters")
    params = {"collection": provider.get_collection("mena")}
    source_dataframe = pandas.read_json("tests/data/json/walters/unfiltered.json")

    assert f"{tmp_path}/walters.json" in filter_dataframe(**params)
    
    filtered_dataframe = pandas.read_json(f"{tmp_path}/walters.json")
    assert source_dataframe.shape[0] == 18
    assert filtered_dataframe.shape[0] == 15
    # This test confirms that the ObjectIDs 11, 14, and 74 are not in the filtered dataframe
    # ObjectID 11 because Culture = "Not Egyptian" which is not in the Culture filter list
    # ObjectIDs 14 and 74 because Collection = "Islamic Art" which are not in this Collection filter list
    assert set(source_dataframe.ObjectID).difference(set(filtered_dataframe.ObjectID)) == {11, 14, 74}


def test_filter_walters_mena_islamic_art(mocker, mock_collection_datafile, tmp_path):
    provider = Provider("walters")
    params = {"collection": provider.get_collection("mena_islamic_art")}
    source_dataframe = pandas.read_json("tests/data/json/walters/unfiltered.json")

    assert f"{tmp_path}/walters.json" in filter_dataframe(**params)
    
    filtered_dataframe = pandas.read_json(f"{tmp_path}/walters.json")
    assert source_dataframe.shape[0] == 18
    assert filtered_dataframe.shape[0] == 2
    # This test confirms that the ObjectIDs 14, and 74 are in the Collection filter list
    assert set(filtered_dataframe.ObjectID) == {14, 74}
