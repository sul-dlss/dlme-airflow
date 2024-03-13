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


def test_filter_walters_records(mocker, mock_collection_datafile, tmp_path):
    provider = Provider("walters")
    params = {"collection": provider.get_collection("mena")}
    source_dataframe = pandas.read_json("tests/data/json/walters/unfiltered.json")

    assert f"{tmp_path}/walters.json" in filter_dataframe(**params)
    
    filtered_dataframe = pandas.read_json(f"{tmp_path}/walters.json")
    assert source_dataframe.shape[0] == 18
    assert filtered_dataframe.shape[0] == 15
    assert set(source_dataframe.ObjectID).difference(set(filtered_dataframe.ObjectID)) == {11, 14, 74}
