import pytest
import pandas
import shutil

from dlme_airflow.utils.filter_dataframe import filter_dataframe
from dlme_airflow.models.provider import Provider
from dlme_airflow.models.collection import Collection


@pytest.fixture
def mock_collection_datafile(monkeypatch, tmp_path):
    def mock_datafile(_self, format):
        tmp_file = f"{tmp_path}/yale.json"
        shutil.copy("tests/data/json/yale/unfiltered.json", tmp_file)
        return tmp_file

    monkeypatch.setattr(Collection, "datafile", mock_datafile)


def test_filter_yale_babylonian(mocker, mock_collection_datafile, tmp_path):
    provider = Provider("yale")
    params = {"collection": provider.get_collection("babylonian")}
    source_dataframe = pandas.read_json("tests/data/json/yale/unfiltered.json")

    assert f"{tmp_path}/yale.json" in filter_dataframe(**params)

    filtered_dataframe = pandas.read_json(f"{tmp_path}/yale.json")
    assert source_dataframe.shape[0] == 20
    assert filtered_dataframe.shape[0] == 7
    # This test confirms that the only records remaining are those with geographic_country = "Egypt"
    # The test data set inlcuded Italy, Mexico, and Egypt. The filter removed Italy and Mexico
    assert set(source_dataframe.geographic_country).difference(
        set(filtered_dataframe.geographic_country)
    ) == {"Italy", "Mexico"}
    assert set(filtered_dataframe.geographic_country) == {"Egypt"}
