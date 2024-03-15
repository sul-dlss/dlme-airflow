import pytest
import pandas
import shutil
import itertools

from dlme_airflow.utils.filter_dataframe import filter_dataframe
from dlme_airflow.models.provider import Provider
from dlme_airflow.models.collection import Collection


@pytest.fixture
def mock_collection_datafile(monkeypatch, tmp_path):
    def mock_datafile(_self, format):
        tmp_file = f"{tmp_path}/princeton.json"
        shutil.copy("tests/data/json/princeton/unfiltered.json", tmp_file)
        return tmp_file

    monkeypatch.setattr(Collection, "datafile", mock_datafile)


def test_filter_exclude_filter_with_sets(mocker, mock_collection_datafile, tmp_path):
    provider = Provider("princeton")
    params = {"collection": provider.get_collection("islamic_manuscripts")}
    source_dataframe = pandas.read_json("tests/data/json/princeton/unfiltered.json")

    assert f"{tmp_path}/princeton.json" in filter_dataframe(**params)

    filtered_dataframe = pandas.read_json(f"{tmp_path}/princeton.json")
    assert source_dataframe.shape[0] == 2
    assert filtered_dataframe.shape[0] == 1
    # This test confirms that the only records remaining are those where
    # member-of-collections does not include "Yemeni Manuscript Digitization Initiative"
    assert "Yemeni Manuscript Digitization Initiative" in list(
        itertools.chain(*source_dataframe["member-of-collections"].tolist())
    )
    assert "Yemeni Manuscript Digitization Initiative" not in list(
        itertools.chain(*filtered_dataframe["member-of-collections"].tolist())
    )
