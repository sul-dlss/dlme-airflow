import pytest
import pandas

from dlme_airflow.utils.yale import filter_df, remove_babylonian_non_relevant
from dlme_airflow.models.provider import Provider


# This mock prevents writing to the fixture CSV when testing
@pytest.fixture
def mock_dataframe_to_csv(monkeypatch):
    def mock_to_csv(_self, _filename):
        return True

    monkeypatch.setattr(pandas.DataFrame, "to_csv", mock_to_csv)


def test_remove_babylonian_non_relevant(mocker, mock_dataframe_to_csv):
    provider = Provider("yale")
    params = {"collection": provider.get_collection("babylonian")}

    assert "working/yale/babylonian/data.json" in remove_babylonian_non_relevant(
        **params
    )


def test_filter_df():
    df = pandas.read_csv("tests/data/csv/yale.csv")

    # make sure the DataFrame looks how we expect
    assert df.shape[0] == 5

    # remove non MENA records
    df = filter_df(df)

    # make sure the correct rows were removed
    assert df.shape[0] == 3

    df = pandas.read_json("tests/data/json/yale.json")

    # make sure the DataFrame looks how we expect
    assert df.shape[0] == 7

    # remove non MENA records
    df = filter_df(df)

    # make sure the correct rows were removed
    assert df.shape[0] == 6
