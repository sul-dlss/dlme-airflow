import pytest
import pandas

from dlme_airflow.utils.walters import filter_df, remove_walters_non_relevant
from dlme_airflow.models.provider import Provider


# This mock prevents writing to the fixture CSV when testing
@pytest.fixture
def mock_dataframe_to_csv(monkeypatch):
    def mock_to_csv(_self, _filename):
        return True

    monkeypatch.setattr(pandas.DataFrame, "to_csv", mock_to_csv)


def test_remove_walters_non_relevant(mocker, mock_dataframe_to_csv):
    provider = Provider("walters")
    params = {"collection": provider.get_collection("mena")}

    assert "working/walters/mena/data.json" in remove_walters_non_relevant(**params)


def test_filter_df():
    df = pandas.read_csv("tests/data/csv/walters.csv")

    # make sure the DataFrame looks how we expect
    assert df.shape[0] == 4

    # remove non MENA records
    df = filter_df(df)

    # make sure the correct rows were removed
    assert df.shape[0] == 3
    assert set(df.ObjectID) == {14, 74, 98}

    df = pandas.read_json("tests/data/json/walters.json")

    # make sure the DataFrame looks how we expect
    assert df.shape[0] == 18

    # remove non MENA records
    df = filter_df(df)

    # make sure the correct rows were removed
    assert df.shape[0] == 17
