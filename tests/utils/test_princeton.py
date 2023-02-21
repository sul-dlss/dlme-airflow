import pytest
import pandas

from dlme_airflow.utils.princeton import filter_df, remove_ymdi
from dlme_airflow.models.provider import Provider


# This mock prevents writing to the fixture CSV when testing
@pytest.fixture
def mock_dataframe_to_csv(monkeypatch):
    def mock_to_csv(_self, _filename):
        return True

    monkeypatch.setattr(pandas.DataFrame, "to_csv", mock_to_csv)


def test_remove_ymdi(mocker, mock_dataframe_to_csv):
    mocker.patch(
        "dlme_airflow.utils.catalog.get_working_csv",
        return_value="tests/data/csv/princeton.csv",
    )
    provider = Provider("princeton")
    params = {"collection": provider.get_collection("islamic_manuscripts")}

    assert "metadata/princeton/islamic_manuscripts/data.csv" in remove_ymdi(**params)


def test_filter_df():
    df = pandas.read_csv("tests/data/csv/princeton.csv")

    # make sure the DataFrame looks how we expect
    assert df.shape[0] == 100

    # merge the rows
    df = filter_df(df)

    # make sure the correct number of rows were removed
    assert df.shape[0] == 95
