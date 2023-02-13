import pandas
import pytest

from dlme_airflow.utils.qnl import (
    get_working_csv,
    merge_df,
    merge_records,
    read_csv_with_lists,
)
from dlme_airflow.models.provider import Provider


def test_merge_df():
    df = pandas.DataFrame(
        [
            {
                "location_shelfLocator": "123",
                "title": ["Hi!"],
                "subject": ["Greetings", "Greetings"],
            },
            {
                "location_shelfLocator": "123",
                "title": ["Bonjour!"],
                "subject": ["Greetings", "Welcome!"],
            },
        ]
    )

    # make sure the DataFrame looks how we expect
    assert len(df) == 2
    assert df.iloc[0].title == ["Hi!"]
    assert df.iloc[1].title == ["Bonjour!"]

    # merge the rows
    df = merge_df(df)

    # make sure the rows got merged
    assert len(df) == 1
    row = df.iloc[0]
    assert type(row.title) == list
    assert set(row.title) == {"Hi!", "Bonjour!"}
    assert set(row.subject) == {"Greetings", "Welcome!"}


def test_read_csv_with_lists():
    # this csv file was generated with bin/get qnl qnl --limit 250
    # each cell in the dataframe contains a list serialized as a string
    # read_csv_with_lists() ensures that these are parsed back into lists
    df = read_csv_with_lists("tests/data/csv/qnl.csv")
    assert type(df.iloc[0].title) == list


# This mock prevents writing to the fixture CSV when testing
@pytest.fixture
def mock_dataframe_to_csv(monkeypatch):
    def mock_to_csv(_self, _filename):
        return True

    monkeypatch.setattr(pandas.DataFrame, "to_csv", mock_to_csv)


def test_merge_records(mocker, mock_dataframe_to_csv):
    mocker.patch(
        "dlme_airflow.utils.qnl.get_working_csv",
        return_value="tests/data/csv/qnl.csv",
    )
    provider = Provider("qnl")
    params = {"collection": provider.get_collection("qnl")}

    assert "tests/data/csv/qnl.csv" in merge_records(**params)


def test_get_working_csv():
    assert "metadata/test_collection/data.csv" in get_working_csv("test_collection")
