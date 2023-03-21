import pandas
import pytest
import shutil
import tempfile

from dlme_airflow.models.provider import Provider
from dlme_airflow.models.collection import Collection

from dlme_airflow.utils.qnl import (
    merge_df,
    merge_records,
)


@pytest.fixture
def mock_dataframe(monkeypatch):
    # copy the test data to a temporary path since it will be written to
    tmp = tempfile.NamedTemporaryFile()
    shutil.copy("tests/data/json/qnl.json", tmp.name)

    # mock Collection.datafile to return the path for the temp test data
    monkeypatch.setattr(Collection, "datafile", lambda *args, **kwargs: tmp.name)


def test_merge_df():
    df = pandas.DataFrame(
        [
            {
                "id": "1",
                "location_shelfLocator": "123",
                "title": ["Hi!"],
                "subject": ["Greetings", "Greetings"],
            },
            {
                "id": "2",
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

    assert type(row.id) == str, "there is only one identifier"
    assert row.id == "1", "the first identifier wins"

    assert type(row.title) == list, "title is a list"
    assert set(row.title) == {"Hi!", "Bonjour!"}, "title values are correct"

    assert type(row.subject) == list, "subject is a list"
    assert set(row.subject) == {"Greetings", "Welcome!"}, "subject values are correct"


def test_merge_records(mocker, mock_dataframe):
    provider = Provider("qnl")
    collection = provider.get_collection("qnl")

    result = merge_records(collection=collection)
    df = pandas.read_json(result, orient="records")
    df = df.sort_values("id")

    assert len(df) == 25, "correct number of records"
    assert df.iloc[0].id == "81055/vdc_100000000041.0x0001c1_ar", "id is still correct"
