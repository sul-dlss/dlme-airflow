import pandas

from dlme_airflow.utils.qnl import merge_df, merge_records, read_csv_with_lists
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


def test_merge_records():
    provider = Provider("qnl")
    params = {"collection": provider.get_collection("qnl")}

    assert "metadata/qnl/british_library_combined/data.csv" in merge_records(**params)
