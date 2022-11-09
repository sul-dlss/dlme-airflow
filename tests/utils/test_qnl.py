import pandas

from dlme_airflow.utils.qnl import merge_df


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
