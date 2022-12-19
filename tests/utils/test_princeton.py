import pandas

from dlme_airflow.utils.princeton import filter_df


def test_filter_df():
    df = pandas.read_csv("tests/data/csv/princeton.csv")

    # make sure the DataFrame looks how we expect
    assert df.shape[0] == 100

    # merge the rows
    df = filter_df(df)

    # make sure the correct number of rows were removed
    assert df.shape[0] == 95
