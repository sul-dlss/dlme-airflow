import pandas as pd

from dlme_airflow.utils.read_df import read_datafile_with_lists


def test_read_datafile_with_lists():
    df1 = pd.read_json("tests/data/json/walters/unfiltered.json")
    df2 = read_datafile_with_lists("tests/data/json/walters/unfiltered.json")

    assert isinstance(df2, pd.DataFrame)
    assert df1.shape == df2.shape
    assert isinstance(df2["ObjectName"].values[0], str)
