import pandas as pd
import tempfile

from dlme_airflow.utils.read_df import read_datafile_with_lists


def test_read_datafile_with_lists():
    df1 = pd.read_json("tests/data/json/walters/unfiltered.json")
    df2 = read_datafile_with_lists("tests/data/json/walters/unfiltered.json")

    assert isinstance(df2, pd.DataFrame)
    assert df1.shape == df2.shape
    assert isinstance(df2["ObjectName"].values[0], str)


def test_date():
    with tempfile.NamedTemporaryFile() as fh:
        json_path = fh.name
        df1 = pd.DataFrame({"id": [1, 2], "date": ["1910", "2014"]})
        df1.to_json(json_path)

        df2 = read_datafile_with_lists(json_path)
        assert df2.iloc[0]["date"] == "1910"
