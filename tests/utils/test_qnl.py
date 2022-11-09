import pandas
from ast import literal_eval

from dlme_airflow.utils.qnl import merge_df


def test_merge():
    df = pandas.read_csv("tests/data/csv/qnl.csv")
    assert len(df) == 20
    assert len(df.columns) == len(
        merge_df(df).columns
    ), "number of columns didn't change"

    df = merge_df(df)
    assert len(df) == 10
    assert [isinstance(i, list) for i in df["subject_name_namePart"]]
    assert len(literal_eval(df.subject_topic[1])) == 6
    assert len(literal_eval(df.subject_name_namePart[1])) == 10
    # assert df.subject_topic[1].count('[') == 1
    # assert df.subject_topic[1].count(']') == 1
    # assert df.physicalDescription_extent[1].count('[') == 1
    # assert df.physicalDescription_extent[1].count(']') == 1
