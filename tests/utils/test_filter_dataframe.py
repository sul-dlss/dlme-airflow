import pandas

from dlme_airflow.utils.filter_dataframe import filter_by_field


def test_filter_by_culture():
    df = pandas.read_json("tests/data/json/records_to_filter.json")
    df_filtered_by_culture = pandas.read_json(
        "tests/data/json/records_filtered_by_culture.json"
    )

    # make sure the DataFrame has the expected number of rows
    assert df.shape[0] == 18

    # remove records missing the relevant cultures
    df = filter_by_field(
        df,
        "Culture",
        {
            "include": [
                "Egyptian-Islamic",
                "Egyptian",
                "Byzantine",
                "Gutian or Neo-Sumerian",
            ]
        },
    )

    # make sure the correct rows were removed
    assert df.shape[0] == 16
    assert df.reset_index(drop=True).equals(
        df_filtered_by_culture.reset_index(drop=True)
    )


def test_filter_by_collection():
    df = pandas.read_json("tests/data/json/records_to_filter.json")
    df_filtered_by_collection = pandas.read_json(
        "tests/data/json/records_filtered_by_collection.json"
    )

    # remove records with non-relevant cultures
    df = filter_by_field(df, "CollectionName", {"include": ["Islamic Art"]})
    # missing values for Exhibitions force the field to a float, convert for comparison
    df["Exhibitions"] = df["Exhibitions"].astype(int)

    # make sure the correct rows were removed
    assert df.shape[0] == 2
    assert df.reset_index(drop=True).equals(
        df_filtered_by_collection.reset_index(drop=True)
    )
