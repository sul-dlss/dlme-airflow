# /bin/python
import os
import pandas as pd
from dlme_airflow.utils.read_df import read_datafile_with_lists


# Objects from these cultures will be retained
RELEVANT_CULTURES = [
    "Egyptian",
    "Egyptian-Islamic",
    "Anatolian",
    "Anatolian?",
    "Akkadian",
    "Armenian",
    "Babylonian",
    "Byzantine",
    "Byzantine; Christian",
    "Christian; Armenian",
    "Christian; Byzantine; Armenian",
    "Coptic",
    "Early Christian",
    "Gnostic",
    "Gutian or Neo-Sumerian",
    "Iranian-Islamic",
    "Islamic",
    "Mamluk",
    "Mesopotamia",
    "Mesopotamian",
    "Neo-Assyrian",
    "Neo-Babylonian",
    "Neo-Babylonian or Achaemenid",
    "Neo-Sumerian",
    "Neo-Sumerian or Babylonian",
    "Neo-Sumerian; Ur III",
    "Old Babylonian",
    "Ottoman",
    "Ottoman-Islamic",
    "Persian",
    "Persian~Islamic",
    "Sumerian",
    "Syrian",
    "Syro-Palestine",
]


def remove_walters_non_relevant(**kwargs):
    """Called by the Airflow workflow to merge records in multiple languages"""
    collection = kwargs["collection"]
    data_file = collection.datafile("json")
    if os.path.isfile(data_file):
        df = read_datafile_with_lists(data_file)
        df = filter_df(df)
        df.to_json(data_file, orient="records", force_ascii=False)

    return data_file


def filter_df(df):
    mena_culture = df[df["Culture"].isin(RELEVANT_CULTURES)]
    islamic_art_collection = df[df["CollectionName"] == "Islamic Art"]

    df = pd.concat([mena_culture, islamic_art_collection]).drop_duplicates(
        subset="ObjectID"
    )

    return df
