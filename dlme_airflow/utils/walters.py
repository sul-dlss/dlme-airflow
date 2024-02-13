# /bin/python
import os
import pandas as pd


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
    coll = kwargs["collection"]
    working_csv = coll.datafile("csv")

    if os.path.isfile(working_csv):
        df = pd.read_csv(working_csv)
        # Filter out non relevant records and over write the csv
        df = filter_df(df)

        df.to_csv(working_csv)

    return working_csv


def filter_df(df):
    mena_culture = df[df["Culture"].isin(RELEVANT_CULTURES)]
    islamic_art_collection = df[df["CollectionName"] == "Islamic Art"]

    df = pd.concat([mena_culture, islamic_art_collection]).drop_duplicates(
        subset="ObjectID"
    )

    return df
