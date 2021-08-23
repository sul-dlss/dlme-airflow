import logging

import pandas as pd


def check_equality(harvested_df: pd.DataFrame, saved_df: pd.DataFrame):
    """Checks for DataFrame equality between latest harvested data with
    persisted DataFrame.

    @param -- harvested_df
    @param -- saved_df
    """
    if not saved_df.equals(harvested_df):
        logging.error("harvested dataframe does not equal saved dataframe")
