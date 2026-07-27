import logging

import pandas as pd

logger = logging.getLogger(__name__)


def check_equality(harvested_df: pd.DataFrame, saved_df: pd.DataFrame):
    """Checks for DataFrame equality between latest harvested data with
    persisted DataFrame.

    @param -- harvested_df
    @param -- saved_df
    """
    if not saved_df.equals(harvested_df):
        logger.error("harvested dataframe does not equal saved dataframe")
