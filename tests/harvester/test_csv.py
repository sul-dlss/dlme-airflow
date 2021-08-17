import logging
import pytest
import pandas as pd
from harvester.csv import csv_harvester, check_equality

LOGGER = logging.getLogger(__name__)


def test_csv_harvester(caplog):
    assert csv_harvester
    # TODO: Need to mock dlme-metadata


def test_provider_not_found():
    with pytest.raises(ValueError, match=r"bad_provider not found in catalog"):
        csv_harvester("bad_provider")


def test_check_equality_pass(caplog):
    harvested_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    saved_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    with caplog.at_level(logging.ERROR):
        check_equality(harvested_df, saved_df)
    assert "harvested dataframe does not equal saved dataframe" not in caplog.text


def test_check_equality_fail(caplog):
    harvested_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    saved_df = pd.DataFrame({"col1": [1, 2], "col2": [4, 5]})
    with caplog.at_level(logging.ERROR):
        check_equality(harvested_df, saved_df)
    assert "harvested dataframe does not equal saved dataframe" in caplog.text
