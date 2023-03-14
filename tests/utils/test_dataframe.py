import pandas as pd

from dlme_airflow.utils.dataframe import (
    datafile_for_collection,
    datafile_for_collection_archive,
    dataframe_from_file,
    dataframe_from_previous,
)
from dlme_airflow.models.provider import Provider


def test_dataframe_from_previous(mocker):
    provider = Provider("aims")
    mocker.patch(
        "dlme_airflow.utils.dataframe.datafile_for_collection_archive",
        return_value="tests/data/csv/example1.csv",
    )
    assert (
        dataframe_from_previous(provider.get_collection("aims")).size
    ) == pd.read_csv("tests/data/csv/example1.csv").size


def test_dataframe_from_files(mocker):
    provider = Provider("aims")
    mocker.patch(
        "dlme_airflow.utils.dataframe.datafile_for_collection",
        return_value="tests/data/csv/example1.csv",
    )
    assert (dataframe_from_file(provider.get_collection("aims")).size) == pd.read_csv(
        "tests/data/csv/example1.csv"
    ).size


def test_datafile_for_collection():
    provider = Provider("aims")
    assert "working/aims/data.csv" in datafile_for_collection(
        provider.get_collection("aims")
    )


def test_datafile_for_collection_archive():
    provider = Provider("aims")
    assert "working/aims/archive/data.csv" in datafile_for_collection_archive(
        provider.get_collection("aims")
    )
