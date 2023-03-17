import pytest
import os

from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider


def test_Collection():
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    assert collection.label() == "aub_aco"
    assert collection.data_path() == "aub/aco"
    assert collection.intermediate_representation_location() == "output-aub-aco.ndjson"


def test_datafile():
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    working_data_path = os.path.abspath("working")
    assert collection.datafile("csv") == os.path.join(
        working_data_path, "aub", "aco", "data.csv"
    )
    assert collection.datafile("json") == os.path.join(
        working_data_path, "aub", "aco", "data.json"
    )


def test_Provider_NotSupported():
    with pytest.raises(Exception) as error:
        provider = Provider("aub")
        collection = Collection(provider, "aco")
        collection.datafile("xml")

    assert str(error.value) == "Unsupported data output format: xml"


def test_Provider_NotFound():
    with pytest.raises(ValueError) as error:
        provider = Provider("aub")
        Collection(provider, "amc").catalog

    assert str(error.value) == "Provider (aub.amc) not found in catalog"
