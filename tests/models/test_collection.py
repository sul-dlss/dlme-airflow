import pytest

from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider


def test_Collection():
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    assert collection.label() == "aub_aco"
    assert collection.data_path() == "aub/aco"
    assert (
        collection.intermidiate_representation_location()
        == "https://dlme-metadata-dev.s3.us-west-2.amazonaws.com/output/output-aub-aco.ndjson"
    )


def test_Provider_NotFound():
    with pytest.raises(ValueError) as error:
        provider = Provider("aub")
        Collection(provider, "amc").catalog

    assert str(error.value) == "Provider (aub.amc) not found in catalog"
