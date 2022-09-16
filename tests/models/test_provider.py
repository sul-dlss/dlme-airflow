import pytest

from dlme_airflow.models.provider import Provider


def test_Provider():
    provider = Provider("aims")
    assert provider.label() == "aims"
    assert provider.data_path() == "aims"
    assert len(provider.collections) == 1


def test_Provider_NotFound():
    with pytest.raises(ValueError) as error:
        Provider("does_not_exist")

    assert str(error.value) == "Provider (does_not_exist) not found in catalog"
