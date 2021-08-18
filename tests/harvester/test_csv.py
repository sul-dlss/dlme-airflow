import pytest

from harvester.csv import csv_harvester


def test_csv_harvester():
    assert csv_harvester
    # TODO: Need to mock dlme-metadata


def test_provider_not_found():
    with pytest.raises(ValueError, match=r"bad_provider not found in catalog"):
        csv_harvester("bad_provider")
