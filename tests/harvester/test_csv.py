import pytest
from harvester.csv import csv_harvester


def test_csv_harvester(monkeypatch, tmp_path):
    ans_source = csv_harvester("ans")
    assert ans_source.metadata
    assert csv_harvester is not None


def test_provider_not_found():
    with pytest.raises(ValueError, match=r"bad_provider not found in catalog"):
        csv_harvester("bad_provider")
