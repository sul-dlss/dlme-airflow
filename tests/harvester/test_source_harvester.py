from pathlib import Path

import pytest

from dlme_airflow.harvester.source_harvester import data_source_harvester


@pytest.fixture
def csv_fixture(monkeypatch):
    def mockglob(*args):
        return [{"title": "A title", "url": "https://arabic.io/"}]

    monkeypatch.setattr(Path, "glob", mockglob)


@pytest.mark.skip(reason="TODO Fix tests with mocked provider collection.")
def test_source_harvester():
    assert data_source_harvester


@pytest.mark.skip(reason="TODO Fix tests with mocked provider collection.")
def test_provider_not_found():
    with pytest.raises(AttributeError, match=r"bad_provider"):
        data_source_harvester(provider="bad_provider")


@pytest.mark.skip(reason="TODO Fix tests with mocked provider collection.")
def test_missing_provider():
    with pytest.raises(ValueError, match=r"Missing provider argument."):
        data_source_harvester()
