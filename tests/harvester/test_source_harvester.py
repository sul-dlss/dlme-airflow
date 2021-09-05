from pathlib import Path

import pytest

from harvester.source_harvester import data_source_harvester, provider_key


@pytest.fixture
def csv_fixture(monkeypatch):
    def mockglob(*args):
        return [{"title": "A title", "url": "https://arabic.io/"}]

    monkeypatch.setattr(Path, "glob", mockglob)


def test_source_harvester():
    assert data_source_harvester


def test_provider_not_found():
    with pytest.raises(AttributeError, match=r"bad_provider"):
        data_source_harvester(provider="bad_provider")


def test_missing_provider():
    with pytest.raises(ValueError, match=r"Missing provider argument."):
        data_source_harvester()


def test_key_for_provider():
    key = provider_key(provider="mock_provider")
    assert key == "mock_provider"


def test_key_for_collection():
    key = provider_key(provider="mock_provider", collection="mock_collection")
    assert key == "mock_provider.mock_collection"


def test_missing_arguments():
    key = provider_key()
    assert key is None


def test_provider_key():
    assert provider_key
