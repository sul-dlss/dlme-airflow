from pathlib import Path

import pytest
import pandas as pd

from harvester.source_harvester import data_source_harvester


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
