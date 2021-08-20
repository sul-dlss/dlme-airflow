from pathlib import Path

import pytest
import pandas as pd

from harvester.source_harvester import data_source_harvester, get_existing_df


@pytest.fixture
def csv_fixture(monkeypatch):
    def mockglob(*args):
        return [{"title": "A title", "url": "https://arabic.io/"}]

    monkeypatch.setattr(Path, "glob", mockglob)


def test_source_harvester():
    assert data_source_harvester


def test_provider_not_found():
    with pytest.raises(AttributeError, match=r"bad_provider"):
        data_source_harvester("bad_provider")


def test_get_existing_df(csv_fixture):
    mock_directory = Path()
    result = get_existing_df("csv", mock_directory)
    assert isinstance(result, pd.DataFrame)
