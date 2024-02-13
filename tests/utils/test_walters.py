import pytest
import pandas

from dlme_airflow.utils.walters import remove_walters_non_relevant
from dlme_airflow.models.provider import Provider


# This mock prevents writing to the fixture CSV when testing
@pytest.fixture
def mock_dataframe_to_csv(monkeypatch):
    def mock_to_csv(_self, _filename):
        return True

    monkeypatch.setattr(pandas.DataFrame, "to_csv", mock_to_csv)


def test_remove_walters_non_relevant(mocker, mock_dataframe_to_csv):
    provider = Provider("walters")
    params = {"collection": provider.get_collection("mena")}

    assert "working/walters/mena/data.csv" in remove_walters_non_relevant(**params)
