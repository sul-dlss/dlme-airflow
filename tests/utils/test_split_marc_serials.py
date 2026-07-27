import shutil
import tempfile

import pandas
import pytest

from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider
from dlme_airflow.utils.split_marc_serials import (
    split_marc_serials,
)


@pytest.fixture
def mock_datafile(monkeypatch):
    # copy the test data to a temporary path since it will be written to
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_name = tmp.name
    shutil.copy("tests/data/hathi_trust/serials.json", tmp_name)

    # mock Collection.datafile to return the path for the temp test data
    monkeypatch.setattr(Collection, "datafile", lambda *args, **kwargs: tmp_name)


def test_split_marc_serials(mocker, mock_datafile):
    provider = Provider("michigan")
    collection = provider.get_collection("serials")

    df = pandas.read_json(collection.datafile("json"))
    assert len(df) == 1, "correct number of records"

    result = split_marc_serials(collection=collection)
    df = pandas.read_json(result, orient="records")

    assert len(df) == 4, "correct number of records"
