import pytest
import requests

from dlme_airflow.tasks.index import index_collection
from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider


@pytest.fixture
def mock_request(monkeypatch):
    def mock_post(self, **data):
        return '{"message":"Harvest successfully initiated"}'

    monkeypatch.setattr(requests, "post", mock_post)


def test_index_collection(mock_request):
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    params = {"collection": collection}
    assert index_collection(**params) == '{"message":"Harvest successfully initiated"}'
