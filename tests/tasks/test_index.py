import pytest
import requests

from dlme_airflow.tasks.index import index_collection
from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider


@pytest.fixture
def mock_request(monkeypatch):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    def mock_post(self, **data):
        return MockResponse({"message": "Harvest successfully initiated"}, 202)

    monkeypatch.setattr(requests, "post", mock_post)


def test_index_collection(mock_request):
    provider = Provider("aub")
    collection = Collection(provider, "aladab")
    params = {"collection": collection}
    assert index_collection(**params) == "Harvest successfully initiated"
