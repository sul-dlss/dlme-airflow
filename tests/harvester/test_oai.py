import pytest
import requests
# import pandas as pd

from harvester.oai import harvest

class MockOAIListRecordsResponse:
    @staticmethod
    def raw():
        return {
            "manifests": [
                {"@id": "https://collection.edu/iiif/p15795coll29:28/manifest.json"}
            ]
        }


@pytest.fixture
def mock_response(monkeypatch):
    def mock_post(*args, **kwargs):
        return MockOAIListRecordsResponse()

    monkeypatch.setattr(requests, "post", mock_post)


@pytest.fixture
def oai_test_list_records():
    metadata = {
        "fields": {
            "context": {"path": "@context", "optional": True},
            "iiif_format": {"path": "sequences..format"},
        }
    }
    return harvest(provider='aub')


# def test_harvest(oai_test_list_records, mock_response):
#     assert len(oai_test_list_records) == 0