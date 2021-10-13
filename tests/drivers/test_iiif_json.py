import pytest
import requests
import pandas as pd

from drivers.iiif_json import IIIfJsonSource


class MockIIIFCollectionResponse:
    @staticmethod
    def json():
        return {
            "manifests": [
                {"@id": "https://collection.edu/iiif/p15795coll29:28/manifest.json"}
            ]
        }

def text():
    return {
        "manifests": [
            {"@id": "https://collection.edu/iiif/p15795coll29:28/manifest.json"}
        ]
    }


class MockIIIFManifestResponse:
    @staticmethod
    def json():
        return {
            "@context": "http://iiif.io/api/presentation/2/context.json",
            "@id": "https://collection.edu/iiif/p15795coll29:28/manifest.json",
            "metadata": [
                {
                    "label": "Source",
                    "value": "Rare Books and Special Collections Library",
                },
                {"label": "Title (main)", "value": "A great title of the Middle East"},
            ],
            "sequences": [
                {"canvases": [{"images": [{"resource": {"format": "image/jpeg"}}]}]}
            ],
        }


@pytest.fixture
def mock_response(monkeypatch):
    def mock_get(*args, **kwargs):
        if args[0].endswith("collection.json"):
            return MockIIIFCollectionResponse()
        if args[0].endswith("manifest.json"):
            return MockIIIFManifestResponse()
        return

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def iiif_test_source():
    metadata = {
        "fields": {
            "context": {"path": "@context", "optional": True},
            "iiif_format": {"path": "sequences..format"},
        }
    }
    return IIIfJsonSource(
        collection_url="http://iiif_collection.json", metadata=metadata
    )


def test_IIIfJsonSource_initial(iiif_test_source, mock_response):
    assert len(iiif_test_source._manifest_urls) == 0


def test_IIIfJsonSource_get_schema(iiif_test_source, mock_response):
    iiif_test_source._get_schema()
    assert (
        iiif_test_source._manifest_urls[0]
        == "https://collection.edu/iiif/p15795coll29:28/manifest.json"
    )


def test_IIIfJsonSource_read(iiif_test_source, mock_response):
    iiif_df = iiif_test_source.read()
    test_columns = ["context", "iiif_format", "source", "title-main"]
    assert all([a == b for a, b in zip(iiif_df.columns, test_columns)])


def test_test_IIIfJsonSource_df(iiif_test_source, mock_response):
    iiif_df = iiif_test_source.read()
    test_df = pd.DataFrame(
        [
            {
                "context": "http://iiif.io/api/presentation/2/context.json",
                "iiif_format": "image/jpeg",
                "source": "Rare Books and Special Collections Library",
                "title-main": "A great title of the Middle East",
            }
        ]
    )
    assert iiif_df.equals(test_df)
