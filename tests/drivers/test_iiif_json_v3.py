import json
import logging
import pytest
import requests

from dlme_airflow.drivers.iiif_json_v3 import IiifV3JsonSource

LOGGER = logging.getLogger(__name__)


class MockIIIFCollectionV2Response:
    @property
    def status_code(self):
        return 200

    @staticmethod
    def json():
        return {
            "manifests": [
                {"@id": "https://collection.edu/iiif/p15795coll29:28/manifest.json"}
            ]
        }


class MockIIIFCollectionV3Response:
    @property
    def status_code(self):
        return 200

    @staticmethod
    def json():
        with open("tests/data/iiif_v3/collection_items.json") as f:
            data = json.load(f)
        return data

class MockIIIFManifestResponse:
    @property
    def status_code(self):
        return 200

    @staticmethod
    def json():
        with open("tests/data/iiif_v3/item_manifest.json") as f:
            data = json.load(f)
        return data


@pytest.fixture
def mock_response(monkeypatch):
    def mock_get(*args, **kwargs):
        if args[0].endswith("v2_collection.json"):
            return MockIIIFCollectionV2Response()
        # if args[0].endswith("v3_collection.json"):
        #     return MockIIIFCollectionV3Response()
        if args[0].endswith("manifest"):
            return MockIIIFManifestResponse()
        if "iiifservices/collection/al-Adab" in args[0]:
            return MockIIIFCollectionV3Response()
        return

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def iiif_test_v3_source():
    metadata = {
        "fields": {
            "id": {
                "path": "id",
            },  # a specified field with one value in the metadata
        }
    }
    paging = {
        "pages_url": "https://iiif_v3_collection/iiifservices/collection/al-Adab/{offset}/{limit}",
        "page_data": "items",
        "limit": 1000
    }
    return IiifV3JsonSource(
        collection_url="https://iiif_v3_collection/iiifservices/collection/Posters",
        paging=paging,
        metadata=metadata
    )


def test_IiifJsonSource_initial(iiif_test_v3_source, mock_response):
    assert len(iiif_test_v3_source._manifests) == 0


def test_IiifJsonSource_get_schema(iiif_test_v3_source, mock_response):
    iiif_test_v3_source._get_schema()
    assert len(iiif_test_v3_source._manifests) == 2
    assert (
        iiif_test_v3_source._manifests[0]["id"]
        == "https://libraries.aub.edu.lb/iiifservices/item/ark86073b3hd1k/manifest"
    )


def test_IiifJsonSource_read(iiif_test_v3_source, mock_response):
    iiif_df = iiif_test_v3_source.read()
    print(f"Columns: {iiif_df.columns}")
    test_columns = [
        "id",
        "title",
        "identifier",
        "language",
        "date",
        "authors",
        "descriptions",
        "extent",
        "subjects",
        "collection",
        "rights",
    ]
    assert all([a == b for a, b in zip(iiif_df.columns, test_columns)])


def test_IiifJsonSource_df(iiif_test_v3_source, mock_response):
    iiif_df = iiif_test_v3_source.read()
    assert len(iiif_df.get('id')) == 2
    assert iiif_df.get('id').to_list() == ["https://libraries.aub.edu.lb/iiifservices/item/ark86073b3x34b/manifest", "https://libraries.aub.edu.lb/iiifservices/item/ark86073b3x34b/manifest"]


@pytest.fixture
def iiif_test_v3_source_with_profile():
    metadata = {
        "fields": {
            "thumbnail": {
                "path": "thumbnail",
                "optional": True,
            },  # a specified field with one value in the metadata
            "profile": {
                "path": "profile",
                "optional": False,
            },
        }
    }
    paging = {
        "pages_url": "https://iiif_v3_collection/iiifservices/collection/al-Adab/{offset}/{limit}",
        "page_data": "items",
        "limit": 1000
    }
    return IiifV3JsonSource(
        collection_url="http://iiif_v3_collection/iiifservices/collection/al-Adab",
        paging=paging,
        metadata=metadata
    )

def test_IiifJsonSource_logging(iiif_test_v3_source_with_profile, mock_response, caplog):
    with caplog.at_level(logging.WARNING):
        iiif_test_v3_source_with_profile.read()
        print(f"CAPLOG: {caplog.text}")
    assert (
        "https://libraries.aub.edu.lb/iiifservices/item/ark86073b3x34b/manifest missing required field: 'profile'; searched path: 'profile'"  # noqa: E501
        in caplog.text
    )
    assert "missing optional field" not in caplog.text

    with caplog.at_level(logging.DEBUG):
        iiif_test_v3_source_with_profile.read()
    assert (
        "https://libraries.aub.edu.lb/iiifservices/item/ark86073b3x34b/manifest missing optional field: 'thumbnail'; searched path: 'thumbnail'"  # noqa: E501
        in caplog.text
    )


# def test_wait(iiif_test_v3_source):
#     driver = IiifV3JsonSource("https://example.com/iiif/", wait=2)
#     assert driver, "IiifJsonSource constructor accepts wait parameter"


def test_list_encode(iiif_test_v3_source, mock_response):
    iiif_df = iiif_test_v3_source.read()
    assert iiif_df.get("date").to_list() == [["1998"], ["1998"]]
