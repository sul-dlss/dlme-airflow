import logging
import pytest
import requests
import pandas as pd

from dlme_airflow.drivers.iiif_json import IiifJsonSource

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
        return {
            "items": [
                {"id": "https://collection.edu/iiif/p15795coll29:28/manifest.json"}
            ]
        }


class MockIIIFManifestResponse:
    @property
    def status_code(self):
        return 200

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
                {"label": "Title (sub)", "value": "Subtitle 1"},
                {"label": "Title (sub)", "value": "Subtitle 2"},
                {"label": "Date Created", "value": ["1974"]},
            ],
            "sequences": [
                {
                    "canvases": [
                        {"images": [{"resource": {"format": "image/jpeg"}}]},
                        {"images": [{"resource": {"format": "image/jpeg"}}]},
                    ]
                }
            ],
            "description": ["A descriptive phrase", " with further elaboration "],
        }


@pytest.fixture
def mock_response(monkeypatch):
    def mock_get(*args, **kwargs):
        if args[0].endswith("v2_collection.json"):
            return MockIIIFCollectionV2Response()
        if args[0].endswith("v3_collection.json"):
            return MockIIIFCollectionV3Response()
        if args[0].endswith("manifest.json"):
            return MockIIIFManifestResponse()
        return

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def iiif_test_v2_source():
    metadata = {
        "fields": {
            "context": {
                "path": "@context",
                "optional": True,
            },  # a specified field with one value in the metadata
            "description_top": {"path": "description", "optional": True},
            "iiif_format": {
                "path": "sequences..format"
            },  # a specified field with multiple values in the metadata
            "profile": {"path": "sequences..profile"},  # a missing required field
            "thumbnail": {
                "path": "thumbnail..@id",
                "optional": True,
            },  # missing optional field
        }
    }
    return IiifJsonSource(
        collection_url="http://iiif_v2_collection.json", metadata=metadata
    )


@pytest.fixture
def iiif_test_v2_no_collection_source():
    metadata = {
        "fields": {
            "context": {
                "path": "@context",
                "optional": True,
            },  # a specified field with one value in the metadata
            "description_top": {"path": "description", "optional": True},
            "iiif_format": {
                "path": "sequences..format"
            },  # a specified field with multiple values in the metadata
            "profile": {"path": "sequences..profile"},  # a missing required field
            "thumbnail": {
                "path": "thumbnail..@id",
                "optional": True,
            },  # missing optional field
        }
    }
    return IiifJsonSource(
        manifest_urls=["https://collection.edu/iiif/p15795coll29:28/manifest.json"], metadata=metadata
    )


@pytest.fixture
def iiif_test_v3_source():
    metadata = {
        "fields": {
            "context": {
                "path": "@context",
                "optional": True,
            },  # a specified field with one value in the metadata
            "description_top": {"path": "description", "optional": True},
            "iiif_format": {
                "path": "sequences..format"
            },  # a specified field with multiple values in the metadata
            "profile": {"path": "sequences..profile"},  # a missing required field
            "thumbnail": {
                "path": "thumbnail..@id",
                "optional": True,
            },  # missing optional field
        }
    }
    return IiifJsonSource(
        collection_url="http://iiif_v3_collection.json", metadata=metadata
    )


def test_IiifJsonSource_initial(iiif_test_v2_source, mock_response):
    assert len(iiif_test_v2_source._manifest_urls) == 0


def test_IiifJsonSource_get_schema(iiif_test_v2_source, mock_response):
    iiif_test_v2_source._get_schema()
    assert (
        iiif_test_v2_source._manifest_urls[0]
        == "https://collection.edu/iiif/p15795coll29:28/manifest.json"
    )


def test_IiifJsonSource_read(iiif_test_v2_source, mock_response):
    iiif_df = iiif_test_v2_source.read()
    test_columns = [
        "context",
        "description_top",
        "iiif_format",
        "source",
        "title-main",
        "title-sub",
    ]
    assert all([a == b for a, b in zip(iiif_df.columns, test_columns)])


def test_IiifJsonNoCollectionSource_read(iiif_test_v2_no_collection_source, mock_response):
    iiif_df = iiif_test_v2_no_collection_source.read()
    test_columns = [
        "context",
        "description_top",
        "iiif_format",
        "source",
        "title-main",
        "title-sub",
    ]
    assert all([a == b for a, b in zip(iiif_df.columns, test_columns)])

def test_IiifJsonSource_df(iiif_test_v2_source, mock_response):
    iiif_df = iiif_test_v2_source.read()
    test_df = pd.DataFrame(
        [
            {
                "context": "http://iiif.io/api/presentation/2/context.json",
                "description_top": ["A descriptive phrase", "with further elaboration"],
                "iiif_format": ["image/jpeg", "image/jpeg"],
                "source": ["Rare Books and Special Collections Library"],
                "title-main": ["A great title of the Middle East"],
                "title-sub": ["Subtitle 1", "Subtitle 2"],
                "date-created": ["1974"],
            }
        ]
    )
    assert iiif_df.equals(test_df)


def test_IiifJsonSource_logging(iiif_test_v2_source, mock_response, caplog):
    with caplog.at_level(logging.WARNING):
        iiif_test_v2_source.read()
    assert (
        "https://collection.edu/iiif/p15795coll29:28/manifest.json missing required field: 'profile'; searched path: 'sequences..profile'"  # noqa: E501
        in caplog.text
    )
    assert "missing optional field" not in caplog.text

    with caplog.at_level(logging.DEBUG):
        iiif_test_v2_source.read()
    assert (
        "https://collection.edu/iiif/p15795coll29:28/manifest.json missing optional field: 'thumbnail'; searched path: 'thumbnail..@id'"  # noqa: E501
        in caplog.text
    )


def test_wait(iiif_test_v2_source):
    driver = IiifJsonSource("https://example.com/iiif/", wait=2)
    assert driver, "IiifJsonSource constructor accepts wait parameter"


def test_list_encode(iiif_test_v2_source, mock_response):
    iiif_df = iiif_test_v2_source.read()
    assert iiif_df["date-created"][0] == ["1974"]
