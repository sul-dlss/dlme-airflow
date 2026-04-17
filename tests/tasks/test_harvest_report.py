from collections import Counter, defaultdict
import io
import json

import pytest
import requests
from PIL import Image

import dlme_airflow.tasks.mapping_report as mapping_report


@pytest.fixture
def mock_catalog_for_provider(monkeypatch):
    """Mock catalog_for_provider() to return a Catalog class with a metadata attribute"""

    def mockreturn(provider):
        class MockCatalog:
            def __init__(self):
                self.metadata = {"config": "testmuseum"}

        return MockCatalog()

    monkeypatch.setattr(
        "dlme_airflow.tasks.mapping_report.catalog_for_provider", mockreturn
    )

    monkeypatch.setattr("dlme_airflow.models.provider.catalog_for_provider", mockreturn)

    def mock_collection(self, collection):
        class MockCollection:
            def __init__(self):
                self.name = collection

            def data_path(self):
                return "testmuseum"

            def intermediate_representation_location(self):
                return "output-testmuseum.ndjson"

        return MockCollection()

    monkeypatch.setattr(
        "dlme_airflow.models.provider.Provider.get_collection", mock_collection
    )


def test_successful_mapping_report(
    requests_mock, mock_catalog_for_provider, thumbnail_image
):
    # Mock the harvest output data that is input to mapping_report. It is an ndjson file.
    testmuseum_harvest_data = open("tests/data/ndjson/output-testmuseum.ndjson").read()
    requests_mock.get(
        "https://s3-us-west-2.amazonaws.com/dlme-metadata-dev/output/output-testmuseum.ndjson",
        text=testmuseum_harvest_data,
    )

    # Mock traject config request to return a test config
    # f"https://raw.githubusercontent.com/sul-dlss/dlme-transform/main/traject_configs/{catalog.metadata.get('config')}.rb"
    testmuseum_config = open("tests/data/testmuseum_config.rb").read()
    requests_mock.get(
        "https://raw.githubusercontent.com/sul-dlss/dlme-transform/main/traject_configs/testmuseum.rb",
        text=testmuseum_config,
    )
    # Mock URLs in tests/data/ndjson/output-testmuseum.ndjson for resolvability checking
    requests_mock.head("https://example.com", status_code=200)
    requests_mock.get("https://example.com/image1.jpg", content=thumbnail_image)

    options = {
        "provider": "testmuseum",
        "collection": "test",
        "data_path": "testmuseum",
    }
    doc = mapping_report.mapping_report(**options)

    assert "h2" in doc
    # Coverage Report
    assert "agg_data_provider_collection_id: (100% coverage)" in doc
    assert "agg_is_shown_at: (100% coverage)" in doc
    assert "cho_creator: (100% coverage)" in doc
    assert "cho_date_range_hijri: (100% coverage)" in doc
    assert "cho_date_range_norm: (100% coverage)" in doc
    assert "cho_dc_rights: (100% coverage)" in doc
    assert "cho_dc_rights: (100% coverage)" in doc
    assert "cho_edm_type: (100% coverage)" in doc
    assert "cho_extent: (66% coverage)" in doc
    assert "cho_title: (100% coverage)" in doc
    assert "cho_has_type: (100% coverage)" in doc
    # Resource Report
    assert "3 of 3 records had valid urls to thumbnail images." in doc
    assert "3 of 3 records had valid urls to resources." in doc
    assert "0 of 3 records had iiif manifests." in doc
    # Rights Report
    assert (
        "3 of 3 records had a clearly expressed copyright status for the cultural heritage object."
        in doc
    )
    assert (
        "3 of 3 records had a clearly expressed copyright status for the web resource."
        in doc
    )
    assert "3 of 3 records had clearly expressed aggregation rights." in doc
    # Thumbnail Quality Report
    assert (
        "0% of the 3 thumbnail images sampled had a width or height of 400 or greater."
        in doc
    )

    assert "cho_provenance" not in doc


@pytest.fixture
def thumbnail_image():
    image = Image.new("RGB", size=(150, 56), color=(256, 0, 0))
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG")
    image_binary = buffer.getvalue()

    return image_binary


@pytest.fixture
def large_thumbnail_image():
    image = Image.new("RGB", size=(400, 300), color=(256, 0, 0))
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG")
    image_binary = buffer.getvalue()

    return image_binary


def test_image_size(requests_mock, thumbnail_image):
    # mock thumbnail URL lookup
    requests_mock.get("https://example.com/image1.jpg", content=thumbnail_image)
    response = requests.get("https://example.com/image1.jpg")
    size = mapping_report.image_size(response.content)

    assert size == (150, 56)


def test_sample_image_sizes(requests_mock, thumbnail_image):
    thumbnail_urls = ["https://example.com/image1.jpg" for _ in range(101)]
    requests_mock.get("https://example.com/image1.jpg", content=thumbnail_image)

    assert len(mapping_report.sample_image_sizes(thumbnail_urls)) == 50


def test_thumbnail_report(large_thumbnail_image):
    images_sizes = [(100, 200), (400, 300), (100, 500)]
    report = mapping_report.thumbnail_report(images_sizes)

    assert (
        report
        == "67% of the 3 thumbnail images sampled had a width or height of 400 or greater."
    )


def test_resolve_good_resource_url(requests_mock):
    requests_mock.head("https://example.com", status_code=200)
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    unresolvable = []
    mapping_report.resolve_resource_url(record, unresolvable)

    assert len(unresolvable) == 0


def test_resolve_bad_resource_url(requests_mock):
    requests_mock.head("https://example.com", status_code=404)
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    unresolvable = []
    mapping_report.resolve_resource_url(record, unresolvable)

    assert len(unresolvable) == 1


def test_resolve_good_thumbnail_url(requests_mock):
    requests_mock.get("https://example.com/image1.jpg", status_code=200)
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    urls: list = []
    unresolvable: list = []
    mapping_report.resolve_thumbnail_url(record, urls, unresolvable)

    assert len(unresolvable) == 0
    assert len(urls) == 1


def test_resolve_bad_thumbnail_url(requests_mock):
    requests_mock.get("https://example.com/image1.jpg", status_code=404)
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    urls: list = []
    unresolvable: list = []
    mapping_report.resolve_thumbnail_url(record, urls, unresolvable)

    assert len(unresolvable) == 1
    assert len(urls) == 0


def test_resolve_invalid_thumbnail_url(requests_mock):
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    record["agg_preview"]["wr_id"] = "not_a_url"
    urls: list = []
    unresolvable: list = []
    mapping_report.resolve_thumbnail_url(record, urls, unresolvable)

    assert len(unresolvable) == 1
    assert len(urls) == 0


def test_resolve_null_thumbnail_url(requests_mock):
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    record["agg_preview"]["wr_id"] = None
    urls: list = []
    unresolvable: list = []
    mapping_report.resolve_thumbnail_url(record, urls, unresolvable)

    assert len(unresolvable) == 1
    assert len(urls) == 0


def test_count_fields():
    field = "cho_test_dict"
    metadata = {"en": ["Test item title"]}
    counts = defaultdict(Counter)
    mapping_report.count_fields(field, metadata, counts)
    assert dict(counts) == {"cho_test_dict": {"fields_covered": 1, "en": 1}}


def test_count_fields_str():
    field = "agg_provider_collection_id"
    metadata = "abcd"
    counts = defaultdict(Counter)
    mapping_report.count_fields(field, metadata, counts)
    assert dict(counts) == {
        "agg_provider_collection_id": {"fields_covered": 1, "values": 1}
    }


def test_count_fields_dict():
    field = "cho_test_edm"
    metadata = {"en": ["Object"], "ar-Arab": ["كائن"]}
    counts = defaultdict(Counter)
    mapping_report.count_fields(field, metadata, counts)
    assert dict(counts) == {
        "cho_test_edm": {"fields_covered": 1, "en": 1, "ar-Arab": 1}
    }
