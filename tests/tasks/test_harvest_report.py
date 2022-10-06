from collections import Counter, defaultdict
import io
import json

import pytest
import requests
from PIL import Image

import dlme_airflow.tasks.harvest_report as harvest_report


@pytest.fixture
def mock_catalog_for_provider(monkeypatch):
    """Mock catalog_for_provider() to return a Catalog class with a metadata attribute"""

    def mockreturn(provider):
        class MockCatalog:
            def __init__(self):
                self.metadata = {"config": "testmuseum"}

        return MockCatalog()

    monkeypatch.setattr(
        "dlme_airflow.tasks.harvest_report.catalog_for_provider", mockreturn
    )


def test_successful_harvest_report(
    requests_mock, mock_catalog_for_provider, thumbnail_image
):
    # Mock the harvest output data that is input to harvest_report. It is an ndjson file.
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
    doc = harvest_report.main(**options)

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
    size = harvest_report.image_size(response.content)

    assert size == (150, 56)


def test_sample_image_sizes(requests_mock, thumbnail_image):
    thumbnail_urls = ["https://example.com/image1.jpg" for _ in range(101)]
    requests_mock.get("https://example.com/image1.jpg", content=thumbnail_image)

    assert len(harvest_report.sample_image_sizes(thumbnail_urls)) == 50


def test_thumbnail_report(large_thumbnail_image):
    images_sizes = [(100, 200), (400, 300), (100, 500)]
    report = harvest_report.thumbnail_report(images_sizes)

    assert (
        report
        == "67% of the 3 thumbnail images sampled had a width or height of 400 or greater."
    )


@pytest.fixture
def unresolvable_resources(monkeypatch):
    unresolvable_resources = []
    monkeypatch.setattr(
        harvest_report, "unresolvable_resources", unresolvable_resources
    )


def test_resolve_good_resource_url(requests_mock, unresolvable_resources):
    requests_mock.head("https://example.com", status_code=200)
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    harvest_report.resolve_resource_url(record)

    assert len(harvest_report.unresolvable_resources) == 0


def test_resolve_bad_resource_url(requests_mock, unresolvable_resources):
    requests_mock.head("https://example.com", status_code=404)
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    harvest_report.resolve_resource_url(record)

    assert len(harvest_report.unresolvable_resources) == 1


@pytest.fixture
def thumbnail_image_urls(monkeypatch):
    """Monkeypatch global variable"""
    thumbnail_image_urls = []
    monkeypatch.setattr(harvest_report, "thumbnail_image_urls", thumbnail_image_urls)


@pytest.fixture
def unresolvable_thumbnails(monkeypatch):
    """Monkeypatch global variable"""
    unresolvable_thumbnails = []
    monkeypatch.setattr(
        harvest_report, "unresolvable_thumbnails", unresolvable_thumbnails
    )


def test_resolve_good_thumbnail_url(
    requests_mock, thumbnail_image_urls, unresolvable_thumbnails
):
    requests_mock.get("https://example.com/image1.jpg", status_code=200)
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    harvest_report.resolve_thumbnail_url(record)

    assert len(harvest_report.unresolvable_thumbnails) == 0
    assert len(harvest_report.thumbnail_image_urls) == 1


def test_resolve_bad_thumbnail_url(
    requests_mock, thumbnail_image_urls, unresolvable_thumbnails
):
    requests_mock.get("https://example.com/image1.jpg", status_code=404)
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    harvest_report.resolve_thumbnail_url(record)

    assert len(harvest_report.unresolvable_thumbnails) == 1
    assert len(harvest_report.thumbnail_image_urls) == 0


def test_resolve_invalid_thumbnail_url(
    requests_mock, thumbnail_image_urls, unresolvable_thumbnails
):
    record = json.loads(open("tests/data/ndjson/output-testmuseum.ndjson").readline())
    record["agg_preview"]["wr_id"] = "not_a_url"
    harvest_report.resolve_thumbnail_url(record)

    assert len(harvest_report.unresolvable_thumbnails) == 1
    assert len(harvest_report.thumbnail_image_urls) == 0


@pytest.fixture
def counts_global(monkeypatch):
    """Monkeypatch global variable"""
    counts = defaultdict(Counter)
    monkeypatch.setattr(harvest_report, "counts", counts)


def test_count_fields(counts_global):
    field = "cho_test_dict"
    metadata = {"en": ["Test item title"]}
    harvest_report.count_fields(field, metadata)
    assert dict(harvest_report.counts) == {
        "cho_test_dict": {"fields_covered": 1, "en": 1}
    }


def test_count_fields_str(counts_global):
    field = "agg_provider_collection_id"
    metadata = "abcd"
    harvest_report.count_fields(field, metadata)
    assert dict(harvest_report.counts) == {
        "agg_provider_collection_id": {"fields_covered": 1, "values": 1}
    }


def test_count_fields_dict(counts_global):
    field = "cho_test_edm"
    metadata = {"en": ["Object"], "ar-Arab": ["كائن"]}
    harvest_report.count_fields(field, metadata)
    assert dict(harvest_report.counts) == {
        "cho_test_edm": {"fields_covered": 1, "en": 1, "ar-Arab": 1}
    }
