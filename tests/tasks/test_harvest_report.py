import io
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
import requests
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from PIL import Image

from dlme_airflow.tasks import mapping_report


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
    requests_mock, mock_catalog_for_provider, reset_globals, thumbnail_image
):
    # Mock the harvest output data that is input to mapping_report. It is an ndjson file.
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        testmuseum_harvest_data = f.read()
    requests_mock.get(
        "https://s3-us-west-2.amazonaws.com/dlme-metadata-dev/output/output-testmuseum.ndjson",
        text=testmuseum_harvest_data,
    )

    # Mock traject config request to return a test config
    # f"https://raw.githubusercontent.com/sul-dlss/dlme-transform/main/traject_configs/{catalog.metadata.get('config')}.rb"
    with open("tests/data/testmuseum_config.rb") as f:
        testmuseum_config = f.read()
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


def test_thumbnail_report():
    images_sizes = [(100, 200), (400, 300), (100, 500)]
    report = mapping_report.thumbnail_report(images_sizes)

    assert (
        report
        == "67% of the 3 thumbnail images sampled had a width or height of 400 or greater."
    )


@pytest.fixture
def unresolvable_resources(monkeypatch):
    unresolvable_resources = []
    monkeypatch.setattr(
        mapping_report, "unresolvable_resources", unresolvable_resources
    )


def test_resolve_good_resource_url(requests_mock, unresolvable_resources):
    requests_mock.head("https://example.com", status_code=200)
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        record = json.loads(f.readline())
    mapping_report.resolve_resource_url(record)

    assert len(mapping_report.unresolvable_resources) == 0


def test_resolve_bad_resource_url(requests_mock, unresolvable_resources):
    requests_mock.head("https://example.com", status_code=404)
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        record = json.loads(f.readline())
    mapping_report.resolve_resource_url(record)

    assert len(mapping_report.unresolvable_resources) == 1


@pytest.fixture
def thumbnail_image_urls(monkeypatch):
    """Monkeypatch global variable"""
    thumbnail_image_urls = []
    monkeypatch.setattr(mapping_report, "thumbnail_image_urls", thumbnail_image_urls)


@pytest.fixture
def unresolvable_thumbnails(monkeypatch):
    """Monkeypatch global variable"""
    unresolvable_thumbnails = []
    monkeypatch.setattr(
        mapping_report, "unresolvable_thumbnails", unresolvable_thumbnails
    )


def test_resolve_good_thumbnail_url(
    requests_mock, thumbnail_image_urls, unresolvable_thumbnails
):
    requests_mock.get("https://example.com/image1.jpg", status_code=200)
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        record = json.loads(f.readline())
    mapping_report.resolve_thumbnail_url(record)

    assert len(mapping_report.unresolvable_thumbnails) == 0
    assert len(mapping_report.thumbnail_image_urls) == 1


def test_resolve_bad_thumbnail_url(
    requests_mock, thumbnail_image_urls, unresolvable_thumbnails
):
    requests_mock.get("https://example.com/image1.jpg", status_code=404)
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        record = json.loads(f.readline())
    mapping_report.resolve_thumbnail_url(record)

    assert len(mapping_report.unresolvable_thumbnails) == 1
    assert len(mapping_report.thumbnail_image_urls) == 0


def test_resolve_invalid_thumbnail_url(
    requests_mock, thumbnail_image_urls, unresolvable_thumbnails
):
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        record = json.loads(f.readline())
    record["agg_preview"]["wr_id"] = "not_a_url"
    mapping_report.resolve_thumbnail_url(record)

    assert len(mapping_report.unresolvable_thumbnails) == 1
    assert len(mapping_report.thumbnail_image_urls) == 0


def test_resolve_null_thumbnail_url(
    requests_mock, thumbnail_image_urls, unresolvable_thumbnails
):
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        record = json.loads(f.readline())
    record["agg_preview"]["wr_id"] = None
    mapping_report.resolve_thumbnail_url(record)

    assert len(mapping_report.unresolvable_thumbnails) == 1
    assert len(mapping_report.thumbnail_image_urls) == 0


@pytest.fixture
def counts_global(monkeypatch):
    """Monkeypatch global variable"""
    counts = defaultdict(Counter)
    monkeypatch.setattr(mapping_report, "counts", counts)


def test_count_fields(counts_global):
    field = "cho_test_dict"
    metadata = {"en": ["Test item title"]}
    mapping_report.count_fields(field, metadata)
    assert dict(mapping_report.counts) == {
        "cho_test_dict": {"fields_covered": 1, "en": 1}
    }


def test_count_fields_str(counts_global):
    field = "agg_provider_collection_id"
    metadata = "abcd"
    mapping_report.count_fields(field, metadata)
    assert dict(mapping_report.counts) == {
        "agg_provider_collection_id": {"fields_covered": 1, "values": 1}
    }


def test_count_fields_dict(counts_global):
    field = "cho_test_edm"
    metadata = {"en": ["Object"], "ar-Arab": ["كائن"]}
    mapping_report.count_fields(field, metadata)
    assert dict(mapping_report.counts) == {
        "cho_test_edm": {"fields_covered": 1, "en": 1, "ar-Arab": 1}
    }


def test_count_fields_ignored_field(counts_global):
    """Fields in IGNORE_FIELDS are silently skipped"""
    mapping_report.count_fields("id", "some-id-value")
    assert dict(mapping_report.counts) == {}


def test_count_fields_non_list_dict_value(counts_global):
    """Dict metadata with a non-list value counts as 1"""
    mapping_report.count_fields("agg_is_shown_at", {"wr_id": "https://example.com"})
    assert dict(mapping_report.counts) == {
        "agg_is_shown_at": {"fields_covered": 1, "wr_id": 1}
    }


# --- validate_url ---


def test_validate_url_valid():
    assert mapping_report.validate_url("https://example.com") is True


def test_validate_url_invalid():
    assert mapping_report.validate_url("not-a-url") is False


def test_validate_url_none():
    assert mapping_report.validate_url(None) is False


# --- write_file ---


def test_write_file_request_exception(requests_mock, tmp_path):
    """write_file logs error and re-raises on RequestException"""
    requests_mock.get(
        "https://example.com/config.rb",
        exc=requests.exceptions.ConnectionError,
    )
    with pytest.raises(requests.exceptions.ConnectionError):
        mapping_report.write_file(
            "https://example.com/config.rb", str(tmp_path / "config.rb")
        )


# --- thumbnail_report ---


def test_thumbnail_report_empty():
    """Returns a message when no thumbnail images are resolvable"""
    assert mapping_report.thumbnail_report([]) == "No thumbnail images were resolvable."


# --- resolve_resource_url ---


def test_resolve_resource_url_request_exception(requests_mock, unresolvable_resources):
    """Network error appends to unresolvable_resources"""
    requests_mock.head(
        "https://example.com", exc=requests.exceptions.ConnectionError
    )
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        record = json.loads(f.readline())
    mapping_report.resolve_resource_url(record)
    assert len(mapping_report.unresolvable_resources) == 1


def test_resolve_resource_url_invalid_url(unresolvable_resources):
    """Invalid URL appends to unresolvable_resources without making a request"""
    record = {
        "id": "test_id",
        "dlme_source_file": "test.ndjson",
        "agg_is_shown_at": {"wr_id": "not-a-valid-url"},
    }
    mapping_report.resolve_resource_url(record)
    assert len(mapping_report.unresolvable_resources) == 1


# --- resolve_thumbnail_url ---


def test_resolve_thumbnail_url_no_preview(thumbnail_image_urls, unresolvable_thumbnails):
    """Returns early without side-effects when agg_preview is absent"""
    record = {"id": "test_id", "dlme_source_file": "test.ndjson"}
    mapping_report.resolve_thumbnail_url(record)
    assert len(mapping_report.unresolvable_thumbnails) == 0
    assert len(mapping_report.thumbnail_image_urls) == 0


def test_resolve_thumbnail_url_request_exception(
    requests_mock, thumbnail_image_urls, unresolvable_thumbnails
):
    """Network error appends to unresolvable_thumbnails"""
    requests_mock.get(
        "https://example.com/image1.jpg",
        exc=requests.exceptions.ConnectionError,
    )
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        record = json.loads(f.readline())
    mapping_report.resolve_thumbnail_url(record)
    assert len(mapping_report.unresolvable_thumbnails) == 1
    assert len(mapping_report.thumbnail_image_urls) == 0


# --- sample_image_sizes ---


def test_sample_image_sizes_over_5000(requests_mock, thumbnail_image):
    """Samples exactly 250 URLs when the list exceeds 5000"""
    thumbnail_urls = ["https://example.com/image1.jpg"] * 5001
    requests_mock.get("https://example.com/image1.jpg", content=thumbnail_image)
    assert len(mapping_report.sample_image_sizes(thumbnail_urls)) == 250


def test_sample_image_sizes_request_exception(requests_mock):
    """Skips URLs that raise a RequestException and returns an empty list"""
    thumbnail_urls = ["https://example.com/image1.jpg"]
    requests_mock.get(
        "https://example.com/image1.jpg",
        exc=requests.exceptions.ConnectionError,
    )
    assert mapping_report.sample_image_sizes(thumbnail_urls) == []


# --- mapping_report integration: shared fixture ---


@pytest.fixture
def reset_globals(monkeypatch):
    """Reset all module-level mutable globals before each integration test"""
    monkeypatch.setattr(mapping_report, "thumbnail_image_urls", [])
    monkeypatch.setattr(mapping_report, "unresolvable_resources", [])
    monkeypatch.setattr(mapping_report, "unresolvable_thumbnails", [])
    monkeypatch.setattr(mapping_report, "counts", defaultdict(Counter))


# --- mapping_report: unresolvable URL sections in HTML ---


def test_mapping_report_with_unresolvable_urls(
    requests_mock, mock_catalog_for_provider, reset_globals
):
    """HTML report includes listing sections when thumbnails and resources are unresolvable"""
    with open("tests/data/testmuseum_config.rb") as f:
        testmuseum_config = f.read()

    requests_mock.get(
        "https://raw.githubusercontent.com/sul-dlss/dlme-transform/main/traject_configs/testmuseum.rb",
        text=testmuseum_config,
    )
    requests_mock.head("https://example.com", status_code=404)
    requests_mock.get("https://example.com/image1.jpg", status_code=404)

    doc = mapping_report.mapping_report(
        provider="testmuseum", collection="test", data_path="testmuseum"
    )

    assert "The following thumbnails urls were unresolvable when testing:" in doc
    assert "The following resource urls were unresolvable when testing:" in doc


# --- mapping_report: wr_edm_rights branch in rights report ---


@pytest.fixture
def mock_catalog_for_edm_museum(monkeypatch):
    """Mock catalog returning edm-museum collection with wr_edm_rights data"""

    def mockreturn(provider):
        class MockCatalog:
            def __init__(self):
                self.metadata = {"config": "testmuseum"}

        return MockCatalog()

    monkeypatch.setattr(
        "dlme_airflow.tasks.mapping_report.catalog_for_provider", mockreturn
    )
    monkeypatch.setattr(
        "dlme_airflow.models.provider.catalog_for_provider", mockreturn
    )

    def mock_collection(self, collection):
        class MockCollection:
            def __init__(self):
                self.name = collection

            def data_path(self):
                return "edm-museum"

            def intermediate_representation_location(self):
                return "output-edm-museum.ndjson"

        return MockCollection()

    monkeypatch.setattr(
        "dlme_airflow.models.provider.Provider.get_collection", mock_collection
    )


def test_mapping_report_wr_edm_rights(
    requests_mock, mock_catalog_for_edm_museum, reset_globals, thumbnail_image
):
    """Rights report uses wr_edm_rights count when it is present"""
    with open("tests/data/testmuseum_config.rb") as f:
        testmuseum_config = f.read()

    requests_mock.get(
        "https://raw.githubusercontent.com/sul-dlss/dlme-transform/main/traject_configs/testmuseum.rb",
        text=testmuseum_config,
    )
    requests_mock.head("https://example.com", status_code=200)
    requests_mock.get("https://example.com/image1.jpg", content=thumbnail_image)

    doc = mapping_report.mapping_report(
        provider="edm-museum", collection="test", data_path="edm-museum"
    )

    assert "1 of 1 records had a clearly expressed copyright status for the web resource." in doc


# --- mapping_report: EXTRACT_MACROS match in crosswalk ---


@pytest.fixture
def mock_catalog_for_extract_macros(monkeypatch):
    """Mock catalog pointing to the extract_macros_config fixture"""

    def mockreturn(provider):
        class MockCatalog:
            def __init__(self):
                self.metadata = {"config": "extract_macros"}

        return MockCatalog()

    monkeypatch.setattr(
        "dlme_airflow.tasks.mapping_report.catalog_for_provider", mockreturn
    )
    monkeypatch.setattr(
        "dlme_airflow.models.provider.catalog_for_provider", mockreturn
    )

    def mock_collection(self, collection):
        class MockCollection:
            def __init__(self):
                self.name = collection

            def data_path(self):
                return "extract-museum"

            def intermediate_representation_location(self):
                return "output-testmuseum.ndjson"

        return MockCollection()

    monkeypatch.setattr(
        "dlme_airflow.models.provider.Provider.get_collection", mock_collection
    )


def test_mapping_report_with_extract_macros(
    requests_mock, mock_catalog_for_extract_macros, reset_globals, thumbnail_image
):
    """Crosswalk table is populated when a config line matches an EXTRACT_MACRO key"""
    with open("tests/data/extract_macros_config.rb") as f:
        extract_macros_config = f.read()

    requests_mock.get(
        "https://raw.githubusercontent.com/sul-dlss/dlme-transform/main/traject_configs/extract_macros.rb",
        text=extract_macros_config,
    )
    requests_mock.head("https://example.com", status_code=200)
    requests_mock.get("https://example.com/image1.jpg", content=thumbnail_image)

    doc = mapping_report.mapping_report(
        provider="extract-museum", collection="test", data_path="extract-museum"
    )

    assert "Seperate values on ';', then downcase" in doc


# --- mapping_report: empty line in ndjson ---


def test_mapping_report_empty_line_in_ndjson(
    requests_mock, mock_catalog_for_provider, reset_globals, tmp_path, monkeypatch, thumbnail_image
):
    """Empty lines in the ndjson input are skipped without error"""
    with open("tests/data/ndjson/output-testmuseum.ndjson") as f:
        first_record = f.readline()

    ndjson_with_empty_line = first_record + "\n" + first_record
    ndjson_path = tmp_path / "output-testmuseum.ndjson"
    ndjson_path.write_text(ndjson_with_empty_line)
    monkeypatch.setenv("METADATA_REPORT_PATH", str(tmp_path))

    with open("tests/data/testmuseum_config.rb") as f:
        testmuseum_config = f.read()

    requests_mock.get(
        "https://raw.githubusercontent.com/sul-dlss/dlme-transform/main/traject_configs/testmuseum.rb",
        text=testmuseum_config,
    )
    requests_mock.head("https://example.com", status_code=200)
    requests_mock.get("https://example.com/image1.jpg", content=thumbnail_image)

    doc = mapping_report.mapping_report(
        provider="testmuseum", collection="test", data_path="testmuseum"
    )

    assert "2 of 2 records had valid urls to resources." in doc


# --- build_mapping_report_task ---


def test_build_mapping_report_task():
    """build_mapping_report_task returns a PythonOperator with the correct task_id"""
    dag = DAG("test_dag", start_date=datetime(2024, 1, 1, tzinfo=UTC))

    collection = MagicMock()
    collection.label.return_value = "testcollection"
    collection.provider.name = "testprovider"
    collection.name = "testcollection"
    collection.data_path.return_value = "testprovider/testcollection"

    with dag:
        task_group = TaskGroup("test_group", dag=dag)
        task = mapping_report.build_mapping_report_task(collection, task_group, dag)

    assert task.task_id == "test_group.testcollection_mapping_report"
