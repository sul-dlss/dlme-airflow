import pytest
import json
import requests
import pandas as pd
from sickle import Sickle
from sickle.models import Record, OAIItem
from lxml import etree

from drivers.oai_xml import OAIXmlSource


@pytest.fixture
def mock_oai_mods():
    with open("tests/fixtures/oai_mods.xml") as data:
        return data


@pytest.fixture
def mock_oai_record(mock_oai_mods, monkeypatch):
    def mock_raw():
        return mock_oai_mods
    
    monkeypatch.setattr(OAIItem, "raw", mock_raw)


@pytest.fixture
def mock_sickle_client(mock_oai_record, monkeypatch):
    def mock_list_records(*args, **kwargs):
        metadata_prefix = kwargs["metadataPrefix"]
        set = kwargs["set"]
        ignore_deleted = kwargs["ignore_deleted"]
        breakpoint()
        return mock_oai_record
    
    monkeypatch.setattr(Sickle, "ListRecords", mock_list_records)
    # monkeypatch.setattr(OAIItem, "raw", mock_oai_mods)

# @pytest.fixture
# def mock_response(monkeypatch):
#     def mock_get(*args, **kwargs):
#         if args[0].endswith("collection.json"):
#             return MockIIIFCollectionResponse()
#         if args[0].endswith("manifest.json"):
#             return MockIIIFManifestResponse()
#         return

#     monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def oai_mock_source():
    metadata = {
        "fields": {
            "id": {
                "path": "//header:identifier",
                "optional": True,
                "namespace": {
                    "header": "http://www.openarchives.org/OAI/2.0/",
                }
            },
        }
    }
    return OAIXmlSource(
        collection_url="http://oai_mods_collection.xml",
        set="abc",
        metadata_prefix="mods",
        metadata=metadata
    )


def test_OAIXmlSource_initial(oai_mock_source):
    assert len(oai_mock_source._records) == 0


def test_OAIXmlSource_open_set(oai_mock_source, mock_sickle_client):
    assert len(oai_mock_source._open_set()) == 1

# def test_IIIfJsonSource_get_schema(iiif_test_source, mock_response):
#     iiif_test_source._get_schema()
#     assert (
#         iiif_test_source._manifest_urls[0]
#         == "https://collection.edu/iiif/p15795coll29:28/manifest.json"
#     )


# def test_IIIfJsonSource_read(iiif_test_source, mock_response):
#     iiif_df = iiif_test_source.read()
#     test_columns = ["context", "iiif_format", "source", "title-main"]
#     assert all([a == b for a, b in zip(iiif_df.columns, test_columns)])


# def test_test_IIIfJsonSource_df(iiif_test_source, mock_response):
#     iiif_df = iiif_test_source.read()
#     test_df = pd.DataFrame(
#         [
#             {
#                 "context": "http://iiif.io/api/presentation/2/context.json",
#                 "iiif_format": "image/jpeg",
#                 "source": "Rare Books and Special Collections Library",
#                 "title-main": "A great title of the Middle East",
#             }
#         ]
#     )
#     assert iiif_df.equals(test_df)
