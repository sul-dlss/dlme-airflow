import pytest

from sickle.oaiexceptions import BadResumptionToken
from dlme_airflow.drivers.oai_xml import OaiXmlSource


def test_oai_dc(requests_mock):
    requests_mock.get(
        "https://example.org?metadataPrefix=oai_dc&verb=ListRecords",
        text=open("tests/data/xml/oai-dc.xml").read(),
    )
    oai = OaiXmlSource("https://example.org", "oai_dc")
    df = oai.read()
    assert len(df) == 100, "expected number of rows"
    assert len(df.columns) == 14, "expected number of columns"
    assert "title" in df.columns


def test_mods(requests_mock):
    requests_mock.get(
        "https://example.org?metadataPrefix=mods_no_ocr&verb=ListRecords",
        text=open("tests/data/xml/oai-mods.xml").read(),
    )
    oai = OaiXmlSource("https://example.org", "mods_no_ocr")
    df = oai.read()
    assert len(df) == 10, "expected number of rows"
    assert len(df.columns) == 14, "expected number of columns"
    assert "location_shelfLocator" in df.columns, "hierarchical data encoded in header"


def test_marc21(requests_mock):
    requests_mock.get(
        "https://example.org?metadataPrefix=marc21&verb=ListRecords",
        text=open("tests/data/xml/oai-marc21.xml").read(),
    )
    oai = OaiXmlSource("https://example.org", "marc21")
    df = oai.read()
    assert len(df) == 182, "expected number of rows"
    assert len(df.columns) == 52, "expected number of columns"
    assert "245_a" in df.columns, "marc field 245 subfield a extracted"


def test_wait():
    # ensure that the wait option can be used
    oai = OaiXmlSource("https://example.org", "oai_dc", wait=2)
    assert oai


def test_invalid_resumption_token(requests_mock):
    # return a response with a resumption token for the next set of results
    requests_mock.get(
        "https://example.org?metadataPrefix=oai_dc&verb=ListRecords",
        text=open("tests/data/xml/oai-dc-resumption.xml").read(),
    )

    # returns an invalid resumption token error
    requests_mock.get(
        "https://example.org?resumptionToken=abc123&verb=ListRecords",
        text=open("tests/data/xml/oai-dc-resumption-error.xml").read(),
    )

    # this should throw an exception
    oai = OaiXmlSource("https://example.org", "oai_dc")
    with pytest.raises(BadResumptionToken):
        df = oai.read()

    # allow_expiration should return what was collected
    oai = OaiXmlSource("https://example.org", "oai_dc", allow_expiration=True)
    df = oai.read()
    assert len(df) == 100
