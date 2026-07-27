import pytest
from sickle.oaiexceptions import BadResumptionToken

from dlme_airflow.drivers.oai_xml import OaiXmlSource


def test_oai_record(requests_mock):
    with open("tests/data/xml/record_12345.xml") as f:
        record_12345_text = f.read()
    requests_mock.get(
        "https://example.org?metadataPrefix=mods_no_ocr&verb=GetRecord&identifier=12345",
        text=record_12345_text,
    )
    oai = OaiXmlSource(
        "https://example.org",
        "mods_no_ocr",
        metadata={
            "identifier": "12345",
            "fields": {
                "title": {
                    "path": "//mods:titleInfo[not(ancestor::mods:relatedItem)]/mods:title",
                    "namespace": {
                        "mods": "http://www.loc.gov/mods/v3",
                    },
                    "optional": "false",
                },
            },
        },
    )
    df = oai.read()
    assert len(df) == 1, "One record expected"
    assert df.iloc[0]["identifier"] == ["12345"]
    assert df.iloc[0]["title"] == [
        '‘A draught of the South land lately discovered’ ["Tasmania"] and Covering Sheet'
    ]


def test_oai_dc(requests_mock):
    with open("tests/data/xml/oai-dc.xml") as f:
        oai_dc_text = f.read()
    requests_mock.get(
        "https://example.org?metadataPrefix=oai_dc&verb=ListRecords",
        text=oai_dc_text,
    )
    oai = OaiXmlSource("https://example.org", "oai_dc")
    df = oai.read()
    assert len(df) == 100, "expected number of rows"
    assert len(df.columns) == 14, "expected number of columns"
    assert "title" in df.columns
    assert "publisher" in df.columns
    assert df.iloc[0]["publisher"] == ["HAL CCSD", "Wiley-VCH Verlag"]


def test_mods(requests_mock):
    with open("tests/data/xml/oai-mods.xml") as f:
        oai_mods_text = f.read()
    requests_mock.get(
        "https://example.org?metadataPrefix=mods_no_ocr&verb=ListRecords",
        text=oai_mods_text,
    )
    oai = OaiXmlSource("https://example.org", "mods_no_ocr")
    df = oai.read()
    assert len(df) == 20, "expected number of rows"
    assert len(df.columns) == 14, "expected number of columns"
    assert "location_shelfLocator" in df.columns, "hierarchical data encoded in header"
    assert [isinstance(i, list) for i in df["subject_name_namePart"]]


def test_marc21(requests_mock):
    with open("tests/data/xml/oai-marc21.xml") as f:
        oai_marc21_text = f.read()
    requests_mock.get(
        "https://example.org?metadataPrefix=marc21&verb=ListRecords",
        text=oai_marc21_text,
    )
    oai = OaiXmlSource("https://example.org", "marc21")
    df = oai.read()
    assert len(df) == 182, "expected number of rows"
    assert len(df.columns) == 52, "expected number of columns"
    assert "245_a" in df.columns, "marc field 245 subfield a extracted"
    assert "035_a" in df.columns, "marc field 035 subfield a extracted"
    assert len(df.iloc[0]["035_a"]) == 3, "much field 035 subfield a contains 3 values"


def test_oai_dpla(requests_mock):
    with open("tests/data/xml/oai-dpla.xml") as f:
        oai_dpla_text = f.read()
    requests_mock.get(
        "https://example.org?metadataPrefix=oai_dpla&verb=ListRecords",
        text=oai_dpla_text,
    )
    oai = OaiXmlSource("https://example.org", "oai_dpla")
    df = oai.read()
    assert len(df) == 25, "expected number of rows"
    assert len(df.columns) == 14, "expected number of columns"
    assert "title" in df.columns, "extracte title"
    assert "description" in df.columns, "extracted description"


def test_construct_fields(requests_mock):
    with open("tests/data/xml/oai-mods.xml") as f:
        oai_mods_text = f.read()
    requests_mock.get(
        "https://example.org?metadataPrefix=mods_no_ocr&verb=ListRecords",
        text=oai_mods_text,
    )
    oai = OaiXmlSource("https://example.org", "mods_no_ocr")
    df = oai.read()
    assert len(df.subject_topic[0]) == 2


def test_wait():
    # ensure that the wait option can be used
    oai = OaiXmlSource("https://example.org", "oai_dc", wait=2)
    assert oai


def test_invalid_resumption_token(requests_mock):
    # return a response with a resumption token for the next set of results
    with open("tests/data/xml/oai-dc-resumption.xml") as f:
        oai_dc_resumption_text = f.read()
    requests_mock.get(
        "https://example.org?metadataPrefix=oai_dc&verb=ListRecords",
        text=oai_dc_resumption_text,
    )

    # returns an invalid resumption token error
    with open("tests/data/xml/oai-dc-resumption-error.xml") as f:
        oai_dc_resumption_error_text = f.read()
    requests_mock.get(
        "https://example.org?resumptionToken=abc123&verb=ListRecords",
        text=oai_dc_resumption_error_text,
    )

    # this should throw an exception
    oai = OaiXmlSource("https://example.org", "oai_dc")
    with pytest.raises(BadResumptionToken):
        df = oai.read()

    # allow_expiration should return what was collected
    oai = OaiXmlSource("https://example.org", "oai_dc", allow_expiration=True)
    df = oai.read()
    assert len(df) == 100
