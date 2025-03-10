from dlme_airflow.drivers.xml import XmlSource


def test_read(requests_mock):
    requests_mock.get(
        "https://example.com/themaghribpodcast/feed.xml",
        text=open("tests/data/xml/aims-feed.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    itunes_namespace = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"}
    yahoo_namespace = {"media": "http://search.yahoo.com/mrss/"}
    metadata = {
        "data_path": "aims",
        "record_selector": {"path": ".//item", "namespace": None},
        "fields": {
            "id": {"path": ".//guid", "namespace": None, "optional": False},
            "title": {"path": ".//title", "namespace": None, "optional": True},
            "author": {
                "path": ".//itunes:author",
                "namespace": itunes_namespace,
                "optional": True,
            },
            "pub_date": {"path": ".//pubDate", "namespace": None, "optional": True},
            "description": {
                "path": ".//description",
                "namespace": None,
                "optional": True,
            },
            "extent": {
                "path": ".//itunes:duration",
                "namespace": itunes_namespace,
                "optional": True,
            },
            "link": {"path": ".//link", "namespace": None, "optional": True},
            "thumbnail": {
                "path": ".//media:content/@url",
                "namespace": yahoo_namespace,
                "optional": True,
            },
        },
    }

    source = XmlSource(
        collection_url="https://example.com/themaghribpodcast/feed.xml",
        metadata=metadata,
    )

    df = source.read()

    cols = [
        "id",
        "title",
        "author",
        "pub_date",
        "description",
        "extent",
        "link",
        "thumbnail",
    ]

    assert set(df.columns) == set(cols)
    assert len(df) == 144

    assert (
        df.id[0] == "themaghribpodcast.podbean.com/7ab002fd-c4ac-3b48-96fa-eb83f4f324d5"
    )
    assert df.title[0] == "Contemporary Art in Tunisia"
    assert df.author[0] == "themaghribpodcast.com"
    assert df.pub_date[0] == "Thu, 02 Jun 2022 09:31:00 -0500"

    assert (
        df.id[143]
        == "themaghribpodcast.podbean.com/volubilis-between-romans-awraba-and-idris-i-24b00f90dd1700f333f1ee8f1a4eedd1"
    )
    assert df.title[143] == "Volubilis: Between Romans, Awraba and Idris I"
    assert df.author[143] == "themaghribpodcast.com"
    assert df.pub_date[143] == "Mon, 25 Sep 2017 09:58:46 -0500"


def test_multi_field(requests_mock):
    requests_mock.get(
        "https://example.com/",
        text=open("tests/data/xml/multi.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    metadata = {
        "record_selector": {"path": ".//record", "namespace": None},
        "fields": {
            "id": {"path": "id", "namespace": None},
            "title": {
                "path": "title",
                "namespace": None,
            },
            "name": {"path": "name", "namespace": None},
        },
    }

    source = XmlSource(
        collection_url="https://example.com/",
        metadata=metadata,
    )

    df = source.read()
    assert len(df) == 2
    assert df.name[0] == ["Donna Haraway", "bell hooks"]
    assert df.name[1] == ["Elinor Ostrom", "Ivan Illich"]


def test_paged_xml(requests_mock):
    requests_mock.get(
        "https://example.com/?paged=1",
        text=open("tests/data/xml/paged/page_1.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    requests_mock.get(
        "https://example.com/?paged=2",
        text=open("tests/data/xml/paged/page_2.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    requests_mock.get(
        "https://example.com/?paged=3",
        text=None,
        headers={"Accept": "application/text+xml"},
    )

    metadata = {
        "record_selector": {"path": ".//item", "namespace": None},
        "fields": {
            "id": {"path": ".//guid", "namespace": None},
            "title": {
                "path": ".//title",
                "namespace": None,
            },
            "creator": {"path": ".//dc:creator", "namespace": {"dc": "http://purl.org/dc/elements/1.1/"}},
        },
    }

    source = XmlSource(
        collection_url="https://example.com/?paged={offset}",
        metadata=metadata,
        paging={"increment": 1},
    )

    df = source.read()
    assert(len(df) == 4)
    assert df.title[0] == 'Plasco Building, Tehran'
    assert df.title[1] == "1920s Vintage Ads in Iranian Newspapers"
    assert df.title[2] == "Ettelaat Daily’s Old Building"
    assert df.title[3] == "Iran National Football Team"

def test_resumptionToken_paged_xml(requests_mock):
    requests_mock.get(
        "https://example.com/export?resumptionToken=0",
        text=open("tests/data/xml/paged/resumption_token_1.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    requests_mock.get(
        "https://example.com/export?resumptionToken=2",
        text=open("tests/data/xml/paged/resumption_token_2.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    metadata = {
        "record_selector": {"path": ".//item", "namespace": None},
        "fields": {
            "id": {"path": ".//node_id", "namespace": None},
            "title": {"path": ".//title", "namespace": None},
            "type": {"path": ".//type", "namespace": None},
        },
    }

    source = XmlSource(
        collection_url="https://example.com/export?resumptionToken={offset}",
        metadata=metadata,
        paging={"resumptionToken": True, "increment": 0},
    )

    df = source.read()
    assert(len(df) == 2)
    assert df.title[0] == 'First page of records'
    assert df.title[1] == "Second page of records"


def test_pagination_paged_xml(requests_mock):
    requests_mock.get(
        "https://example.com/export?limit=100&start=0",
        text=open("tests/data/xml/paged/pagination_1.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    requests_mock.get(
        "https://example.com/export?limit=100&start=100",
        text=open("tests/data/xml/paged/pagination_2.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    metadata = {
        "record_selector": {"path": "/h:results/h:items/mods:mods", "namespace": {"h": "http://api.lib.harvard.edu/v2/item", "mods": "http://www.loc.gov/mods/v3"}},
        "fields": {
            "title": {"path": "//mods:titleInfo[not(ancestor::mods:relatedItem)]/mods:title", "namespace": {"mods": "http://www.loc.gov/mods/v3"}},
        },
    }

    source = XmlSource(
        collection_url="https://example.com/export?limit={offset}&start={start}",
        metadata=metadata,
        paging={
            "pagination": True,
            "increment": 100,
            "max_results": {"path": "/h:results/h:pagination/h:maxPageableSet", "namespace": {"h": "http://api.lib.harvard.edu/v2/item"}},
            "num_results": {"path": "/h:results/h:pagination/h:numFound", "namespace": {"h": "http://api.lib.harvard.edu/v2/item"}}
        }
    )

    df = source.read()
    assert(len(df) == 2)
    assert df.title[0] == 'Title 1'
    assert df.title[1] == "Title 2"


def test_parse_record_urls_xml(requests_mock):
    requests_mock.get(
        "https://example.com/collection.html",
        text=open("tests/data/xml/paged/collection.html", "r").read(),
        headers={"Accept": "application/html"},
    )

    requests_mock.get(
        "https://example.com/record_1.xml",
        text=open("tests/data/xml/paged/record_1.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    requests_mock.get(
        "https://example.com/record_2.xml",
        text=open("tests/data/xml/paged/record_2.xml", "r").read(),
        headers={"Accept": "application/text+xml"},
    )

    metadata = {
        "fields": {
            "title": {"path": "/TEI/teiHeader/fileDesc/titleStmt/title"},
        },
    }

    source = XmlSource(
        collection_url="https://example.com/collection.html",
        metadata=metadata,
        paging={"link_text": "TEI XML", "base_url": "https://example.com"},
    )

    df = source.read()
    assert(len(df) == 2)
    assert df.title[0] == 'Title 1'
    assert df.title[1] == "Title 2"
