import json

from dlme_airflow.drivers.json import JsonSource


def test_happy_path_pages_from_provider(requests_mock):
    # mock the http call to return some real loc.gov data
    loc_data = json.load(open("tests/data/json/loc.json"))
    requests_mock.get(
        "https://www.loc.gov/collections/persian-language-rare-materials/?c=100&fo=json",
        json=loc_data,
    )

    # the collection selector json path defined in the Intake catalog
    record_selector = "content.results"

    # the collection url defined in the Intake catalog
    collection_url = (
        "https://www.loc.gov/collections/persian-language-rare-materials/?c=100&fo=json"
    )

    # this jsonpath configuration is required in the intake catalog
    metadata = {
        "fields": {
            "id": {"path": "id"},
            "title": {"path": "title"},
            "thumbnail": {
                "path": "resources[0].image",
                "optional": True,  # some records lack a thumbnail
            },
        },
    }

    # create our JsonSource object
    js = JsonSource(
        collection_url,
        record_selector=record_selector,
        metadata=metadata,
        paging={"urls": "pagination.page_list"},
    )

    # get the DataFrame
    df = js.read()

    assert len(df) == 100
    assert len(df.columns) == 3
    assert set(df.columns) == set(["id", "title", "thumbnail"])

    assert df.id[0] == "http://www.loc.gov/item/2017498321/"
    assert df.title[0] == "[Majmuʻah, or, Collection]"
    assert (
        df.thumbnail[0]
        == "https://tile.loc.gov/image-services/iiif/service:amed:plmp:m154:0334/full/pct:6.25/0/default.jpg"
    )

    assert df.id[99] == "http://www.loc.gov/item/00313408/"
    assert (
        df.title[99] == "Sakīnat al-fuz̤alāʼ mawsūm bi-ism-i tārīkhī-i Bahār-i Afghānī"
    )
    assert (
        df.thumbnail[99]
        == "https://tile.loc.gov/image-services/iiif/service:amed:amedpllc:00406537456:0055/full/pct:6.25/0/default.jpg"
    )


def test_happy_path_incremental_paging(requests_mock):
    # mock the http call to return some real loc.gov data
    loc_data = json.load(open("tests/data/json/incremental_paging.json"))
    requests_mock.get(
        "https://example.com/",
        json=loc_data,
    )

    # the collection selector json path defined in the Intake catalog
    record_selector = "results"

    # the collection url defined in the Intake catalog
    collection_url = "https://example.com/"

    # this jsonpath configuration is required in the intake catalog
    metadata = {
        "fields": {
            "id": {"path": "accessionNumber"},
            "title": {"path": "title"},
            "thumbnail": {"path": "image"},
        },
    }

    # create our JsonSource object
    js = JsonSource(
        collection_url,
        record_selector=record_selector,
        metadata=metadata,
        paging={
            "increment": 20,
            "query_param": "offset",
            "result_count": "totalResults",
        },
    )

    # get the DataFrame
    df = js.read()

    assert len(df) == 20
    assert len(df.columns) == 3
    assert set(df.columns) == set(["id", "title", "thumbnail"])

    assert df.id[0] == "23.3.935"
    assert df.title[0] == "Wall casing"
    assert (
        df.thumbnail[0]
        == "https://images.metmuseum.org/CRDImages/eg/mobile-large/bb468-70.jpg"
    )

    assert df.id[19] == "26.7.1183"
    assert df.title[19] == "Kohl Tube in the Form of a Papyrus Column"
    assert (
        df.thumbnail[19]
        == "https://images.metmuseum.org/CRDImages/eg/mobile-large/LC-26_7_1183_EGDP034542.jpg"
    )


def test_happy_path_prefetch_urls(requests_mock):
    # mock the http call to return some real loc.gov data
    object_pages = json.load(open("tests/data/json/pre_fetch_ids.json"))
    requests_mock.get(
        "https://example.com/collection?limit=3&offset=0",
        json=object_pages,
    )

    data_for_116787 = json.load(open("tests/data/json/116787.json"))
    requests_mock.get(
        "https://example.com/object/116787",
        json=data_for_116787,
    )

    data_for_60732 = json.load(open("tests/data/json/60732.json"))
    requests_mock.get(
        "https://example.com/object/60732",
        json=data_for_60732,
    )

    # the collection selector json path defined in the Intake catalog
    record_selector = "data"

    # the collection url defined in the Intake catalog
    collection_url = "https://example.com/object/"

    # this jsonpath configuration is required in the intake catalog
    metadata = {
        "fields": {
            "id": {"path": "id"},
            "title": {"path": "title"},
            "thumbnail": {"path": "primary_image"},
        },
    }

    # create our JsonSource object
    js = JsonSource(
        collection_url,
        record_selector=record_selector,
        metadata=metadata,
        paging={
            "pages_url": "https://example.com/collection?limit={limit}&offset={offset}",
            "urls": "data.id",
            "limit": 3,
            "page_data": "data",
        },
    )

    # get the DataFrame
    df = js.read()

    assert len(df) == 2
    assert len(df.columns) == 3
    assert set(df.columns) == set(["id", "title", "thumbnail"])
