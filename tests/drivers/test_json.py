import json

from dlme_airflow.drivers.json import JsonSource


def test_happy_path(requests_mock):
    # mock the http call to return some real loc.gov data
    loc_data = json.load(open("tests/data/json/loc.json"))
    requests_mock.get(
        "https://www.loc.gov/collections/persian-language-rare-materials/?c=100&fo=json",
        json=loc_data,
    )

    # the collection url defined in the Intake catalog
    collection_url = (
        "https://www.loc.gov/collections/persian-language-rare-materials/?c=100&fo=json"
    )

    # this jsonpath configuration is required in the intake catalog
    metadata = {
        "record_selector": "content.results",
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
    js = JsonSource(collection_url, metadata=metadata)

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
