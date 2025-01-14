from unittest import mock

from dlme_airflow.drivers.hathi_trust import HathiTrustSource


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code

        def ok(self):
            return self.status_code == 200

        def read(self, *args, **kwargs):
            return self.content.read()

        def close(self):
            return None

    if args[0].endswith("example.org/collection"):
        return MockResponse(open("tests/data/hathi_trust/collection.txt", "r").read().encode("utf-8"), 200)
    if args[0].endswith("5245329.xml"):
        return MockResponse(open("tests/data/hathi_trust/5245329.xml", "rb"), 200)
    if args[0].endswith("2583088.xml"):
        return MockResponse(open("tests/data/hathi_trust/2583088.xml", "rb"), 200)
    return


@mock.patch('requests.get', side_effect=mocked_requests_get)
@mock.patch('urllib.request.urlopen', side_effect=mocked_requests_get)
def test_read(self, *args, **kwargs):

    metadata = {
        "data_path": "hathi",
        "catalog_url": "https://example.org/Record/{id}.xml",
    }

    source = HathiTrustSource(
        collection_url="https://example.org/collection",
        metadata=metadata,
        object_path="ht_bib_key",
    )

    df = source.read()
    print(df)
    assert len(df) == 2
