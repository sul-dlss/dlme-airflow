import pytest
import requests

from dlme_airflow.utils.schema import get_schema


def test_get_schema():
    data = get_schema("https://collections.penn.museum/collections/object/15390")
    assert (
        data["thumbnailUrl"]
        == "https://www.penn.museum//collections/assets/032T/327k/327554_800.jpg"
    )


def test_no_schema():
    data = get_schema("http://example.com")
    assert data is None


def test_no_html(requests_mock):
    url = "https://example.org/image.jpg"
    requests_mock.get(url, content=b"\xff\xd8\xff", headers={"content-type": "image/jpeg"})
    data = get_schema(url)
    assert data is None


def test_http_error(requests_mock):
    url = "https://example.org/object/1"
    requests_mock.get(url, status_code=403, text="<html>Forbidden</html>", headers={"content-type": "text/html"})
    with pytest.raises(requests.HTTPError):
        get_schema(url)


def test_control_chars():
    data = get_schema("https://collections.penn.museum/collections/object/46326")
    assert type(data) is dict


def test_stray_backslash(requests_mock):
    url = "https://example.org/object/455157"
    html = (
        '<html><script type="application/ld+json">'
        '{"description": "8661a joins 4544\\4550 \\\\5 \\"quoted\\" \\u00e9", "thumbnailUrl": "https://example.org/t.jpg"}'
        "</script></html>"
    )
    requests_mock.get(url, text=html, headers={"content-type": "text/html"})
    data = get_schema(url)
    assert data["thumbnailUrl"] == "https://example.org/t.jpg"
    assert data["description"] == '8661a joins 4544\\4550 \\5 "quoted" é'
