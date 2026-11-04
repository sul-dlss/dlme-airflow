import json
from unittest.mock import patch

import pandas as pd
import pytest
import requests

from dlme_airflow.utils.add_thumbnails import (
    CACHE_FILENAME,
    add_thumbnails,
    get_thumbnail,
)


@pytest.fixture
def mock_collection(tmp_path):
    working_json = tmp_path / "data.json"

    class MockCollection:
        def data_path(self):
            return '/mock/path'

        def datafile(self, filetype):
            return str(working_json)

    return MockCollection()


def url(id):
    return f'https://example.org/object/{id}'


def write_records(collection, urls):
    pd.DataFrame({'Record URL': urls}).to_json(collection.datafile('json'), orient='records')


def read_records(collection):
    return pd.read_json(collection.datafile('json'))


def cache_path(collection):
    return collection.datafile('json').replace('data.json', CACHE_FILENAME)


def test_add_thumbnails_success(mock_collection, monkeypatch):
    def mock_get_thumbnail(id):
        return f'{id}.jpg'

    monkeypatch.setattr('dlme_airflow.utils.add_thumbnails.get_thumbnail', mock_get_thumbnail)
    write_records(mock_collection, [url(1), url(2), url(3)])

    add_thumbnails(collection=mock_collection)

    df = read_records(mock_collection)
    assert sorted(df['thumbnail']) == [f'{url(i)}.jpg' for i in (1, 2, 3)]

    with open(cache_path(mock_collection)) as f:
        assert json.load(f) == {url(i): f'{url(i)}.jpg' for i in (1, 2, 3)}


def test_add_thumbnails_uses_cache(mock_collection, monkeypatch):
    with open(cache_path(mock_collection), 'w') as f:
        json.dump({url(1): 'http://cached1.jpg'}, f)

    looked_up = []

    def mock_get_thumbnail(id):
        looked_up.append(id)
        return f'{id}.jpg'

    monkeypatch.setattr('dlme_airflow.utils.add_thumbnails.get_thumbnail', mock_get_thumbnail)
    write_records(mock_collection, [url(1), url(2)])

    add_thumbnails(collection=mock_collection)

    assert looked_up == [url(2)]
    df = read_records(mock_collection).set_index('Record URL')
    assert df.loc[url(1), 'thumbnail'] == 'http://cached1.jpg'
    assert df.loc[url(2), 'thumbnail'] == f'{url(2)}.jpg'


def test_add_thumbnails_drops_records_without_thumbnail(mock_collection, monkeypatch):
    monkeypatch.setattr(
        'dlme_airflow.utils.add_thumbnails.get_thumbnail',
        lambda id: None if id == url(2) else f'{id}.jpg',
    )
    write_records(mock_collection, [url(1), url(2), url(3)])

    add_thumbnails(collection=mock_collection)

    assert sorted(read_records(mock_collection)['Record URL']) == [url(1), url(3)]


def test_add_thumbnails_tolerates_a_few_failures(mock_collection, monkeypatch):
    def mock_get_thumbnail(id):
        if id == url(0):
            raise requests.ConnectionError('timed out')
        return f'{id}.jpg'

    monkeypatch.setattr('dlme_airflow.utils.add_thumbnails.get_thumbnail', mock_get_thumbnail)
    write_records(mock_collection, [url(i) for i in range(100)])

    add_thumbnails(collection=mock_collection)

    assert len(read_records(mock_collection)) == 99


def test_add_thumbnails_fails_when_too_many_lookups_fail(mock_collection, monkeypatch):
    def mock_get_thumbnail(id):
        if int(id.rsplit('/', 1)[-1]) < 10:
            raise requests.ConnectionError('timed out')
        return f'{id}.jpg'

    monkeypatch.setattr('dlme_airflow.utils.add_thumbnails.get_thumbnail', mock_get_thumbnail)
    write_records(mock_collection, [url(i) for i in range(100)])

    with pytest.raises(RuntimeError, match='10 of 100 thumbnail lookups failed'):
        add_thumbnails(collection=mock_collection)

    # successful lookups are kept so a re-run only retries the failures
    with open(cache_path(mock_collection)) as f:
        assert len(json.load(f)) == 90

    # the working data is left untouched
    assert 'thumbnail' not in read_records(mock_collection).columns


def test_get_thumbnail_success():
    # Mock the schema response
    with patch('dlme_airflow.utils.add_thumbnails.get_schema', return_value={"thumbnailUrl": "http://thumbnail.jpg"}):
        result = get_thumbnail('123')

        # Assertions
        assert result == "http://thumbnail.jpg"


def test_get_thumbnail_no_schema():
    # Mock the schema response as None
    with patch('dlme_airflow.utils.add_thumbnails.get_schema', return_value=None):
        result = get_thumbnail('123')

        # Assertions
        assert result is None


def test_get_thumbnail_json_decode_error():
    # Simulate a JSON decode error
    with patch('dlme_airflow.utils.add_thumbnails.get_schema', side_effect=json.JSONDecodeError("Expecting value", "doc", 0)):
        result = get_thumbnail('123')

        # Assertions
        assert result is None


def test_get_thumbnail_network_error_is_raised():
    # Network errors are raised so add_thumbnails can count them as failures
    with (
        patch('dlme_airflow.utils.add_thumbnails.get_schema', side_effect=requests.ConnectionError('timed out')),
        pytest.raises(requests.ConnectionError),
    ):
        get_thumbnail('123')
