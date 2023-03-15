import pandas
import pytest
import shutil
import logging

from pathlib import Path

from dlme_airflow.models.provider import Provider
from dlme_airflow.models.collection import Collection
from dlme_airflow.tasks import transform_validation
from dlme_airflow.tasks.transform_validation import validate_transformation


@pytest.fixture
def cleanup():
    metadata = Path("test-metadata")
    # setup
    if metadata.is_dir():
        shutil.rmtree(metadata)
    yield
    # teardown
    if metadata.is_dir():
        shutil.rmtree(metadata)


@pytest.fixture
def setup_df(monkeypatch):
    # mock the dataframe for the harvest
    df = pandas.DataFrame({"id": [1, 2, 3], "title": ["a", "b", "c"]})
    monkeypatch.setattr(transform_validation, "dataframe_from_file", lambda _: df)


@pytest.fixture
def setup_df_extra(monkeypatch):
    # mock a dataframe for the harvest with an extra row which will not match ndson
    df = pandas.DataFrame({"id": [1, 2, 3, 4], "title": ["a", "b", "c", "d"]})
    monkeypatch.setattr(transform_validation, "dataframe_from_file", lambda _: df)


@pytest.fixture
def setup_ndjson(monkeypatch):
    # mock the transformed dataframe serialized as ndjson
    ndjson = Path("test-metadata/aims/aims/output-aims-aims.ndjson")
    ndjson.parent.mkdir(parents=True)
    monkeypatch.setattr(transform_validation, "get_transformed_path", lambda _: ndjson)
    open(ndjson, "w").writelines(
        [
            '{"id": 1, "title": "a"}\n',
            '{"id": 2, "title": "b"}\n',
            '{"id": 3, "title": "c"}\n',
        ]
    )


def test_transform_path():
    collection = Collection(Provider("princeton"), "islamic_manuscripts")
    path = transform_validation.get_transformed_path(collection)
    assert str(path).endswith("metadata/output-princeton-islamic-manuscripts.ndjson")


def test_validation_passes(caplog, cleanup, setup_df, setup_ndjson):
    collection = Collection(Provider("aims"), "aims")

    with caplog.at_level(logging.INFO):
        validate_transformation(collection)

    assert (
        "OK: dataframe harvested record count == transform output record count (3)"
        in caplog.text
    )


def test_validation_fails(caplog, cleanup, setup_df_extra, setup_ndjson):
    collection = Collection(Provider("aims"), "aims")

    with pytest.raises(Exception) as excinfo:
        with caplog.at_level(logging.DEBUG):
            validate_transformation(collection)

    assert (
        "ERROR: failed to transform all harvested records: harvested record count (4) != transformed record count (3)"
        in str(excinfo.value)
    )

    assert (
        "OK: dataframe harvested record count == transform output record count"
        not in caplog.text
    )
