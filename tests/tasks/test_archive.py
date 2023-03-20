import pytest
import shutil
import datetime

from pathlib import Path
from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider
from dlme_airflow.tasks.archive import archive_collection

test_working = Path("test-working")
test_archive = Path("test-archive")
test_dir = test_working / "aub" / "aco"
test_csv = test_dir / "data.csv"
test_json = test_dir / "data.json"
test_now = datetime.datetime(2023, 3, 13, 18, 6, 31)


@pytest.fixture
def mock_collection_datafile(monkeypatch):
    def mock_datafile(_, format):
        match format:
            case "json":
                return str(test_json.absolute())
            case _:
                return str(test_csv.absolute())

    monkeypatch.setattr(
        "dlme_airflow.models.collection.Collection.datafile", mock_datafile
    )


@pytest.fixture
def mock_now(monkeypatch):
    monkeypatch.setattr("dlme_airflow.tasks.archive.now", lambda: test_now)


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setenv("ARCHIVE_PATH", str(test_archive))

    if test_working.is_dir():
        shutil.rmtree(test_working)
    if test_archive.is_dir():
        shutil.rmtree(test_archive)

    test_csv.parent.mkdir(parents=True)
    test_archive.mkdir(parents=True)

    yield  # the following will run during test teardown

    if test_working.is_dir():
        shutil.rmtree(test_working)
    if test_archive.is_dir():
        shutil.rmtree(test_archive)


def test_archive_dir():
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    assert collection.archive_dir().endswith("archive/aub/aco")


def test_csv_with_data(setup, mock_collection_datafile, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")

    fh = test_csv.open("w")
    fh.write("id,author,title\n")
    fh.write("1,Jalāl al-Dīn Muḥammad Rūmī,Maṭnawīye Ma'nawī\n")
    fh.close()

    result = archive_collection(collection=collection)

    assert result is not None
    assert result["csv"].endswith(
        "test-archive/aub/aco/data-20230313180631.csv"
    ), "returned CSV archive filename"
    assert Path(result["csv"]).is_file(), "archived file exists"
    assert test_csv.is_file(), "original data file should still be there"


def test_empty_csv(setup, mock_collection_datafile, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    test_csv.touch()

    result = archive_collection(collection=collection)

    assert len(result) == 0, "no archived file for empty csv"


def test_csv_with_header(setup, mock_collection_datafile, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")

    test_csv.open("w").write("id,author,title\n")

    result = archive_collection(collection=collection)

    assert len(result) == 0, "no archived file for csv with no data"


# mock_now not used here since we want to call at two different times
def test_identical_csv(setup, mock_collection_datafile):
    provider = Provider("aub")
    collection = Collection(provider, "aco")

    fh = test_csv.open("w")
    fh.write("id,author,title\n")
    fh.write("1,Jalāl al-Dīn Muḥammad Rūmī,Maṭnawīye Ma'nawī\n")
    fh.close()

    result = archive_collection(collection=collection)
    assert len(result) != 0 and result["csv"].endswith(
        ".csv"
    ), "first archive is created"

    result = archive_collection(collection=collection)
    assert len(result) == 0, "identical archive not created"

    fh = test_csv.open("w")
    fh.write("id,author,title\n")
    fh.write("1,Jalāl al-Dīn Muḥammad Rūmī,Maṭnawīye Ma'nawī\n")
    fh.write("2,Tawfiq al-Hakim,Usfur min Sharq\n")
    fh.close()

    result = archive_collection(collection=collection)
    assert len(result) != 0 and result["csv"].endswith(
        ".csv"
    ), "new data creates an archive"


def test_json_with_data(setup, mock_collection_datafile, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")

    fh = test_json.open("w")
    fh.write("""{"id": 1, "title": "Maṭnawīye Ma'nawī"}\n""")
    fh.close()

    result = archive_collection(collection=collection)

    assert len(result) != 0
    assert result["json"].endswith(
        "test-archive/aub/aco/data-20230313180631.json"
    ), "returned archived json filename"
    assert Path(result["json"]).is_file(), "archived file exists"
    assert test_json.is_file(), "original data file should still be there"


def test_empty_json(setup, mock_collection_datafile, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    test_json.touch()

    result = archive_collection(collection=collection)

    assert len(result) == 0, "no archived file for empty json"
