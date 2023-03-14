import pytest
import shutil
import datetime

from pathlib import Path
from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider
from dlme_airflow.tasks.archive import archive_collection

test_working = Path("test-working")
test_dir = test_working / "aub" / "aco"
test_csv = test_dir / "data.csv"
test_json = test_dir / "data.json"
test_now = datetime.datetime(2023, 3, 13, 18, 6, 31)


@pytest.fixture
def mock_csv(monkeypatch):
    def mock(_):
        return str(test_csv.absolute())

    monkeypatch.setattr("dlme_airflow.tasks.archive.datafile_for_collection", mock)


@pytest.fixture
def mock_json(monkeypatch):
    def mock(_):
        return str(test_json.absolute())

    monkeypatch.setattr("dlme_airflow.tasks.archive.datafile_for_collection", mock)


@pytest.fixture
def mock_now(monkeypatch):
    monkeypatch.setattr("dlme_airflow.tasks.archive.now", lambda: test_now)


@pytest.fixture
def setup_dir():
    if test_csv.parent.is_dir():
        shutil.rmtree(test_dir)
    test_csv.parent.mkdir(parents=True)


def test_csv_with_data(setup_dir, mock_csv, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")

    fh = test_csv.open("w")
    fh.write("id,author,title\n")
    fh.write("1,Jalāl al-Dīn Muḥammad Rūmī,Maṭnawīye Ma'nawī\n")
    fh.close()

    result = archive_collection(collection=collection)

    assert result is not None
    assert result.endswith(
        "test-working/aub/aco/archive/data-20230313180631.csv"
    ), "returned archived filename"
    assert Path(result).is_file(), "archived file exists"
    assert test_csv.is_file(), "original data file should still be there"


def test_empty_csv(setup_dir, mock_csv, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    test_csv.touch()

    result = archive_collection(collection=collection)

    assert result is None, "no archived file for empty csv"


def test_csv_with_header(setup_dir, mock_csv, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")

    test_csv.open("w").write("id,author,title\n")

    result = archive_collection(collection=collection)

    assert result is None, "no archived file for csv with no data"


# mock_now not used here since we want to call at two different times
def test_identical_csv(setup_dir, mock_csv):
    provider = Provider("aub")
    collection = Collection(provider, "aco")

    fh = test_csv.open("w")
    fh.write("id,author,title\n")
    fh.write("1,Jalāl al-Dīn Muḥammad Rūmī,Maṭnawīye Ma'nawī\n")
    fh.close()

    result = archive_collection(collection=collection)
    assert result is not None and result.endswith(".csv"), "first archive is created"

    result = archive_collection(collection=collection)
    assert result is None, "identical archive not created"

    fh = test_csv.open("w")
    fh.write("id,author,title\n")
    fh.write("1,Jalāl al-Dīn Muḥammad Rūmī,Maṭnawīye Ma'nawī\n")
    fh.write("2,Tawfiq al-Hakim,Usfur min Sharq\n")
    fh.close()

    result = archive_collection(collection=collection)
    assert result is not None and result.endswith(".csv"), "new data creates an archive"


def test_json_with_data(setup_dir, mock_json, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")

    fh = test_json.open("w")
    fh.write("""{"id": 1, "title": "Maṭnawīye Ma'nawī"}\n""")
    fh.close()

    result = archive_collection(collection=collection)

    assert result is not None
    assert result.endswith(
        "test-working/aub/aco/archive/data-20230313180631.json"
    ), "returned archived json filename"
    assert Path(result).is_file(), "archived file exists"
    assert test_json.is_file(), "original data file should still be there"


def test_empty_json(setup_dir, mock_json, mock_now):
    provider = Provider("aub")
    collection = Collection(provider, "aco")
    test_json.touch()

    result = archive_collection(collection=collection)

    assert result is None, "no archived file for empty json"
