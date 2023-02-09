from dlme_airflow.utils.yale import get_working_csv, remove_babylonian_non_relevant
from dlme_airflow.models.provider import Provider


def test_remove_babylonian_non_relevant(mocker):
    mocker.patch(
        "dlme_airflow.utils.yale.get_working_csv",
        return_value="tests/data/csv/yale.csv",
    )
    provider = Provider("yale")
    params = {"collection": provider.get_collection("babylonian")}

    assert "tests/data/csv/yale.csv" in remove_babylonian_non_relevant(**params)


def test_get_working_csv():
    assert "working/test_collection/data.csv" in get_working_csv("test_collection")
