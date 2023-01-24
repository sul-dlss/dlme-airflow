from dlme_airflow.utils.yale import remove_babylonian_non_relevant
from dlme_airflow.models.provider import Provider


def test_remove_babylonian_non_relevant():
    provider = Provider("yale")
    params = {"collection": provider.get_collection("babylonian")}

    assert "working/yale/babylonian/data.csv" in remove_babylonian_non_relevant(
        **params
    )
