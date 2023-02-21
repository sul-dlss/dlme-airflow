from dlme_airflow.utils.catalog import get_working_csv

def test_get_working_csv():
    assert "metadata/test_collection/data.csv" in get_working_csv("test_collection")
