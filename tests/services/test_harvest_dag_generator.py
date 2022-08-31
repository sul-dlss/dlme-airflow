import pytest
import inspect

from airflow import DAG, models
from airflow.utils.dag_cycle_tester import check_cycle


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key):
        if key == "data_manager_email":
            return "datamanager@institution.edu"

    monkeypatch.setattr(models.Variable, "get", mock_get)


def test_create_provider_dags(mock_variable):
    # import here after our mock has had a chance to take effect
    from dags import harvest_catalog

    for name, member in inspect.getmembers(harvest_catalog):
        if isinstance(member, DAG):
            check_cycle(member)
