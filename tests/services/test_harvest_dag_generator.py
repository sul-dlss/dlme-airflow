import pytest

from airflow import models
from airflow.utils.dag_cycle_tester import check_cycle

from dlme_airflow.services.harvest_dag_generator import (
    create_provider_dags,
    harvest_dags,
)


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key):
        if key == "data_manager_email":
            return "datamanager@institution.edu"

    monkeypatch.setattr(models.Variable, "get", mock_get)


def test_create_provider_dags(mock_variable):
    create_provider_dags()
    for dag in harvest_dags().values():
        check_cycle(dag)
