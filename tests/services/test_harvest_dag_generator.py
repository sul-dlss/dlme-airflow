import pytest

from airflow import models

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
    assert len(harvest_dags()) > 0
