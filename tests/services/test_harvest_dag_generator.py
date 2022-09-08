import pytest

from airflow import models
from airflow.utils.dag_cycle_tester import check_cycle

from dlme_airflow.services.harvest_dag_generator import (
    create_provider_dags,
    harvest_dags,
)
from dlme_airflow.drivers import register_drivers
from dlme_airflow.utils.catalog import fetch_catalog


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key):
        if key == "data_manager_email":
            return "datamanager@institution.edu"

    monkeypatch.setattr(models.Variable, "get", mock_get)


def test_create_provider_dags(mock_variable):
    register_drivers()
    create_provider_dags()
    assert list(harvest_dags().keys()) == list(fetch_catalog())
    for provider in iter(list(fetch_catalog())):
        dag = harvest_dags()[provider]
        check_cycle(dag)
