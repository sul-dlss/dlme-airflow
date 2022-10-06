import json
import logging
import pytest

from dlme_airflow.models.collection import Collection
from dlme_airflow.models.provider import Provider
from dlme_airflow.tasks.transform_validation import validate_transformation


class MockTaskInstance:
    def __init__(self, upstream_task_id, row_count):
        self.upstream_task_id = upstream_task_id
        self.row_count = row_count

    def xcom_pull(self, task_ids: str, key: str):
        if task_ids == self.upstream_task_id and key == "dataframe_stats":
            return {"record_count": self.row_count}
        else:
            return None


def mock_ndjson_response_text(row_count) -> str:
    # TODO: row_count is doubled here to simulate a current bug in the dlme-transform code.
    # get rid of doubling once https://github.com/sul-dlss/dlme-transform/issues/931 is resolved.
    json_rows = [json.dumps({"row_num": i}) for i in range(row_count * 2)]
    return "\n".join(json_rows)


def setup_mock_ndjson_response(collection, ndjson_row_count, requests_mock):
    data_path = collection.data_path().replace(
        "/", "-"
    )  # penn/egyptian-museum => penn-egyptian-museum

    requests_mock.get(
        f"https://s3-us-west-2.amazonaws.com/dlme-metadata-dev/output/output-{data_path}.ndjson",
        text=mock_ndjson_response_text(ndjson_row_count),
    )


def test_validation_passes(requests_mock, caplog):
    collection = Collection(Provider("aims"), "aims")
    harvest_task_id = "aims_aims_harvest"

    mock_task_instance = MockTaskInstance(harvest_task_id, 5)
    setup_mock_ndjson_response(collection, 5, requests_mock)

    with caplog.at_level(logging.INFO):
        validate_transformation(
            mock_task_instance, collection=collection, harvest_task_id=harvest_task_id
        )

    assert (
        "OK: dataframe harvested record count == transform output record count (5)"
        in caplog.text
    )


def test_validation_fails(requests_mock, caplog):
    collection = Collection(Provider("aims"), "aims")
    harvest_task_id = "aims_aims_harvest"

    mock_task_instance = MockTaskInstance(harvest_task_id, 5)
    setup_mock_ndjson_response(collection, 4, requests_mock)

    with pytest.raises(Exception) as excinfo:
        with caplog.at_level(logging.DEBUG):
            validate_transformation(
                mock_task_instance,
                collection=collection,
                harvest_task_id=harvest_task_id,
            )

    assert (
        "ERROR: failed to transform all harvested records: harvested record count (5) != transformed record count (4)"
        in str(excinfo.value)
    )
    assert (
        "OK: dataframe harvested record count == transform output record count"
        not in caplog.text
    )
