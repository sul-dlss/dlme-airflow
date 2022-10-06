import logging
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def _fetch_transform_output(collection) -> str:
    data_path = collection.data_path().replace(
        "/", "-"
    )  # penn/egyptian-museum => penn-egyptian-museum
    transform_output_url = f"https://s3-us-west-2.amazonaws.com/dlme-metadata-dev/output/output-{data_path}.ndjson"
    output_file_path = (
        f"/tmp/output-{collection.provider.name}-{collection.name}.ndjson"
    )
    r = requests.get(transform_output_url, allow_redirects=True)
    open(output_file_path, "wb").write(r.content)
    return output_file_path


def _get_transformed_record_count(collection) -> int:
    output_file_path = _fetch_transform_output(collection)
    transformed_record_count = 0
    with open(output_file_path, "r") as file:
        records = file.readlines()
        transformed_record_count = len(records)

    # TODO: row_count is halved here to work around a current bug in the dlme-transform code.
    # get rid of halving once https://github.com/sul-dlss/dlme-transform/issues/931 is resolved.
    return int(transformed_record_count / 2)


def validate_transformation(task_instance, **kwargs):
    collection = kwargs.get("collection")
    dataframe_stats = task_instance.xcom_pull(
        task_ids=kwargs.get("harvest_task_id"), key="dataframe_stats"
    )

    df_record_count = dataframe_stats["record_count"]
    transformed_record_count = _get_transformed_record_count(collection)
    if df_record_count != transformed_record_count:
        raise Exception(
            f"ERROR: failed to transform all harvested records: harvested record count ({df_record_count}) != transformed record count ({transformed_record_count})"  # noqa: E501
        )
    else:
        logging.info(
            f"OK: dataframe harvested record count == transform output record count ({transformed_record_count})"
        )


def build_transform_validation_task(
    collection, task_group: TaskGroup, dag: DAG, harvest_task_id: str
):
    return PythonOperator(
        task_id=f"{collection.label()}_transform_validation",
        task_group=task_group,
        dag=dag,
        python_callable=validate_transformation,
        op_kwargs={"collection": collection, "harvest_task_id": harvest_task_id},
    )
