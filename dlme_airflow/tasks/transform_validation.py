import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dlme_airflow.models.collection import Collection
from dlme_airflow.utils.dataframe import dataframe_from_file


def build_transform_validation_task(
    collection: Collection, task_group: TaskGroup, dag: DAG
) -> PythonOperator:
    return PythonOperator(
        task_id=f"{collection.label()}_transform_validation",
        task_group=task_group,
        dag=dag,
        python_callable=validate_transformation,
        op_kwargs={"collection": collection},
    )


def validate_transformation(collection: Collection) -> None:
    df_record_count = get_record_count(collection)

    if collection.catalog.metadata.get("record_count_formula"):
        df_record_count = eval_record_count_formula(
            df_record_count,
            collection.catalog.metadata.get("record_count_formula"),
        )

    transformed_record_count = get_transformed_record_count(collection)
    if df_record_count != transformed_record_count:
        raise Exception(
            f"ERROR: failed to transform all harvested records: harvested record count ({df_record_count}) != transformed record count ({transformed_record_count})"  # noqa: E501
        )
    else:
        logging.info(
            f"OK: dataframe harvested record count == transform output record count ({transformed_record_count})"
        )


def eval_record_count_formula(
    harvested_record_count: int, record_count_formula: str
) -> int:
    try:
        return eval(f"{harvested_record_count}{record_count_formula}")
    except ZeroDivisionError:
        return 0


def get_record_count(collection: Collection) -> int:
    return len(
        dataframe_from_file(
            collection, collection.catalog.metadata.get("output_format")
        )
    )


def get_transformed_record_count(collection: Collection) -> int:
    output_file_path = get_transformed_path(collection)
    count = 0
    for line in open(output_file_path, "r"):
        count += 1
    return count


def get_transformed_path(collection) -> str:
    transform_file = collection.intermediate_representation_location()
    transform_dir = os.path.join("metadata", transform_file)
    return os.path.abspath(transform_dir)
