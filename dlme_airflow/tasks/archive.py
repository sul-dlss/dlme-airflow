import shutil
import hashlib
import logging

from typing import Union
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dlme_airflow.models.collection import Collection
from dlme_airflow.utils.dataframe import datafile_for_collection


def archive_collection(collection: Collection) -> Union[str, None]:
    """Pass in a Collection and get back the path for an archive that was created"""
    src = Path(datafile_for_collection(collection))

    # don't clutter up the archive with empty data files, which can happen
    # with incremental harvests of oai-pmh endpoints
    if not has_data(src):
        logging.info("skipping archive for %s since it has no data")
        return None

    # don't bother archiving identical data that is already archived
    dest = archive_path(collection)
    if not has_new_data(src, dest):
        logging.info("skipping archive for %s since it has no new data")
        return None

    if not dest.parent.is_dir():
        dest.parent.mkdir(parents=True)

    logging.info("archiving %s to %s", src, dest)
    shutil.copy(src, dest)

    return str(dest)


def archive_task(
    collection: Collection, task_group: TaskGroup, dag: DAG
) -> PythonOperator:
    return PythonOperator(
        task_id=f"archive_{collection.label()}",
        task_group=task_group,
        dag=dag,
        python_callable=archive_collection,
        op_kwargs={
            "collection": collection,
        },
    )


def now() -> datetime:
    # this is here so it can be mocked easily in the test
    return datetime.now()


def archive_path(collection: Collection) -> Path:
    """Pass in a Collection and get back a timestamped archive path."""
    archive_dir = Path(collection.archive_dir())
    # ext is here in case we ever have .json data files
    ext = Path(datafile_for_collection(collection)).suffix
    return archive_dir / ("data-" + now().strftime("%Y%m%d%H%M%S") + ext)


def has_data(datafile_path: Path) -> bool:
    """Returns true if the data file has data, and not just a column headers."""
    line_count = short_count(datafile_path)
    if datafile_path.suffix == ".csv" and line_count == 2:
        return True
    elif datafile_path.suffix == ".json" and line_count > 0:
        return True
    else:
        return False


def short_count(path: Path) -> int:
    """Returns the number of lines in the file, but if it's more than 2 just return 2"""
    count = 0
    for _ in path.open():
        count += 1
        if count == 2:
            break
    return count


def has_new_data(src: Path, dest: Path) -> bool:
    prev = previous_archive(dest)
    if prev is None:
        return True

    src_digest = digest(src)
    prev_digest = digest(prev)

    return src_digest != prev_digest


def digest(path: Path) -> str:
    sha256 = hashlib.sha256()
    fh = path.open("rb")
    while True:
        buff = fh.read(1024)
        if not buff:
            break
        sha256.update(buff)
    return sha256.hexdigest()


def previous_archive(path: Path) -> Union[Path, None]:
    # if there is no archive dir then there is no previous archive file
    if not path.parent.is_dir():
        return None

    # sort the filenames in order of their datetime
    archive_files = sorted(path.parent.iterdir())
    if len(archive_files) > 0:
        return archive_files[-1]
    else:
        return None
