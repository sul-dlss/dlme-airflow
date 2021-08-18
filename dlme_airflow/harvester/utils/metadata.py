#!/usr/bin/python
import os
import logging
import time
import shutil

def copydir(**kwargs):
    src = "/opt/airflow/output/{}".format(kwargs.get("provider"))
    dst = "/opt/airflow/output/{}".format(time.time())
    shutil.copytree(src, dst)

def validate_metadata_folder(**kwargs):
    metadata_directory = os.environ['AIRFLOW_HOME']+"/metadata/"
    logging.info(f"validate_metadata_folder: {metadata_directory}")

    if not os.path.exists(metadata_directory):
        os.makedirs(metadata_directory)
    if len(os.listdir(metadata_directory)) == 0:
        return 'clone_metadata'
    return 'pull_metadata'