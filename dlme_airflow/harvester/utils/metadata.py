#!/usr/bin/python
import filecmp
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


def compare_metadata(**kwargs):
    provider = kwargs.get("provider")
    home_directory = os.environ['AIRFLOW_HOME']
    metadata_directory = f"{home_directory}/metadata/{provider}"
    working_directory = f"{home_directory}/working/{provider}"

    return are_dir_trees_equal(metadata_directory, working_directory)


# Copied from: https://stackoverflow.com/a/6681395
def are_dir_trees_equal(metadata_directory, working_directory):
    """
    Compare two directories recursively. Files in each directory are
    assumed to be equal if their names and contents are equal.

    @param metadata_directory: Source metadata path
    @param working_directory: Working metdata path

    @return: 'equal' if the directory trees are the same and
        there were no errors while accessing the directories or files,
        'not_equal' otherwise.
   """
    dirs_cmp = filecmp.dircmp(metadata_directory, working_directory)

    if len(dirs_cmp.left_only) > 0 or len(dirs_cmp.right_only) > 0 or len(dirs_cmp.funny_files) > 0:
        return 'not_equal'
    (_, mismatch, errors) = filecmp.cmpfiles(metadata_directory, working_directory, dirs_cmp.common_files, shallow=False)
    if len(mismatch) > 0 or len(errors) > 0:
        return 'not_equal'
    for common_dir in dirs_cmp.common_dirs:
        new_dir1 = os.path.join(metadata_directory, common_dir)
        new_dir2 = os.path.join(working_directory, common_dir)
        return are_dir_trees_equal(new_dir1, new_dir2)

    return 'equal'
