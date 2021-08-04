#!/usr/bin/python
import os
import time
import shutil

def copydir(**kwargs):
    src = "/opt/airflow/output/{}".format(kwargs.get('provider'))
    dst = "/opt/airflow/output/{}".format(time.time())
    shutil.copytree(src, dst)
