#!/usr/bin/python
import os
import time
import shutil

def copydir(**kwargs):
    src = "/tmp/output/{}".format(kwargs.get('provider'))
    dst = "/tmp/output/{}".format(time.time())
    shutil.copytree(src, dst)
