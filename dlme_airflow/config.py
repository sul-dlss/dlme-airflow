#!/usr/bin/python
import yaml

# settings = {}

def Settings(key):
    # settings_cache = settings
    # if not settings_cache:
    with open("/opt/dlme_airflow/config/settings.yml", "r") as ymlfile:
        return yaml.load(ymlfile)[key]

    # return settings[key]
