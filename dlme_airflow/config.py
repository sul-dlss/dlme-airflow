#!/usr/bin/python
import yaml


def Settings(key):
    print(f"Getting {key} from settings.yml")
    with open("/opt/dlme_airflow/config/settings.yml", "r") as ymlfile:
        return yaml.load(ymlfile)[key]
