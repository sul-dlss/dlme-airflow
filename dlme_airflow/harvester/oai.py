#!/usr/bin/python
import os
from sickle import Sickle
from sickle.iterator import OAIResponseIterator

from config import Settings
from airflow.operators.python import PythonOperator

def oai_harvester(provider: str) -> PythonOperator:
    return PythonOperator(
        task_id=f"oai_harvest_{provider}",
        python_callable=harvest,
        op_kwargs={"provider": provider}
    )

def harvest(**kwargs):
    provider_id = kwargs.get("provider")
    provider = Settings('oai')[provider_id]
    print(f"Starting [{provider_id}]...\n")

    sickle = Sickle(provider['url'])
    for set in provider['sets']:
        print(f"Starting [{provider_id}:{set}]...\n")
        directory = f"{os.environ['AIRFLOW_HOME']}/working/{provider_id}/{set}/data/"
        os.makedirs(os.path.dirname(directory), exist_ok=True)

        records = sickle.ListRecords(metadataPrefix=provider['metadata_prefix'], set=set, ignore_deleted=True)
        for counter, record in enumerate(records, start=1):
            with open(f'{directory}{set}-{counter}.xml', 'w') as f:
                f.write(record.raw)

    print(f"Finished [{provider_id}:{set}]...\n")
