#!/usr/bin/python
import os
from sickle import Sickle
from sickle.iterator import OAIResponseIterator

from config import Settings

def Harvest():
    providers = Settings('oai')
    for key, params in providers.items():
        print(f"Starting [{key}]...\n")
        sickle = Sickle(params['url'])
        for set in params['sets']:
          directory = f"working/{key}/{set}/data/"
          os.makedirs(os.path.dirname(directory), exist_ok=True)

          records = sickle.ListRecords(metadataPrefix=params['metadata_prefix'], set=set, ignore_deleted=True)
          for counter, record in enumerate(records, start=1):
              with open(f'{directory}{set}-{counter}.xml', 'w') as f:
                 f.write(record.raw)

        print(f"Finished [{key}]...\n")
