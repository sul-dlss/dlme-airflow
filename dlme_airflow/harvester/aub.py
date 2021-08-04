#!/usr/bin/python
import os
import logging
from sickle import Sickle


def harvest():
    sickle = Sickle("https://libraries.aub.edu.lb/xtf/oai")
    sets = ["aco", "aladab", "postcards", "posters", "travelbooks"]
    logging.info("**** BEGIN AUB HARVEST ****")
    for s in sets:
        directory = "/tmp/output/aub/{}/data/".format(s)
        os.makedirs(os.path.dirname(directory), exist_ok=True)

        logging.info("Start {} collection harvest.".format(s))
        records = sickle.ListRecords(
            metadataPrefix="oai_dc", set=s, ignore_deleted=True
        )
        for counter, record in enumerate(records, start=1):
            with open("{}{}-{}.xml".format(directory, s, counter), "w") as f:
                f.write(record.raw)
        logging.info("End {} collection harvest.".format(s))

    logging.info("**** END AUB HARVEST ****")
