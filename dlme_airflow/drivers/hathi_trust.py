import json
import intake
import requests
import csv
import pandas as pd

from pymarc import parse_xml_to_array


class HathiTrustSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "marc_xml"
    version = "0.0.1"
    partition_access = True

    def __init__(self, collection_url, object_path, marc_urls = [], dtype=None, metadata=None):
        super(HathiTrustSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.object_path = object_path
        self.record_ids = []

    def _open_collection(self):
        collection_result = requests.get(self.collection_url)

        # Ensure the request was successful
        if not collection_result.ok:
            raise Exception(f"Failed to fetch data from URL: {self.collection_url}. Status code: {collection_result.status_code}")

        # Get each line of the collection
        content = collection_result.content.decode('utf-8').splitlines()

        # Read the collection lines into columns
        reader = csv.DictReader(content, delimiter='\t')
        self.record_ids = [row[self.object_path] for row in reader if self.object_path in row]

    def _get_schema(self):
        self._open_collection()

        return intake.source.base.Schema(
            datashape=None,
            dtype=self.dtype,
            shape=None,
            npartitions=len(self.record_ids),
            extra_metadata={},
        )

    def _get_partition(self, i):
        marc_url = self.metadata.get("catalog_url").format(id=self.record_ids[i])
        record = parse_xml_to_array(marc_url)[0]
        data = json.loads(record.as_json())
        if 'fields' in data:
            metadata = self._metadata_from_marc_fields(data.get("fields"))
        else:
            return data

        del data["fields"]
        data.update(metadata)

        return pd.json_normalize(data)

    def _metadata_from_marc_fields(self, fields):
        metadata = {}
        for field in fields:
            marc_field = list(field.keys())[0]
            if isinstance(field[marc_field], str):
                metadata.setdefault(marc_field, []).append(field[marc_field])

            if isinstance(field[marc_field], dict):
                if 'subfields' in field[marc_field]:
                    metadata |= self._flatten_marc_subfields(marc_field, field[marc_field].get("subfields"))

        return metadata

    def _flatten_marc_subfields(self, marc_field, subfields):
        metadata = {}
        for subfield in subfields:
            subfield_marker = next(iter(subfield))
            value = subfield[subfield_marker]
            if value:
                key = f"{marc_field}_{subfield_marker}"
                metadata.setdefault(key, []).append(value)

        return metadata

    def read(self):
        self._load_metadata()
        return pd.concat([self.read_partition(i) for i in range(self.npartitions)])
