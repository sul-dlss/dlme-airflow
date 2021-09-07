import logging
import intake
import requests
import jsonpath_ng
import pandas as pd


class IIIfJsonSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "iiif_json"
    version = "0.0.2"
    partition_access = True

    def __init__(self, collection_url, dtype=None, metadata=None):
        super(IIIfJsonSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.dtype = dtype
        self._manifest_urls = []
        self._path_expressions = {}

    def _open_collection(self):
        collection_result = requests.get(self.collection_url)
        for manifest in collection_result.json().get("manifests", []):
            self._manifest_urls.append(manifest.get("@id"))

    def _open_manifest(self, manifest_url: str):
        manifest_result = requests.get(manifest_url)
        manifest_detail = manifest_result.json()
        record = self._construct_fields(manifest_detail)
        # Handles metadata in IIIf manfest
        record.update(self._from_metadata(manifest_detail.get("metadata", [])))
        return record

    def _construct_fields(self, manifest: dict) -> dict:
        output = {}
        for name, info in self.metadata.get("fields").items():
            expression = self._path_expressions.get(name)
            result = [match.value for match in expression.find(manifest)]
            if len(result) < 1:
                if info.get("optional") is True:
                    # Skip and continue
                    continue
                else:
                    logging.warn(f"{manifest.get('@id')} missing {name}")
            else:
                output[name] = result[0]  # Use first value
        return output

    def _from_metadata(self, metadata) -> dict:
        output = {}
        for row in metadata:
            name = (
                row.get("label")
                .replace(" ", "-")
                .lower()
                .replace("(", "")
                .replace(")", "")
            )
            output[name] = row.get("value")  # this will assign the last value found to output[name]
            # if name in output:
            #     output[name].append(row.get("value"))
            # else:
            #     output[name] = [row.get("value")]
        return output

    def _get_partition(self, i) -> pd.DataFrame:
        result = self._open_manifest(self._manifest_urls[i])
        return pd.DataFrame(
            [
                result,
            ]
        )

    def _get_schema(self):
        for name, info in self.metadata.get("fields", {}).items():
            self._path_expressions[name] = jsonpath_ng.parse(info.get("path"))
        self._open_collection()
        return intake.source.base.Schema(
            datashape=None,
            dtype=self.dtype,
            shape=None,
            npartitions=len(self._manifest_urls),
            extra_metadata={},
        )

    def read(self):
        self._load_metadata()
        return pd.concat([self.read_partition(i) for i in range(self.npartitions)])
