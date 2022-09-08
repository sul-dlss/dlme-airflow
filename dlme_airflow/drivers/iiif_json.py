import logging
import intake
import requests
import jsonpath_ng
import pandas as pd

container = "dataframe"
name = "iiif_json"
version = "0.0.2"
partition_access = True


class IiifJsonSource(intake.source.base.DataSource):
    def __init__(self, collection_url, dtype=None, metadata=None):
        super(IiifJsonSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.dtype = dtype
        self._manifest_urls = []
        self._path_expressions = {}
        self.record_count = 0
        self.record_limit = self.metadata.get("record_limit")

    def _open_collection(self):
        collection_result = requests.get(self.collection_url)
        for manifest in collection_result.json().get("manifests", []):
            self._manifest_urls.append(manifest.get("@id"))

    def _open_manifest(self, manifest_url: str):
        manifest_result = requests.get(manifest_url)
        manifest_detail = manifest_result.json()
        record = self._construct_fields(manifest_detail)
        # Handles metadata in IIIF manifest
        record.update(self._from_manifest_metadata(manifest_detail.get("metadata", [])))
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
                if len(result) == 1:
                    output[name] = result[0].strip()
                else:
                    if name not in output:
                        output[name] = []

                    for data in result:
                        output[name].append(data.strip())
        return output

    def _from_manifest_metadata(self, iiif_manifest_metadata) -> dict:
        output = {}
        for row in iiif_manifest_metadata:
            name = (
                row.get("label")
                .replace(" ", "-")
                .lower()
                .replace("(", "")
                .replace(")", "")
            )
            # initialize or append to output[name] based on whether we've seen the label
            if name in output:
                output[name].append(row.get("value"))
            else:
                output[name] = [row.get("value")]
        return output

    def _get_partition(self, i) -> pd.DataFrame:

        # if we are over the defined limit return an empty DataFrame right away
        if self.record_limit is not None and self.record_count > self.record_limit:
            return pd.DataFrame()

        result = self._open_manifest(self._manifest_urls[i])

        # If the dictionary has AT LEAST one value that is not None return a
        # DataFrame with the keys as columns, and the values as a row.
        # Otherwise return an empty DataFrame that can be concatenated.
        # This will prevent rows with all empty values from being generated
        # For context see https://github.com/sul-dlss/dlme-airflow/issues/192

        if any(result.values()):
            self.record_count += 1
            return pd.DataFrame([result])
        else:
            logging.warn(f"{self._manifest_urls[i]} resulted in empty DataFrame")
            return pd.DataFrame()

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
        df = pd.concat([self.read_partition(i) for i in range(self.npartitions)])
        if self.record_limit:
            return df.head(self.record_limit)
        else:
            return df
