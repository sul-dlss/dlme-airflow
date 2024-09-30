import logging
import intake
import requests
import jsonpath_ng
import pandas as pd
from typing import Any, Optional, Generator
from dlme_airflow.utils.partition_url_builder import PartitionBuilder


class IiifV3JsonSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "iiif_json_v3"
    version = "0.0.2"
    partition_access = True

    def __init__(
        self,
        collection_url,
        paging=None,
        metadata=None
    ):
        super(IiifV3JsonSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.paging = paging
        self._manifest_urls = []
        self._path_expressions = {}
        self.record_count = 0
        self.record_limit = self.metadata.get("record_limit")


    def _open_collection(self):
        self._manifest_urls = self._get_manifest_urls()

    def _get_manifest_urls(self):
        if self.paging:
            return PartitionBuilder(self.collection_url, self.paging).urls()
        else:
            return self._get_manifest_urls_from_items()


    def _get_manifest_urls_from_items(self):
        resp = self._get(self.collection_url)
        collection_result = resp.json()
        urls = []
        if "items" in collection_result:  # IIIF v3
            manifests = collection_result["items"]
        else:
            raise Exception(
                f"Unknown collection manifest format: {self.collection_url}"
            )

        for manifest in manifests:
            if "@id" in manifest:
                url = manifest["@id"]  # valid in IIIF v2 or v3
            elif "id" in manifest:
                url = manifest["id"]  # valid in IIIF v3 only
            else:
                raise Exception(f"Unknown URL in manifest: {manifest}")
            urls.append(url)
        
        return urls


    def _open_manifest(self, manifest_url: str) -> Optional[dict]:
        resp = self._get(manifest_url)
        if resp.status_code == 200:
            manifest_result = resp.json()
        else:
            logging.error(
                f"got {resp.status_code} when fetching manifest {manifest_url}"
            )
            return None

        record = self._extract_specified_fields(manifest_result)

        # Handles metadata in IIIF manifest
        record.update(
            self._extract_manifest_metadata(manifest_result.get("metadata", []))
        )
        return record

    def _extract_specified_fields(self, iiif_manifest: dict) -> dict:
        output: dict [str, Any] = {}
        for name, info in self.metadata.get("fields").items():
            result = self._get_data_for_field(name, iiif_manifest)

            if not result:
                self._optional_field_warning(iiif_manifest.get("id"), name, self._path_expressions.get(name), info.get("optional"))
                continue

            processed_result = _stringify_and_strip_if_list(result)

            if name in output:
                output.update({name: _flatten_list([output[name], processed_result])})
            else:
                output[name] = processed_result

        return output


    def _get_data_for_field(self, field, manifest):
        expression = self._path_expressions.get(field)
        return [match.value for match in expression.find(manifest)]

    def _optional_field_warning(self, id, field, expression, optional):
        if optional is True:
            logging.debug(f"{id} missing optional field: '{field}'; searched path: '{expression}'")
            return
        
        logging.warning(f"{id} missing required field: '{field}'; searched path: '{expression}'")


    def _extract_manifest_metadata(
        self, iiif_manifest_metadata
    ) -> dict[str, list[str]]:
        output: dict[str, list[str]] = {}

        for row in iiif_manifest_metadata:
            for key in row.get("label").keys():
                name = (
                    row.get("label")[key][0]
                    .replace(" ", "-")
                    .lower()
                    .replace("(", "")
                    .replace(")", "")
                )
                # initialize or append to output[name] based on whether we've seen the label
                metadata_value = row.get("value")[key]
                if not metadata_value:
                    continue

                if name in output:
                    output.update({name: _flatten_list([output[name], metadata_value])})
                else:
                    output[name] = metadata_value

        # flatten any nested lists into a single list
        return {k: list(_flatten_list(v)) for (k, v) in output.items()}

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

        if result is not None and any(result.values()):
            self.record_count += 1
            return pd.DataFrame([result])
        else:
            logging.warning(f"{self._manifest_urls[i]} resulted in empty DataFrame")
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

    def _get(self, url):
        return requests.get(url)

    def read(self):
        self._load_metadata()
        df = pd.concat([self.read_partition(i) for i in range(self.npartitions)])
        if self.record_limit:
            return df.head(self.record_limit)
        else:
            return df


def _stringify_and_strip_if_list(record) -> list[str]:
    if isinstance(record, str):
        return str(record).strip()

    result_list = []
    for data in record:
        result_list.append(_stringify_and_strip_if_list(data))
    
    if len(result_list) == 1:
        return result_list[0]
    
    return result_list


def _flatten_list(lst: list) -> Generator:
    for item in lst:
        if type(item) is list:
            yield from _flatten_list(item)
        else:
            yield item
