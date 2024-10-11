import time
import logging
import intake
import requests
import jsonpath_ng
import pandas as pd
from typing import Any, Optional, Generator

container = "dataframe"
name = "iiif_json"
version = "0.0.2"
partition_access = True


class IiifJsonSource(intake.source.base.DataSource):
    def __init__(self, collection_url, dtype=None, metadata=None, wait=None):
        super(IiifJsonSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.dtype = dtype
        self.wait = wait
        self._manifest_urls = []
        self._path_expressions = {}
        self.record_count = 0
        self.record_limit = self.metadata.get("record_limit")

    def _open_collection(self):
        logging.info(f"getting collection {self.collection_url}")
        resp = self._get(self.collection_url)
        if resp.status_code == 200:
            collection_result = resp.json()
            if "manifests" in collection_result:  # IIIF v2
                manifests = collection_result["manifests"]
            elif "items" in collection_result:  # IIIF v3
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
                self._manifest_urls.append(url)
        else:
            logging.error(f"got {resp.status_code} when fetching {self.collection_url}")

    def _open_manifest(self, manifest_url: str) -> Optional[dict]:
        logging.info(f"getting manifest {manifest_url}")
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
        output: dict[str, Any] = {}
        for name, info in self.metadata.get("fields").items():
            expression = self._path_expressions.get(name)
            result = [match.value for match in expression.find(iiif_manifest)]
            if (
                len(result) < 1
            ):  # the JSONPath expression didn't find anything in the manifest
                if info.get("optional") is True:
                    logging.debug(
                        f"{iiif_manifest.get('@id')} missing optional field: '{name}'; searched path: '{expression}'"
                    )
                else:
                    logging.warning(
                        f"{iiif_manifest.get('@id')} missing required field: '{name}'; searched path: '{expression}'"
                    )
            else:
                if (
                    len(result) == 1
                ):  # the JSONPath expression found exactly one result in the manifest
                    output[name] = _stringify_and_strip_if_list(result[0])
                else:  # the JSONPath expression found exactly one result in the manifest
                    if name not in output:
                        output[name] = []

                    for data in result:
                        output[name].append(_stringify_and_strip_if_list(data))
        return output

    def _extract_manifest_metadata(
        self, iiif_manifest_metadata
    ) -> dict[str, list[str]]:
        output: dict[str, list[str]] = {}
        for row in iiif_manifest_metadata:
            name = (
                row.get("label")
                .replace(" ", "-")
                .lower()
                .replace("(", "")
                .replace(")", "")
                .replace("/", "")
            )
            # initialize or append to output[name] based on whether we've seen the label
            metadata_value = row.get("value")
            if not metadata_value:
                continue

            if isinstance(metadata_value[0], dict):
                metadata_value = metadata_value[0].get("@value")

            if name in output:
                output[name].append(metadata_value)
            else:
                output[name] = [metadata_value]

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
        if self.wait:
            logging.info(f"waiting {self.wait} seconds")
            time.sleep(self.wait)
        return requests.get(url)

    def read(self):
        self._load_metadata()
        df = pd.concat([self.read_partition(i) for i in range(self.npartitions)])
        if self.record_limit:
            return df.head(self.record_limit)
        else:
            return df


def _stringify_and_strip_if_list(possible_list) -> list[str]:
    if isinstance(possible_list, list):
        return [str(elt).strip() for elt in possible_list]
    else:
        return possible_list


def _flatten_list(lst: list) -> Generator:
    for item in lst:
        if type(item) == list:
            yield from _flatten_list(item)
        else:
            yield item
