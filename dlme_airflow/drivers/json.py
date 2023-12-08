import time
import logging
import intake
import requests
import jsonpath_ng
import pandas as pd
from typing import Any, Optional, Generator


container = "dataframe"
name = "custom_json"
version = "0.0.1"
partition_access = True


class JsonSource(intake.source.base.DataSource):
    def __init__(
        self,
        collection_url,
        nextpage_path=None,
        increment=None,
        result_count=None,
        collection_selector=None,
        dtype=None,
        metadata=None,
        wait=None,
    ):
        super(JsonSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.nextpage_path = nextpage_path
        self.increment = increment
        self.result_count = result_count
        self.collection_selector = collection_selector
        self.dtype = dtype
        self.wait = wait
        self._page_urls = []
        self._path_expressions = {}
        self.record_count = 0
        self.record_limit = self.metadata.get("record_limit")

    def _open_collection(self):
        # either the API passes the next page or we increment with a url pattern
        if self.nextpage_path:
            self._page_urls.append(self.collection_url)
            logging.info(f"getting collection {self.collection_url}")
            resp = self._get(self.collection_url)
            if resp.status_code == 200:
                collection_result = resp.json()
                expression = jsonpath_ng.parse(self.nextpage_path)
                if [match.value for match in expression.find(collection_result)]:
                    next = [
                        match.value for match in expression.find(collection_result)
                    ][0]
                    while next:
                        self._page_urls.append(next)

                        resp = self._get(next)
                        if resp.status_code == 200:
                            result = resp.json()
                            next = [match.value for match in expression.find(result)][0]
                        else:
                            logging.error(
                                f"got {resp.status_code} when fetching {next}"
                            )
                else:
                    raise Exception(
                        f"Pagination path not found in: {self.collection_url}"
                    )
            else:
                logging.error(
                    f"got {resp.status_code} when fetching {self.collection_url}"
                )
        else:
            self._page_urls.append(self.collection_url)
            logging.info(f"getting collection {self.collection_url}")
            # pass a json path for the result count in the catalog or hard code the count
            if self.result_count:
                if self.result_count.get("path"):
                    resp = self._get(self.collection_url)
                    if resp.status_code == 200:
                        collection_result = resp.json()
                        expression = jsonpath_ng.parse(self.result_count.get("path"))

                        if [
                            match.value for match in expression.find(collection_result)
                        ]:
                            result_count = [
                                match.value
                                for match in expression.find(collection_result)
                            ][0]
                else:
                    try:
                        result_count = int(self.result_count.get("count"))
                    except result_count.does_not_exist:
                        logging.error(
                            "result_count needs to be set in the catalog with a json path or a hard coded number"
                        )
            url = self.collection_url
            if self.increment:
                start = int(self.increment.get("amount"))
                for i in range(
                    int(self.increment.get("amount")),
                    result_count,
                    int(self.increment.get("amount")),
                ):
                    url = url.replace(
                        f"{self.increment.get('variable_string')}{start}",
                        f"{self.increment.get('variable_string')}{start+int(self.increment.get('amount'))}",
                    )
                    start += int(self.increment.get("amount"))
                    self._page_urls.append(url)
            else:
                logging.error("increment key not found in catalog")

    def _open_page(self, page_url: str) -> Optional[list]:
        logging.info(f"getting page {page_url}")
        resp = self._get(page_url)
        if resp.status_code == 200:
            page_result = resp.json()
            expression = jsonpath_ng.parse(self.collection_selector)
            page_result = _flatten_list(
                [match.value for match in expression.find(page_result)]
            )
        else:
            logging.error(f"got {resp.status_code} when fetching manifest {page_url}")
            return None
        records = [self._extract_specified_fields(record) for record in page_result]
        for record in records:
            record.update(self._extract_record_metadata(record.get("metadata", [])))
        return records

    def _extract_specified_fields(self, json_page_result: dict) -> dict:
        output: dict[str, Any] = {}
        for name, info in self.metadata.get("fields").items():
            expression = self._path_expressions.get(name)
            result = [match.value for match in expression.find(json_page_result)]
            if (
                len(result) < 1
            ):  # the JSONPath expression didn't find anything in the manifest
                if info.get("optional") is True:
                    logging.debug(
                        f"{json_page_result} missing optional field: '{name}'; searched path: '{expression}'"
                    )
                else:
                    logging.warning(
                        f"{json_page_result} missing required field: '{name}'; searched path: '{expression}'"
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

    def _extract_record_metadata(self, record_metadata) -> dict[str, list[str]]:
        output: dict[str, list[str]] = {}
        for row in record_metadata:
            name = (
                row.get("label")
                .replace(" ", "-")
                .lower()
                .replace("(", "")
                .replace(")", "")
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

        result = self._open_page(self._page_urls[i])

        # If the dictionary has AT LEAST one value that is not None return a
        # DataFrame with the keys as columns, and the values as a row.
        # Otherwise return an empty DataFrame that can be concatenated.
        # This will prevent rows with all empty values from being generated
        # For context see https://github.com/sul-dlss/dlme-airflow/issues/192

        if result is not None and len(result) > 0:
            self.record_count = len(result)
            return pd.DataFrame(result)
        else:
            logging.warning(f"{self._page_urls[i]} resulted in empty DataFrame")
            return pd.DataFrame()

    def _get_schema(self):
        for name, info in self.metadata.get("fields", {}).items():
            self._path_expressions[name] = jsonpath_ng.parse(info.get("path"))
        self._open_collection()
        return intake.source.base.Schema(
            datashape=None,
            dtype=self.dtype,
            shape=None,
            npartitions=len(self._page_urls),
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
