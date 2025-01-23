import intake
import logging
import requests
import pandas as pd

from lxml import etree
from lxml.html import document_fromstring
from lxml.html.clean import Cleaner

from typing import List, Dict
from dlme_airflow.utils.partition_url_builder import PartitionBuilder


class MissingResumptionToken(Exception):
    def __init__(self, message):
        super().__init__(message)


class EndOfMaxResultSet(Exception):
    def __init__(self, message):
        super().__init__(message)


class XmlSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "xml"
    version = "0.0.2"
    partition_access = True

    def __init__(self, collection_url, paging={}, dtype=None, metadata=None):
        super(XmlSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.paging_config = paging
        self.paging_increment = paging.get("increment", 1)
        self.paging_start = paging.get("start", 0)
        self._record_selector = self._get_record_selector()
        self._path_expressions = self._get_path_expressions()
        self._records = []

    def _open_collection(self):
        collection_result = requests.get(self.collection_url).content
        xtree = etree.fromstring(collection_result)
        record_elements = xtree.findall(
            self._record_selector["path"], namespaces=self._record_selector["namespace"]
        )

        for record_el in record_elements:
            record = self._construct_fields(record_el)
            self._records.append(record)

    def _open_paged_collection(self):
        if self.paging_config.get("link_text"): # Indicates we're parsing links from HTML
            urls = PartitionBuilder(self.collection_url, self.paging_config).urls()
            record_elements = [self._fetch_provider_data(url) for url in urls]
            self._process_records(record_elements)
        else:
            for record_elements in self._fetch_collection():
                self._process_records(record_elements)

    def _fetch_provider_data(self, url):
        response = requests.get(url)
        return etree.fromstring(response.content)

    def _get_collection_url(self, offset, start=0):
        """Generate the collection URL with the current offset."""
        return self.collection_url.format(offset=offset, start=start)

    def _fetch_collection(self):
        """Fetch the content of the collection URL."""
        records = []
        try:
            while True:
                collection_url = self._get_collection_url(self.paging_increment, self.paging_start)
                response = requests.get(collection_url)
                xtree = etree.fromstring(bytes(response.text, encoding='utf-8'))
                records.append(self._get_record_elements(xtree))
                self._set_offset(xtree)
        except etree.XMLSyntaxError as e:
            # If the XML is malformed or empty, we stop fetching and return the records
            logging.info(f"XMLSyntaxError: {e}")
            return records
        except MissingResumptionToken as e:
            # If the XML is malformed or empty, we stop fetching and return the records
            logging.info(f"Missing resumption token: {e}")
            return records
        except EndOfMaxResultSet as e:
            # If the XML is malformed or empty, we stop fetching and return the records
            logging.info(f"{e}")
            return records
        except Exception as e:
            logging.info(f"Error: {e}")
        except ValueError as e:
            logging.info(f"ValueError: {e}")

    def _get_record_elements(self, xtree):
        """Find record elements in the XML tree."""
        return xtree.xpath(
            self._record_selector["path"], namespaces=self._record_selector["namespace"]
        )

    def _process_records(self, record_elements):
        """Process record elements and append to the records list."""
        for record_el in record_elements:
            record = self._construct_fields(record_el)
            self._records.append(record)

    def _set_offset(self, xtree):
        """
        Sets the offset based on the current increment or presence of a resumption token.
        """

        if "resumptionToken" in self.paging_config:
            self._extract_resumption_token(xtree)
            return

        if "pagination" in self.paging_config:
            self._extract_paging_metadata(xtree)
            return

        self.paging_increment += self.paging_config.get("increment", 0)

    def _extract_resumption_token(self, xtree):
        resumption_token = xtree.xpath(".//resumptionToken")
        if resumption_token:
            _, token = resumption_token[0].text.split("=")
            self.paging_increment = int(token)
        else:
            raise MissingResumptionToken(f"No resumption token found after {self.paging_increment}")

    def _extract_paging_metadata(self, xtree):
        # Define the namespace map for XPath
        ns_map = {
            'h': 'http://api.lib.harvard.edu/v2/item'
        }

        maxResults = int(xtree.xpath('/h:results/h:pagination/h:maxPageableSet', namespaces=ns_map)[0].text)
        numFound = int(xtree.xpath('/h:results/h:pagination/h:numFound', namespaces=ns_map)[0].text)
        next_start = self.paging_start + self.paging_increment
        if next_start > maxResults:
            raise EndOfMaxResultSet(f"Results ({numFound} exceed max result set ({maxResults}).")

        self.paging_start = next_start

    def _construct_fields(self, record_el: etree) -> dict:
        record: Dict[str, (str | List)] = {}
        for field in self._path_expressions:
            # look for the field in our data
            path = self._path_expressions[field]["path"]
            namespace = self._path_expressions[field].get("namespace", {})
            optional = self._path_expressions[field].get("optional", False)
            els = record_el.xpath(path, namespaces=namespace)

            if len(els) == 0:
                if optional is True:
                    continue
                else:
                    logging.warn(f"Record missing {field}")
            else:
                for el in els:
                    if hasattr(el, "text") and el.text is not None:
                        try:
                          field_doc = document_fromstring(el.text)
                        except etree.ParserError:
                            # Skip any fields that cannot be parsed
                            continue

                        cleaner = Cleaner(
                            remove_unknown_tags=False, page_structure=True
                        )
                        cleaned_el = cleaner.clean_html(field_doc)
                        value = self.sanitize_value(cleaned_el.text_content())
                    elif issubclass(type(el), str):
                        value = self.sanitize_value(el)
                    else:
                        # we likely have an empty element
                        continue

                    # a record with only value for a field will get a string
                    # but records with multiple values for a field get a list

                    if field in record and type(record[field]) is list:
                        record[field].append(value)  # type: ignore
                    elif field in record:
                        record[field] = [record[field], value]
                    else:
                        record[field] = value

        return record

    def sanitize_value(self, value) -> str:
        return value.strip().replace("\n", " ").replace("\r", "")

    def _get_partition(self, _) -> pd.DataFrame:
        return pd.DataFrame(self._records)

    def _get_record_selector(self):
        record_selector = self.metadata.get("record_selector")
        if not record_selector:
            return

        path = record_selector.get("path")
        if not path:
            raise Exception("Missing path")

        return {"path": path, "namespace": record_selector.get("namespace") or {}}

    def _get_path_expressions(self):
        paths = {}
        for name, info in self.metadata.get("fields", {}).items():
            paths[name] = info

        return paths

    def _get_schema(self):
        if self.paging_config:
            self._open_paged_collection()
        else:
            self._open_collection()

        return intake.source.base.Schema(
            datashape=None,
            dtype=self.dtype,
            shape=None,
            npartitions=1,
            extra_metadata={},
        )

    def read(self):
        self._load_metadata()
        return pd.concat([self.read_partition(i) for i in range(self.npartitions)])
