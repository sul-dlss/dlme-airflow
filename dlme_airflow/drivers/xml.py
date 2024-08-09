import intake
import logging
import requests
import pandas as pd

from lxml import etree
from lxml.html import document_fromstring
from lxml.html.clean import Cleaner

from typing import List, Dict


class XmlSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "xml"
    version = "0.0.2"
    partition_access = True

    def __init__(self, collection_url, dtype=None, metadata=None):
        super(XmlSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
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
                        field_doc = document_fromstring(el.text)
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
            raise Exception("Missing record_selector")

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
