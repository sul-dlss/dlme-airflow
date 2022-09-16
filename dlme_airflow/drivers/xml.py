import intake
import logging
import pandas as pd
import requests
from lxml import etree
from lxml.html import document_fromstring
from lxml.html.clean import Cleaner


class XmlSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "xml"
    version = "0.0.1"
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
        elements = xtree.findall(
            self._record_selector["path"], namespaces=self._record_selector["namespace"]
        )
        for counter, element in enumerate(elements, start=1):
            record = self._construct_fields(element)
            self._records.append(record)

    def _construct_fields(self, manifest: etree) -> dict:
        output = {}
        for field in self._path_expressions:
            path = self._path_expressions[field]["path"]
            namespace = self._path_expressions[field].get("namespace", {})
            optional = self._path_expressions[field].get("optional", False)
            result = manifest.xpath(path, namespaces=namespace)
            if len(result) < 1:
                if optional is True:
                    # Skip and continue
                    continue
                else:
                    logging.warn(f"Manifest missing {field}")
            else:
                try:
                    field_doc = document_fromstring(result[0].text)
                    cleaner = Cleaner(remove_unknown_tags=False, page_structure=True)
                    output[field] = self.sanitize_value(
                        cleaner.clean_html(field_doc).text_content()
                    )  # Use first value
                except AttributeError:
                    output[field] = self.sanitize_value(
                        result[0]
                    )  # if getting text fails, we may be pulling an attribute.
        return output

    def sanitize_value(self, value):
        return value.strip().replace("\n", " ").replace("\r", "")

    def _get_partition(self, i) -> pd.DataFrame:
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
            npartitions=len(self._records),
            extra_metadata={},
        )

    def read(self):
        self._load_metadata()
        return pd.concat([self.read_partition(i) for i in range(self.npartitions)])
