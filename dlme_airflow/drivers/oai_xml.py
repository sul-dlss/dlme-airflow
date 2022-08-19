import logging
import intake
import pandas as pd
from sickle import Sickle
from lxml import etree


class OAIXmlSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "oai_xml"
    version = "0.0.1"
    partition_access = True

    def __init__(
        self, collection_url, metadata_prefix, set=None, dtype=None, metadata=None
    ):
        super(OAIXmlSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.metadata_prefix = metadata_prefix
        self.set = set
        self._collection = Sickle(self.collection_url)
        self._path_expressions = self._get_path_expressions()
        self._records = []

    def _open_set(self):
        oai_records = self._collection.ListRecords(
            set=self.set, metadataPrefix=self.metadata_prefix, ignore_deleted=True
        )

        for counter, oai_record in enumerate(oai_records, start=1):
            xtree = etree.fromstring(oai_record.raw)
            if counter % 100 == 0:
                logging.info(counter)
            record = self._construct_fields(xtree)
            record.update(self._from_metadata(xtree))
            self._records.append(record)

    def _construct_fields(self, manifest: etree) -> dict:
        output = {}
        for field in self._path_expressions:
            path = self._path_expressions[field]["path"]
            namespace = self._path_expressions[field]["namespace"]
            optional = self._path_expressions[field]["optional"]
            result = manifest.xpath(path, namespaces=namespace)
            if len(result) < 1:
                if optional is True:
                    # Skip and continue
                    continue
                else:
                    logging.warn(f"Manifest missing {field}")
            else:
                if len(result) == 1:
                    output[field] = result[0].text.strip()
                else:
                    if field not in output:
                        output[field] = []

                    for data in result:
                        output[field].append(data.text.strip())
        return output

    def uri2label(self, value: str, nsmap: dict):
        for key in list(nsmap.keys()):
            if nsmap[key] in value:
                return value.replace(nsmap[key], "").replace("{}", "")

    # TODO: Discuss if this output shoould be an array (line 63) or a string
    def _from_metadata(self, manifest: etree) -> dict:
        output = {}
        NS = {"oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/"}
        oai_block = manifest.xpath("//oai_dc:dc", namespaces=NS)[
            0
        ]  # we want the first result
        for metadata in oai_block.getchildren():
            tag = self.uri2label(metadata.tag, metadata.nsmap)
            if tag in output:
                if isinstance(output[tag], str):
                    if metadata.text is not None:
                        output[tag] = [output[tag], metadata.text.strip()]
                    else:
                        output[tag] = [output[tag]]
                else:
                    output[tag].append(metadata.text.strip())
            else:
                output[tag] = metadata.text.strip()

        return output

    def _get_partition(self, i) -> pd.DataFrame:
        return pd.DataFrame(self._records)

    def _get_path_expressions(self):
        paths = {}
        for name, info in self.metadata.get("fields", {}).items():
            paths[name] = info

        return paths

    # TODO: Ask/Investigate (with jnelson) what the purpose of dtyle=self.dtype
    def _get_schema(self):
        self._open_set()

        return intake.source.base.Schema(
            datashape=None,
            dtype=self.dtype,
            shape=(None, 1),
            npartitions=1,
            extra_metadata={},
        )

    def read(self):
        self._load_metadata()
        return pd.concat([self.read_partition(i) for i in range(self.npartitions)])
