import intake
import jsonpath_ng
import pandas as pd
from sickle import Sickle
from lxml import etree


class OAIXmlSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "oai_xml"
    version = "0.0.1"
    partition_access = True

    def __init__(self, collection_url, dtype=None, metadata=None):
        super(OAIXmlSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.dtype = dtype
        self._collection = Sickle(self.collection_url)
        self._records = []
        self._path_expressions = {}


    def _open_set(self):
        set = self.metadata.get("name")
        oai_records = self._collection.ListRecords(metadataPrefix='oai_dc', set=set, ignore_deleted=True) # requests.get(manifest_url)
        for oai_record in oai_records:
            xtree = etree.parse(oai_record)
            # xroot = xtree.getroot()
            record = self._contruct_fields(xtree)
            record.update(self._from_metadata(xtree))
            self._records.append(record)


    def _construct_fields(self, manifest: etree) -> dict:
        output = {}
        for name, info in self.metadata.get("fields").items():
            expression = self._path_expressions.get(name)
            # result = [match.value for match in expression.find(manifest)]
            # if len(result) < 1:
            #     if info.get("optional") is True:
            #         # Skip and continue
            #         continue
            #     else:
            #         logging.warn(f"{manifest.get('@id')} missing {name}")
            # else:
            #     output[name] = result[0]  # Use first value
        return output


    # TODO: Discuss if this output shoould be an array (line 63) or a string
    def _from_metadata(self, manifest: etree) -> dict:
        output = {}
        for node in manifest.xpath("//record/metadata/oai:dc"):
            output[node.tag] = node.text.strip()

        return output


    def _get_partition(self, i) -> pd.DataFrame:
        self._open_set()
        return pd.DataFrame(self._records)


    # TODO: Ask/Investigate (with jnelson) what the purpose of dtyle=self.dtype
    def _get_schema(self):
        for name, info in self.metadata.get("fields", {}).items():
            self._path_expressions[name] = jsonpath_ng.parse(info.get("path"))
        self._open_records()
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
