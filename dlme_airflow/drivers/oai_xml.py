import re
import logging
import intake
import pandas as pd

from lxml import etree
from sickle import Sickle


class OaiXmlSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "oai_xml"
    version = "0.0.1"
    partition_access = True

    def __init__(
        self, collection_url, metadata_prefix, set=None, dtype=None, metadata=None
    ):
        super(OaiXmlSource, self).__init__(metadata=metadata)
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

    def _get_tag(self, el):
        """Return the element name, after removing namespace and lowercasing."""
        return re.sub(r"{.+}", "", el.tag).lower()

    # TODO: Discuss if this output shoould be an array (line 63) or a string
    def _from_metadata(self, manifest: etree) -> dict:
        output = {}

        NS = {
            "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
            "mods": "http://www.loc.gov/mods/v3",
        }

        if self.metadata_prefix == "oai_dc":
            oai_block = manifest.xpath("//oai_dc:dc", namespaces=NS)[0]
        elif self.metadata_prefix == "mods" or self.metadata_prefix == "mods_no_ocr":
            oai_block = manifest.xpath("//mods:mods", namespaces=NS)[0]
        elif self.metadata_prefix == "marc21":
            ## TODO: Extract proper marc21 record            
            oai_block = manifest.xpath("//metadata/record", namespaces=NS)[0]
        else:
            raise Exception(f"Unknown metadata prefix {self.metadata_prefix}")

        for metadata in oai_block.getchildren():
            tag = self._get_tag(metadata)
            if tag in output:
                if isinstance(output[tag], str):
                    if metadata.text is not None:
                        output[tag] = [output[tag], metadata.text.strip()]
                    else:
                        output[tag] = [output[tag]]
                else:
                    output[tag].append(metadata.text.strip())
            elif metadata.text is not None:
                output[tag] = metadata.text.strip()
            # if the element has child elements add them too
            elif len(metadata) > 0:
                output.update(self._flatten_tree(metadata))

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

    def _flatten_tree(self, el, metadata=None, prefix=None):
        """This will flatten an element and its children into a dictionary using
        the element path as a dictionary key:

        <a>
          <b>x</b>
          <c>y</c>
          <d>
            <e>z</e>
          </d>
        </a>

        will be converted to:

        {
          "a_b": "x",
          "a_c": "y"
          "a_d_e": "z"
        }

        """
        if metadata is None:
            metadata = {}

        # allow parent elements to accumulate, e.g. parent_child1_child2
        key = self._get_tag(el)
        if prefix is not None:
            key = f"{prefix}_{key}"

        if len(el) == 0 and el.text:
            metadata[key] = el.text
        else:
            for child in el:
                # recursive call to add child elements using key as a prefix
                self._flatten_tree(child, metadata, key)

        return metadata

    def read(self):
        self._load_metadata()
        return pd.concat([self.read_partition(i) for i in range(self.npartitions)])
