import time
import logging
import intake
import pandas as pd

from lxml import etree
from sickle import Sickle
from sickle.iterator import OAIItemIterator
from sickle.oaiexceptions import BadResumptionToken
from typing import Dict

# xml namespaces and the prefixes that are used in parsing

NS = {
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "mods": "http://www.loc.gov/mods/v3",
    "marc21": "http://www.loc.gov/MARC21/slim",
}


class OaiXmlSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "oai_xml"
    version = "0.0.1"
    partition_access = True

    def __init__(
        self,
        collection_url,
        metadata_prefix,
        set=None,
        wait=0,
        allow_expiration=False,
        dtype=None,
        metadata=None,
    ):
        super(OaiXmlSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.metadata_prefix = metadata_prefix
        self.record_limit = self.metadata.get("record_limit", None)
        self.identifier = self.metadata.get("identifier", None)
        self.record_count = 0
        self.set = set
        self.wait = wait
        self.allow_expiration = allow_expiration
        self._collection = Sickle(
            self.collection_url, iterator=make_iterator(self.wait)
        )
        self._path_expressions = self._get_path_expressions()
        self._records = []

    def _open_set(self):
        if self.identifier:
            oai_records = [
                self._collection.GetRecord(
                    set=self.set,
                    identifier=self.identifier,
                    metadataPrefix=self.metadata_prefix,
                    ignore_deleted=True,
                )
            ]
        else:
            oai_records = self._collection.ListRecords(
                set=self.set, metadataPrefix=self.metadata_prefix, ignore_deleted=True
            )

        try:
            for counter, oai_record in enumerate(oai_records, start=1):
                xtree = etree.fromstring(oai_record.raw)
                if counter % 100 == 0:
                    logging.info(counter)
                record = self._construct_fields(xtree)
                record.update(self._from_metadata(xtree))
                self._records.append(record)

                self.record_count += 1
                if self.record_limit and self.record_count > self.record_limit:
                    logging.info(
                        f"truncating results because limit={self.record_limit}"
                    )
                    break
        except BadResumptionToken as e:
            if self.allow_expiration:
                logging.warning(
                    "Caught invalid resumption token, returning incomplete results"
                )
            else:
                raise e

    def _construct_fields(self, manifest: etree) -> dict:
        output: Dict[str, list] = {}
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
                if field not in output:
                    output[field] = []

                for data in result:
                    value = data.text.strip().strip("'").strip('"').replace('"', "###")
                    output[field].append(value)
        return output

    def _get_tag(self, el):
        """Return the element name, after removing namespace and lowercasing.
        If it's a MARC element the tag and code attributes will be used instead.
        """
        qel = etree.QName(el)
        if qel.namespace == NS["marc21"]:
            if qel.localname in ["controlfield", "datafield"]:
                return el.attrib["tag"]
            elif qel.localname == "subfield":
                return el.attrib["code"]
            else:
                return qel.localname
        else:
            return qel.localname

    # TODO: Discuss if this output shoould be an array (line 63) or a string
    def _from_metadata(self, manifest: etree) -> dict:
        if self.metadata_prefix == "oai_dc":
            oai_rec = manifest.xpath("//oai_dc:dc", namespaces=NS)[0]
        elif self.metadata_prefix == "mods" or self.metadata_prefix == "mods_no_ocr":
            oai_rec = manifest.xpath("//mods:mods", namespaces=NS)[0]
        elif self.metadata_prefix == "marc21":
            oai_rec = manifest.xpath("//marc21:record", namespaces=NS)[0]
        else:
            raise Exception(f"Unknown metadata prefix {self.metadata_prefix}")

        return self._element_to_dict(oai_rec)

    def _get_partition(self, i) -> pd.DataFrame:
        return pd.DataFrame(self._records)

    def _get_path_expressions(self):
        paths = {}
        for name, info in self.metadata.get("fields", {}).items():
            paths[name] = info

        return paths

    def _element_to_dict(self, rec_metadata):
        """Given an lxml Element will return a dictionary of key/value pairs."""
        result = {}
        for el in rec_metadata.getchildren():
            sub_element = self._flatten_tree(el)
            if not len(sub_element):
                continue

            tag = list(sub_element.keys())[0]
            # This odd encoding is a temporary measure to address inconsistent data entry
            # issues specifically with QNL data that cannot be addressed up stream at this time.
            # These encodings are decoded in dlme-transform
            value = (
                list(sub_element.values())[0]
                .strip()
                .strip("'")
                .strip('"')
                .replace('"', "###")
                .replace("'", "####")
            )

            if tag not in result:
                result[tag] = [value]
            else:
                result[tag].append(value)

        return result

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
        df = pd.concat([self.read_partition(i) for i in range(self.npartitions)])
        if self.record_limit:
            return df.head(self.record_limit)
        else:
            return df


def make_iterator(wait):
    """
    Return an iterator class depending on the desired wait behavior. If wait is
    0 then the default iterator class is returned: OAIItemIterator. Otherwise a
    WaitIterator class is created dynamically using the wait value, and it is
    returned. WaitIterator will be a subclass of OAIItemIterator but it will
    sleep for the given amount of seconds between issuing requests.
    """
    if wait == 0:
        return OAIItemIterator

    def _next_response(self):
        logging.info(f"sleeping {wait} seconds")
        time.sleep(wait)
        super(type(self), self)._next_response()

    # create a type SlowIterator that inherits from OAIItemIterator and
    # overrides the _next_response method
    return type("SlowIterator", (OAIItemIterator,), {"_next_response": _next_response})
