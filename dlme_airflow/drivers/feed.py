import logging
import intake
import json
import jsonpath_ng
import feedparser
import pandas as pd


class FeedSource(intake.source.base.DataSource):
    container = "dataframe"
    name = "feed"
    version = "0.0.1"
    partition_access = True

    def __init__(self, collection_urls, dtype=None, metadata=None):
        super(FeedSource, self).__init__(metadata=metadata)
        self.feed_urls = collection_urls
        self.dtype = dtype
        self._records = []

    def _open_feed(self, feed_url: str):
        feed = feedparser.parse(feed_url)
        entries = []
        for post in feed.entries:
            entry = self._construct_fields(json.dumps(post))
            entries.append(entry)

        self._records.append(entries)

    def _construct_fields(self, manifest: dict) -> dict:
        output = {}
        for name, info in self.metadata.get("fields").items():
            expression = self._path_expressions.get(name)
            result = [match.value for match in expression.find(manifest)]
            if len(result) < 1:
                if info.get("optional") is True:
                    # Skip and continue
                    continue
                else:
                    logging.warn(f"{manifest.get('@id')} missing {name}")
            else:
                output[name] = result[0]  # Use first value
        return output

    def _get_partition(self, i) -> pd.DataFrame:
        result = self._open_feed(self.feed_urls[i])
        return pd.DataFrame(
            [
                result,
            ]
        )

    def _get_schema(self):
        for name, info in self.metadata.get("fields", {}).items():
            self._path_expressions[name] = jsonpath_ng.parse(info.get("path"))
        # self._open_collection()
        return intake.source.base.Schema(
            datashape=None,
            dtype=self.dtype,
            shape=None,
            npartitions=len(self.collection_urls),
            extra_metadata={},
        )

    def read(self):
        self._load_metadata()
        return pd.concat([self.read_partition(i) for i in range(self.npartitions)])
