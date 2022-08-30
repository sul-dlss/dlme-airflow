import pandas
import logging

from intake.source.base import DataSource, Schema


class SequentialCsvSource(DataSource):
    """Loads multiple CSV files sequentially with pandas rather than in parallel
    with dask. This can be helpful in situations where dask is aggravating the
    web server that is publishing the CSV data with HEAD requests and things
    that it is not able to respond to in parallel.
    """

    container = "dataframe"
    name = "sequential-csv"
    version = "0.0.1"
    partition_access = True

    def __init__(self, urlpath, metadata={}, csv_kwargs={}):
        self.urls = urlpath if type(urlpath) == list else [urlpath]
        self.csv_kwargs = csv_kwargs
        super(SequentialCsvSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        return Schema(
            datashape=None,
            dtype=self.csv_kwargs.get("dtype"),
            shape=None,
            npartitions=len(self.urls),
        )

    def _get_partition(self, i):
        logging.info(f"reading {self.urls[i]}")
        return pandas.read_csv(self.urls[i])

    def read(self):
        self._load_metadata()
        return pandas.concat([self.read_partition(i) for i in range(self.npartitions)])
