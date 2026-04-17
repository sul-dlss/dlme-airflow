import json
import pandas
import logging

from pathlib import Path
from intake.source.base import DataSource, Schema
from dlme_airflow.utils.split_data import detect_id_field, safe_filename


class SequentialCsvSource(DataSource):
    """Loads multiple CSV or TSV files sequentially with pandas rather than in parallel
    with dask. This can be helpful in situations where dask is aggravating the
    web server that is publishing the CSV data with HEAD requests and things
    that it is not able to respond to in parallel.
    """

    container = "dataframe"
    name = "sequential-csv"
    version = "0.0.1"
    partition_access = True

    def __init__(self, urlpath, metadata=None, csv_kwargs=None):
        metadata = metadata or {}
        csv_kwargs = csv_kwargs or {}
        super().__init__(metadata=metadata)
        self.urls = urlpath if isinstance(urlpath, list) else [urlpath]
        self.csv_kwargs = csv_kwargs
        self.record_limit = self.metadata.get("record_limit")
        self.record_count = 0

    def _get_schema(self):
        return Schema(
            datashape=None,
            dtype=self.csv_kwargs.get("dtype"),
            shape=None,
            npartitions=len(self.urls),
            extra_metadata={},
        )

    def _get_partition(self, i):
        if self.record_limit and self.record_count > self.record_limit:
            logging.info(f"skipping partition because record limit {self.record_limit}")
            return pandas.DataFrame()
        logging.info(f"reading {self.urls[i]}")
        if self.urls[i].endswith(".tsv"):
            df = pandas.read_csv(self.urls[i], sep="\t")
        else:
            df = pandas.read_csv(self.urls[i])
        self.record_count += len(df)

        if getattr(self, '_mode', 'production') == 'analyze' and self._output_dir:
            self._output_dir.mkdir(parents=True, exist_ok=True)
            records = df.to_dict(orient='records')
            if records:
                id_field = detect_id_field(records)
                for record in records:
                    record_id = safe_filename(record.get(id_field, 'unknown'))
                    (self._output_dir / f"{record_id}.json").write_text(
                        json.dumps(record, ensure_ascii=False, indent=2)
                    )

        return df

    def read(self, mode="production", output_dir=None, **kwargs):
        self._mode = mode
        self._output_dir = Path(output_dir) if output_dir else None
        self._load_metadata()
        df = pandas.concat(self.read_partition(i) for i in range(self.npartitions))
        if self.record_limit:
            return df.head(self.record_limit)
        else:
            return df
