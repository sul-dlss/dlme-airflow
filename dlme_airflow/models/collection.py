import os

from dlme_airflow.utils.catalog import catalog_for_provider


class Collection(object):
    def __init__(self, provider, collection):
        self.name = collection
        self.provider = provider
        self.catalog = catalog_for_provider(f"{self.provider.name}.{self.name}")
        self.last_harvest_start_date = None

    def label(self):
        return f"{self.provider.name}_{self.name}"

    def data_path(self):
        return self.catalog.metadata.get("data_path")

    def intermediate_representation_location(self):
        normalized_data_path = self.data_path().replace("/", "-").replace("_", "-")
        return f"output-{normalized_data_path}.ndjson"

    def archive_dir(self):
        archive_root = os.environ.get("ARCHIVE_PATH", "archive")
        return os.path.join(archive_root, self.data_path())

    def datafile(self, format):
        OUTPUT_FORMATS = ["csv", "json"]
        working_data_path = os.path.abspath("working")

        if format in OUTPUT_FORMATS:
            filename = f"data.{format}"
            return os.path.join(working_data_path, self.data_path(), filename)
        else:
            raise Exception(f"Unsupported data output format: {format}")
    
    def filters(self):
        return self.catalog.metadata.get("filters", {})
