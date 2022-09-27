from dlme_airflow.utils.catalog import catalog_for_provider


class Collection(object):
    def __init__(self, provider, collection):
        self.name = collection
        self.provider = provider
        self.catalog = catalog_for_provider(f"{self.provider.name}.{self.name}")

    def label(self):
        return f"{self.provider.name}_{self.name}"

    def data_path(self):
        return self.catalog.metadata.get("data_path")

    def intermidiate_representation_location(self):
        normalized_data_path = self.data_path().replace("/", "-")
        return f"https://dlme-metadata-dev.s3.us-west-2.amazonaws.com/output/output-{normalized_data_path}.ndjson"
