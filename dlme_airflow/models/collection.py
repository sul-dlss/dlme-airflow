from utils.catalog import catalog_for_provider


class Collection(object):
    def __init__(self, provider, collection):
        self.name = collection
        self.provider = provider
        self.catalog = catalog_for_provider(f"{provider.name}.{self.name}")

    def label(self):
        return f"{self.provider.name}_{self.name}"

    def data_path(self):
        return self.catalog.metadata.get("data_path")

    def intermidiate_representation_location(self):
        return f"https://dlme-metadata-dev.s3.us-west-2.amazonaws.com/output/output-{self.data_path()}.ndjson"