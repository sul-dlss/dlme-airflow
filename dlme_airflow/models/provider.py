from dlme_airflow.utils.catalog import catalog_for_provider
from dlme_airflow.models.collection import Collection


class Provider(object):
    def __init__(self, catalog):
        self.name = catalog
        self.catalog = catalog_for_provider(self.name)

    @property
    def collections(self):
        _collections = []
        try:
            provider_collections = iter(list(self.catalog))
            for provider_collection in provider_collections:
                _collections.append(Collection(self, provider_collection))
        except TypeError as err:
            print(f"ERROR Parsing collections for provider {self.name}: {err}")

        return _collections

    def get_collection(self, collection_name):
        for coll in self.collections:
            if coll.name == collection_name:
                return coll
        return None

    def label(self):
        return self.name

    def data_path(self):
        return self.catalog.metadata.get("data_path", self.name)
