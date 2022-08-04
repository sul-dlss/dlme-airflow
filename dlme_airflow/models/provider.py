from utils.catalog import catalog_for_provider
from models.collection import Collection

class Provider(object):
  def __init__(self, catalog):
    self.name = catalog
    self.catalog = catalog_for_provider(catalog)
    self.collections = self.__collections_for()


  def data_path(self):
    return self.catalog.metadata.get("data_path", self.name)


  def __collections_for(self):
    collections = []
    try:
      provider_collections = iter(list(self.catalog))
      for provider_collection in provider_collections:
          collections.append(Collection(self, provider_collection))
    except TypeError as err:
      print(f"ERROR Parsing collections for provider: {self.name}")

    return collections
