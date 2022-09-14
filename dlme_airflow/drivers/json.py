import pandas
import logging
import requests
import jsonpath_ng

from intake.source.base import DataSource, Schema


class JsonSource(DataSource):
    """
    JsonSource lets you configure your Intake catalog entry with JSONPath
    expressions that populate the resulting Pandas DataFrame.
    See https://github.com/h2non/jsonpath-ng#jsonpath-syntax for what constitues a valid JSONPath.

    For example if your Intake catalog contains:

        args:
            collection_url: https://www.loc.gov/collections/persian-language-rare-materials/?c=100&fo=json
        metadata:
          record_selector: "content.results"
          fields:
            id:
              path: "id",
            title:
              path: "title"
            thumbnail:
              path: "resources[0].image"

    and the collection URL returns JSON that looks like:

    {
      "content": {
        "results": [
          {
            "id": "http://www.loc.gov/item/2017498321/",
            "title": "[Majmuʻah, or, Collection]",
            "resources": [
              "image": "https://tile.loc.gov/image-services/iiif/service:amed:plmp:m154:0334/full/pct:6.25/0/default.jpg"
            ]
          },
          {
            "id": "http://www.loc.gov/item/00313408/",
            "title": "Sakīnat al-fuz̤alāʼ mawsūm bi-ism-i tārīkhī-i Bahār-i Afghānī",
            "resources": [
              "image": "https://tile.loc.gov/image-services/iiif/service:amed:amedpllc:00406537456:0055/full/pct:6.25/0/default.jpg"
            ]
          }
        ]
      }
    }

    You would end up with a CSV with three columns (id, title and thumbnail) and two rows.
    """

    container = "dataframe"
    name = "custom_json"
    version = "0.0.1"
    partition_access = True

    def __init__(self, collection_url, metadata={}, csv_kwargs={}):
        super(JsonSource, self).__init__(metadata=metadata)
        self.collection_url = collection_url
        self.metadata = metadata
        self.csv_kwargs = csv_kwargs
        self.record_limit = self.metadata.get("record_limit", None)
        self.record_count = 0
        self._setup_json_paths()

    def read(self):
        self._load_metadata()
        df = self._get_dataframe()
        if self.record_limit:
            return df.head(self.record_limit)
        else:
            return df

    def _get_schema(self):
        return Schema(
            datashape=None,
            dtype=self.csv_kwargs.get("dtype"),
            shape=None,
            npartitions=1,
        )

    def _get_dataframe(self):
        resp = requests.get(self.collection_url)
        if resp.status_code != 200:
            raise Exception(
                f"HTTP request for {self.collection_url} resulted in {resp.status_code}"
            )
        return self._process_json(resp.json())

    def _process_json(self, data):
        result = self.record_selector.find(data)
        if len(result) == 0:
            raise Exception(
                f"Couldn't find records selector: {self.record_selector.expression}"
            )

        objects = []
        for rec in result[0].value:
            obj = {}
            for field in self.field_paths:
                path = field["path"]
                name = field["name"]
                result = path.find(rec)
                if len(result) == 1:
                    obj[name] = result[0].value
                elif len(result) > 1:
                    obj[name] = [m.value for m in result]
                elif not field["optional"]:
                    raise Exception(f"{name} is not optional")
            objects.append(obj)

        return pandas.DataFrame(objects)

    def _setup_json_paths(self):
        # get the record selector and parse it
        path = self.metadata.get("record_selector")
        if path is None:
            raise Exception("JsonSource metadata must define a record_selector")
        self.record_selector = jsonpath_ng.parse(path)

        # get the mapping of field names to jsonpaths
        fields = self.metadata.get("fields")
        if fields is None or type(fields) is not dict:
            raise Exception("JsonSource metadata must define fields as an object")

        self.field_paths = []
        for field_name, field_info in fields.items():
            if type(field_info) != dict:
                raise Exception(
                    f"The value for metadata field {field_name} must be an object"
                )

            path = field_info.get("path")
            if path is None:
                raise Exception(f"The metadata field {field_name} must define a path")

            self.field_paths.append(
                {
                    "name": field_name,
                    "path": jsonpath_ng.parse(path),
                    "optional": field_info.get("optional"),
                }
            )

        logging.info(f"Found jsonpaths: {self.field_paths}")
