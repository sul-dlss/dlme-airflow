import pandas
import urllib.request

from dlme_airflow.utils.add_iiif_v3_source import (
    build_key_value_df,
    read_datafile_with_lists,
)


class MockResponse:
    @staticmethod
    def read():
        return """{
          "@context": "http://iiif.io/api/presentation/3/context.json",
          "id": "https://iiif.library.ucla.edu/ark%3A%2F21198%2Fz1qz66ps/manifest",
          "type": "Manifest",
          "label": {
            "none": ["4 Wheeled Cart"]
          },
          "items": [{
            "id": "https://iiif.library.ucla.edu/ark%3A%2F21198%2Fz1qz66ps/manifest/canvas-noc6",
            "type": "Canvas",
            "label": {
              "none": ["4 Wheeled Cart"]
            },
            "height": 2830,
            "width": 4137,
            "thumbnail": [{
              "id": "https://iiif.library.ucla.edu/iiif/2/ark%3A%2F21198%2Fz1qz66ps/full/!200,200/0/default.jpg",
              "type": "Image",
              "format": "image/jpeg"
            }],
            "items": [{
              "id": "https://iiif.library.ucla.edu/ark%3A%2F21198%2Fz1qz66ps/manifest/canvas-noc6/anno-page-bxqo",
              "type": "AnnotationPage",
              "items": [{
                "id": "https://iiif.library.ucla.edu/ark%3A%2F21198%2Fz1qz66ps/manifest/annotations/anno-oxdb",
                "type": "Annotation",
                "motivation": "painting",
                "body": {
                  "id": "https://iiif.library.ucla.edu/iiif/2/ark%3A%2F21198%2Fz1qz66ps/full/600,/0/default.jpg",
                  "type": "Image",
                  "format": "image/jpeg",
                  "service": [{
                    "@id": "https://iiif.library.ucla.edu/iiif/2/ark%3A%2F21198%2Fz1qz66ps",
                    "@type": "ImageService2",
                    "profile": "http://iiif.io/api/image/2/level2.json"
                  }]
                },
                "target": "https://iiif.library.ucla.edu/ark%3A%2F21198%2Fz1qz66ps/manifest/canvas-noc6"
              }]
            }]
          }]
        }"""


def test_build_key_value_df(monkeypatch):
    def mock_urlopen(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    result = build_key_value_df("https://fake_iiif_v3_item_manifest_url.com")

    assert "item_manifest" in result.columns
    assert "id" in result.columns
    assert len(result.columns) == 2


def test_read_datafile_with_lists():
    df = read_datafile_with_lists("tests/data/json/qnl.json")

    assert isinstance(df, pandas.DataFrame)
