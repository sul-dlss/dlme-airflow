# /bin/python
import json
import os
import pandas
import urllib.request


def merge_dataframes(**kwargs):
    """Called by the Airflow workflow to merge multiple data sources for one provider
    e.g. when the data provider has IIIF and OAI-PMH and one source does not have all
    of the metadata."""
    collection = kwargs["collection"]
    data_file = collection.datafile("json")
    if os.path.isfile(data_file):
        df1 = read_datafile_with_lists(data_file)
        df2 = build_key_value_df(collection.catalog.metadata.get("iiif_collection_url"))
        df = pandas.merge(df1, df2, on=["id"])

        df.to_json(data_file, orient="records", force_ascii=False)

    return data_file


def read_datafile_with_lists(path) -> pandas.DataFrame:
    """Reads a JSON datafile and returns a Pandas DataFrame after having converted
    lists serialized as strings back into lists again.
    """
    df = pandas.read_json(path)
    df = df.applymap(lambda v: v if v else None)
    return df


def build_key_value_df(iiif_collection_manifest) -> pandas.DataFrame:
    """Builds a key/value pair datafram from IIIf v3 thumbnail urls and item manitest urls"""
    collection = json.loads(urllib.request.urlopen(iiif_collection_manifest).read())
    data = {}
    for manifest in collection["items"]:  # IIIF v3
        if "@id" in manifest:
            item_manifest_url = manifest["@id"]  # valid in IIIF v2 or v3
        elif "id" in manifest:
            item_manifest_url = manifest["id"]  # valid in IIIF v3 only

        item_manifest = json.loads(urllib.request.urlopen(item_manifest_url).read())
        if "items" in item_manifest:
            thumbnail_url = item_manifest["items"][0]["thumbnail"][0]["id"]

        data[item_manifest_url] = thumbnail_url

    key_value_df = pandas.DataFrame.from_dict(
        data, orient="index", columns=["id"]
    ).reset_index()
    key_value_df = key_value_df.rename(columns={"index": "item_manifest"})

    return key_value_df
