# /bin/python
from bs4 import BeautifulSoup
import os
import pandas as pd

from utils.catalog import catalog_for_provider
from utils.requests_session import requests_retry_session


def dasi_scrape_thumbnail_urls(object_url_string):
    object_url = object_url_string.split("', '")[1].replace("]", "").replace("'", "")
    if object_url.startswith("http"):
        url = requests_retry_session().get(
            "http://dasi.cnr.it/index.php?id=79&prjId=1&corId=5&colId="
            f"0&navId=458870343&recId={object_url.split('recId=')[-1]}"
        )
        soup = BeautifulSoup(url.text, "html.parser")
        thumb_div = soup.find(class_="thumbnail_med")
        if thumb_div is None:
            pass
        else:
            thumb_url = thumb_div.find("img")["src"]
            return thumb_url


def dasi_add_thumbnail_urls(**kwargs):
    # Fetch working directory path from catalog and read file into Pandas dataframe
    catalog = catalog_for_provider("dasi")
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    data_path = catalog.metadata.get("data_path", "dasi/epigraphs")
    working_csv = os.path.join(root_dir, "working", data_path, "data.csv")
    df = pd.read_csv(working_csv)
    # Add thumbnail url column
    df["preview"] = df.apply(
        lambda row: dasi_scrape_thumbnail_urls(row["identifier"]), axis=1
    )

    df.to_csv(working_csv)
