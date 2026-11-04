# /bin/python
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dlme_airflow.utils.schema import get_schema

logger = logging.getLogger(__name__)

# How many pages to fetch at once; kept small to be polite to the provider,
# since a provider's collections can run their post-harvest steps in parallel
MAX_WORKERS = 2

# Seconds to wait for a connection and then for a response
TIMEOUT = (10, 30)

# Fail the task if more than this share of lookups fail, so an outage at the
# provider shows up as a failed task instead of quietly dropped records
MAX_FAILURE_RATE = 0.02

# Save the cache after this many lookups so a failed run keeps its progress
SAVE_EVERY = 500

# Lookups are cached next to the working data so later runs only fetch new records.
# Delete this file to force every thumbnail to be looked up again.
CACHE_FILENAME = "thumbnails.json"

_thread_local = threading.local()


def add_thumbnails(**kwargs) -> None:
    """Add a thumbnail column based on the value in the url column"""
    coll = kwargs["collection"]
    data_path = coll.data_path()
    if data_path is None:
        raise ValueError(f"unable to find data_path for collection={coll}")

    # ensure that the working json is present on the filesystem
    working_json = coll.datafile("json")
    if not os.path.isfile(working_json):
        raise FileNotFoundError(f"unable to locate working json data: {working_json}")

    df = pandas.read_json(working_json)

    cache_path = os.path.join(os.path.dirname(working_json), CACHE_FILENAME)
    cache = load_cache(cache_path)
    urls = [url for url in df["Record URL"].unique() if url not in cache]
    logger.info(f"{len(cache)} thumbnails cached, looking up {len(urls)}")

    failures = lookup_thumbnails(urls, cache, cache_path)
    save_cache(cache, cache_path)

    if urls and len(failures) / len(urls) > MAX_FAILURE_RATE:
        raise RuntimeError(
            f"{len(failures)} of {len(urls)} thumbnail lookups failed; "
            "the completed lookups are cached, so re-running the task will resume"
        )

    # add a thumbnail column and save it; records with no thumbnail are dropped
    df["thumbnail"] = df["Record URL"].map(cache)
    df.dropna(subset=["thumbnail"], inplace=True)
    df.to_json(working_json, orient="records")


def lookup_thumbnails(urls, cache, cache_path) -> list:
    """Look up thumbnails for urls, adding results to cache. Returns the urls that failed."""
    failures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_thumbnail, url): url for url in urls}
        for count, future in enumerate(as_completed(futures), start=1):
            url = futures[future]
            try:
                cache[url] = future.result()
            # catch everything so one bad record can't stop the run; an exception
            # escaping here would also make the executor wait on every queued lookup
            except Exception as e:  # noqa: BLE001
                logger.error(f"unable to get thumbnail for {url}: {e}")
                failures.append(url)

            if count % SAVE_EVERY == 0:
                save_cache(cache, cache_path)
                logger.info(f"looked up {count} of {len(urls)} thumbnails")

    return failures


def get_thumbnail(id) -> str | None:
    """Returns the thumbnail url for a record, or None if its page has none.

    HTTP errors and unparseable JSON-LD are raised rather than returned as None,
    so they count as failed lookups and are retried on the next run instead of
    being cached as "no thumbnail".
    """
    url = id
    logger.debug(f"getting thumbnail for {url}")
    schema = get_schema(url, session=get_session(), timeout=TIMEOUT)
    if schema:
        return schema.get("thumbnailUrl")
    else:
        return None


def get_session() -> requests.Session:
    """Returns a session for the current thread that retries failed requests."""
    if not hasattr(_thread_local, "session"):
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        _thread_local.session = session

    return _thread_local.session


def load_cache(cache_path) -> dict:
    if not os.path.isfile(cache_path):
        return {}

    with open(cache_path) as cache_file:
        return json.load(cache_file)


def save_cache(cache, cache_path) -> None:
    # write to a temporary file first so a crash mid-write can't corrupt the cache
    tmp_path = f"{cache_path}.tmp"
    with open(tmp_path, "w") as cache_file:
        json.dump(cache, cache_file)
    os.replace(tmp_path, cache_path)
