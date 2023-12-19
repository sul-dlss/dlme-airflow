import json

from dlme_airflow.utils.partition_url_builder import PartitionBuilder


def test_urls_from_provider(requests_mock):
    collection_url = "https://www.example.com"
    # mock the http call to return some real loc.gov data
    loc_data = json.load(open("tests/data/json/loc.json"))
    requests_mock.get(
        collection_url,
        json=loc_data,
    )

    partitionBuilder = PartitionBuilder(
        collection_url=collection_url,
        paging_config={"urls": "pagination.page_list"},
    )

    assert partitionBuilder.urls() == ["https://www.example.com"]


def test_calculate_partitions(requests_mock):
    collection_url = "https://www.example.com"
    # mock the http call to return some real loc.gov data
    page_data = json.load(open("tests/data/json/incremental_paging.json"))
    requests_mock.get(
        collection_url,
        json=page_data,
    )

    partitionBuilder = PartitionBuilder(
        collection_url=collection_url,
        paging_config={
            "increment": 10,
            "query_param": "offset",
            "result_count": "totalResults",
        },
    )

    assert partitionBuilder.urls() == [
        "https://www.example.com",
        "https://www.example.com&offset=10",
    ]


def test_prefetch_page_urls(requests_mock):
    collection_url = "https://www.example.com/object/"
    # mock the http call to return some real loc.gov data
    object_pages = json.load(open("tests/data/json/pre_fetch_ids.json"))
    requests_mock.get(
        "https://example.com/collection?limit=3&offset=0",
        json=object_pages,
    )

    partitionBuilder = PartitionBuilder(
        collection_url=collection_url,
        paging_config={
            "pages_url": "https://example.com/collection",
            "urls": "data.id",
            "limit": 3,
        },
    )

    assert partitionBuilder.urls() == [
        "https://www.example.com/object/116787",
        "https://www.example.com/object/60732",
    ]
