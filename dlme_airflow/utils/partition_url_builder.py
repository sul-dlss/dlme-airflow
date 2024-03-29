import requests
import jsonpath_ng


class PartitionBuilder:
    """Determine the method used to extract or format the
    page queries when provider API requires paging.
    """

    def __init__(
        self,
        collection_url,
        paging_config,
        api_key=None,
    ):
        self.collection_url = collection_url
        self.paging_config = paging_config
        self.provider_data = None
        self.api_key = api_key

    def urls(self):
        if self.paging_config.get("pages_url"):
            return self._prefetch_page_urls()

        self.provider_data = self._fetch_provider_data(self.collection_url)
        if self.paging_config.get("increment"):
            return self._calculate_partitions()
        elif self.paging_config.get("urls"):
            return self._urls_from_provider()

        return []

    def _urls_from_provider(self):
        urls = [self.collection_url]
        expression = jsonpath_ng.parse(self.paging_config["urls"])
        [
            urls.append(page["url"])
            for page in expression.find(self.provider_data)[0].value
            if page["url"] is not None
        ]
        return urls

    def _calculate_partitions(self):
        urls = [self.collection_url]
        increment = offset = self.paging_config["increment"]
        offset_param = self.paging_config["query_param"]
        expression = jsonpath_ng.parse(self.paging_config["result_count"])
        record_count = expression.find(self.provider_data)[0].value

        # This is a hack to skip the first page of results because the
        # LOC paging is 0 based with a 1 increment and the "sp=1" page is skipped.
        # Therefore example paging for 4 pages is sp=0, sp=2, sp=3, and sp=4
        if self.paging_config.get("skip_first"):
            offset = self.paging_config["increment"] + 1
            record_count += 1

        while offset < record_count:
            urls.append(f"{self.collection_url}&{offset_param}={offset}")
            offset += increment

        return urls

    def _prefetch_page_urls(self):
        offset = 0
        harvested = 0
        ids = []
        while True:
            api_endpoint = f"{self.paging_config['pages_url']}?limit={self.paging_config['limit']}&offset={offset}"
            data = self._fetch_provider_data(api_endpoint)["data"]
            offset += self.paging_config["limit"]
            harvested = len(data)

            for i in data:
                ids.append(f"{self.collection_url}{i['id']}")

            if harvested < self.paging_config["limit"]:
                break

        return ids

    def _fetch_provider_data(self, url):
        headers = {}
        if self.api_key:
            headers["api_key"] = self.api_key

        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json()
