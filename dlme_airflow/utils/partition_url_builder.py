import requests
import jsonpath_ng


class PartitionBuilder:
    def __init__(
        self,
        collection_url,
        paging_config,
    ):
        self.collection_url = collection_url
        self.paging_config = paging_config
        self.provider_data = None
        self.partition_urls = [collection_url]

    def urls(self):
        self.provider_data = self._fetch_provider_data()

        if self.paging_config.get("urls"):
            self._urls_from_provider()
        elif self.paging_config.get("increment"):
            self._calculate_partitions()

        return self.partition_urls

    def _urls_from_provider(self):
        expression = jsonpath_ng.parse(self.paging_config["urls"])
        for match in expression.find(self.provider_data):
            if [match.value for match in expression.find(self.provider_data)]:
                page_urls = [
                    match.value for match in expression.find(self.provider_data)
                ][0]
            for next in page_urls:
                if next["url"]:
                    self.partition_urls.append(next["url"])

    def _calculate_partitions(self):
        increment = offset = self.paging_config["increment"]
        expression = jsonpath_ng.parse(self.paging_config["result_count"])
        record_count = expression.find(self.provider_data)[0].value

        while offset < record_count:
            self.partition_urls.append(f"{self.collection_url}&offset={offset}")
            offset += increment

    def _fetch_provider_data(self):
        resp = requests.get(self.collection_url)
        if resp.status_code == 200:
            return resp.json()
