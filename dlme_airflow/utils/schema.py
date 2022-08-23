import json
import requests

from typing import Optional
from bs4 import BeautifulSoup

def get_schema(url) -> Optional[dict]:
    "Extract schema.org metadata from a URL and return the parsed JSON."
    data = None
    resp = requests.get(url)

    # if we've got html
    if 'html' in resp.headers.get('content-type', ''):

        # parse the html
        doc = BeautifulSoup(resp.content, 'html.parser')
        
        # look for jsonld in a <script> element and return the first one
        jsonld = doc.select('script[type="application/ld+json"]', first=True)

        # TODO if this is going to be generalizable across sites
        # - normalize the json-ld to account for different vocabs?
        # - look for json-ld in RDFa or schema.org?

        # if we found json-ld parse the first one we found as JSON
        if len(jsonld) > 0:
            data = json.loads(jsonld[0].get_text(), strict=False)

    return data
