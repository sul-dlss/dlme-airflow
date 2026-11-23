import json
import re

import requests
from bs4 import BeautifulSoup


def get_schema(url, session=None, timeout=None) -> dict | None:
    "Extract schema.org metadata from a URL and return the parsed JSON."
    data = None
    resp = (session or requests).get(url, timeout=timeout)

    # raise on error responses so an error page isn't mistaken for a page without metadata
    resp.raise_for_status()

    # if we've got html
    if "html" in resp.headers.get("content-type", ""):
        # parse the html
        doc = BeautifulSoup(resp.content, "html.parser")

        # look for jsonld in a <script> element and return the first one
        jsonld = doc.select('script[type="application/ld+json"]', first=True)

        # TODO if this is going to be generalizable across sites
        # - normalize the json-ld to account for different vocabs?
        # - look for json-ld in RDFa or schema.org?

        # if we found json-ld parse the first one we found as JSON
        if len(jsonld) > 0:
            data = parse_jsonld(jsonld[0].get_text())

    return data


# a valid JSON escape sequence, or a lone backslash that doesn't start one
ESCAPE = re.compile(r'\\["\\/bfnrtu]|\\')


def parse_jsonld(text) -> dict:
    """Parse JSON-LD, tolerating control characters and stray backslashes.

    Some providers put unescaped backslashes in their JSON-LD (e.g. Penn's
    "joins 4544\\4550"), which isn't valid JSON. If parsing fails, escape any
    backslash that doesn't begin a valid escape sequence and try again.
    """
    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError:
        fixed = ESCAPE.sub(lambda m: m.group() if len(m.group()) == 2 else "\\\\", text)
        return json.loads(fixed, strict=False)
