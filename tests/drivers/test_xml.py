import pytest
import requests
import pandas as pd

from dlme_airflow.drivers.xml import XmlSource


class MockHttpResponse:
    def __init__(self, response_text):
        self.content = response_text.encode("utf-8")


@pytest.fixture
def mock_response(monkeypatch):
    def mock_get(*args, **kwargs):
        if args[0] == "https://example.com/themaghribpodcast/feed.xml":
            xml = open("tests/data/xml/aims-feed.xml", "r").read()
            return MockHttpResponse(xml)

        raise f"No response mocked for request to {args[0]}"

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def xml_feed_test_source():
    itunes_namespace = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"}
    yahoo_namespace = {"media": "http://search.yahoo.com/mrss/"}
    metadata = {
        "data_path": "aims",
        "record_selector": {"path": ".//item", "namespace": None},
        "fields": {
            "id": {"path": ".//guid", "namespace": None, "optional": False},
            "title": {"path": ".//title", "namespace": None, "optional": True},
            "author": {
                "path": ".//itunes:author",
                "namespace": itunes_namespace,
                "optional": True,
            },
            "pub_date": {"path": ".//pubDate", "namespace": None, "optional": True},
            "description": {
                "path": ".//description",
                "namespace": None,
                "optional": True,
            },
            "extent": {
                "path": ".//itunes:duration",
                "namespace": itunes_namespace,
                "optional": True,
            },
            "link": {"path": ".//link", "namespace": None, "optional": True},
            "thumbnail": {
                "path": ".//media:content/@url",
                "namespace": yahoo_namespace,
                "optional": True,
            },
        },
    }
    return XmlSource(
        collection_url="https://example.com/themaghribpodcast/feed.xml",
        metadata=metadata,
    )


def test_XmlSource_read_cols(xml_feed_test_source, mock_response):
    xml_df = xml_feed_test_source.read()
    test_columns = [
        "id",
        "title",
        "author",
        "pub_date",
        "description",
        "extent",
        "link",
        "thumbnail",
    ]
    assert all([a == b for a, b in zip(xml_df.columns, test_columns)])


def test_XmlSource_df_content(xml_feed_test_source, mock_response):
    xml_df = xml_feed_test_source.read()
    test_df = pd.DataFrame(  # test_df is what we expect in the first and last rows, respectively, of the actual parsed data frame
        [
            {
                "id": "themaghribpodcast.podbean.com/7ab002fd-c4ac-3b48-96fa-eb83f4f324d5",
                "title": "Contemporary Art in Tunisia",
                "author": "themaghribpodcast.com",
                "pub_date": "Thu, 02 Jun 2022 09:31:00 -0500",
                "description": 'Episode 144: Contemporary Art in Tunisia As part of the AIMS Contemporary Art Fellowship, Ignacio\xa0Villalón conducted research into the contemporary art scene in Tunisia, exploring private and public cultural institutions, sources of funding, questions of language, and ongoing challenges. This project culminated in a report, written\xa0for academic and non-academic audiences alike. In this podcast,\xa0Villalón summarises the main findings of his research, focusing on a few select phenomena in the Tunisian art scene.\xa0 Ignacio Villalón is a writer, researcher, and journalist with a focus on politics and culture in the Mediterranean region. He received his Master\'s degree in History from the École des Hautes Études en Sciences Sociales, for which he conducted research on emigration (hijra) in early 20th century Algeria. As AIMS Contemporary Arts Fellow, he carried out research on the arts scene in Tunisia. He has published articles in "Le Quotidien d\'Oran" and "Africa is a Country." Ignacio is currently CAORC Social Sciences Fellow. This interview\xa0was recorded on May 13, 2022, via Zoom and led by\xa0Katarzyna Falecka, Lecturer in Art History at Newcastle University and Project Coordinator at the\xa0Centre d\'Études Maghrébines à Tunis\xa0(CEMAT) To see related slides, please visit our website: \xa0www.themaghribpodcast.com \xa0 We thank our friend Ignacio Villalón, AIMS\xa0contemporary art follow\xa0for his guitar performance of\xa0A vava Inouva\xa0of\xa0Idir\xa0for the introduction and conclusion of this podcast.\xa0 \xa0 Edited and Posted by:\xa0Hayet Lansari, Librarian, Outreach Coordinator, Content Curator (CEMA).',  # noqa: E501
                "extent": "31:55",
                "link": "https://themaghribpodcast.podbean.com/e/contemporary-art-in-tunisia/",
                "thumbnail": "https://pbcdn1.podbean.com/imglogo/ep-logo/pbblog1668744/Ignacio_Villalo_n_sizeaf193.png",
            },
            {
                "id": "themaghribpodcast.podbean.com/volubilis-between-romans-awraba-and-idris-i-24b00f90dd1700f333f1ee8f1a4eedd1",
                "title": "Volubilis: Between Romans, Awraba and Idris I",
                "author": "themaghribpodcast.com",
                "pub_date": "Mon, 25 Sep 2017 09:58:46 -0500",
                "description": "Episode 2:\xa0Volubilis: Between Romans, Awraba and Idris I [in English]    In this lecture, Dr. Elizabeth Fentress, Archaeologist, Honorary Visiting Professor at University College London and creator of Fasti Online discusses findings from excavations carried out between 2000 and 2005 by the Moroccan Institut National des Sciences de l'Archéologie et du Patrimoinee and University College London. This podcast describes the excavations in detail, and concentrates on the differences between those two communities. It should be listened to with the slides (see here), which are essential to understanding the site. The lecture, part of Languages & Societies in the Maghrib lecture series, was recorded for the\xa0 Centre d'Études Maghrébine en Algérie (CEMA), in Algiers, Algeria on 12 September 2017.",  # noqa: E501
                "extent": "42:15",
                "link": "https://themaghribpodcast.podbean.com/e/volubilis-between-romans-awraba-and-idris-i/",
                "thumbnail": "https://pbcdn1.podbean.com/imglogo/ep-logo/pbblog1668744/Slide_2.jpg",
            },
        ]
    )
    assert xml_df.iloc[0].equals(test_df.iloc[0])
    assert xml_df.iloc[20735].equals(test_df.iloc[1])
    assert len(xml_df) == 20736
