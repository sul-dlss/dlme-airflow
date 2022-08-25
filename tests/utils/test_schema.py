from dlme_airflow.utils.schema import get_schema


def test_get_schema():
    data = get_schema("https://www.penn.museum/collections/object/15390")
    assert (
        data["thumbnailUrl"]
        == "https://www.penn.museum/collections/assets/327554_800.jpg"
    )


def test_no_schema():
    data = get_schema("https://example.org")
    assert data is None


def test_no_html():
    data = get_schema(
        "https://upload.wikimedia.org/wikipedia/commons/2/2b/Stanford_University_Green_Library_Bing_Wing.jpg"
    )
    assert data is None


def test_control_chars():
    data = get_schema("https://www.penn.museum/collections/object/46326")
    assert type(data) == dict
