import pytest

from harvester.iiif_json import iiif_json_harvester


def test_iiif_json_harvester():
    assert iiif_json_harvester


def test_provider_not_found():
    with pytest.raises(ValueError, match=r"bad_provider not found in catalog"):
        iiif_json_harvester("bad_provider")
