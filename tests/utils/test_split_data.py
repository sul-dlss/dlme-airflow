import json

from dlme_airflow.utils.split_data import detect_id_field, safe_filename, split_data_file


def test_detect_id_field_uses_known_candidate():
    records = [{"id": "abc", "title": "foo"}, {"id": "def", "title": "bar"}]
    assert detect_id_field(records) == "id"


def test_detect_id_field_prefers_earlier_candidate():
    # "id" should beat "identifier" since it appears first in _ID_CANDIDATES
    records = [{"id": "1", "identifier": "i1"}, {"id": "2", "identifier": "i2"}]
    assert detect_id_field(records) == "id"


def test_detect_id_field_falls_back_to_most_unique():
    records = [{"ref": "r1", "type": "a"}, {"ref": "r2", "type": "a"}]
    # "ref" has more unique values (2) than "type" (1)
    assert detect_id_field(records) == "ref"


def test_split_data_file_creates_output_dir(tmp_path):
    data = [{"id": "obj1", "title": "Object One"}, {"id": "obj2", "title": "Object Two"}]
    data_json = tmp_path / "data.json"
    data_json.write_text(json.dumps(data))

    out_dir = split_data_file(data_json)

    assert out_dir.is_dir()
    assert (out_dir / "obj1.json").exists()
    assert (out_dir / "obj2.json").exists()


def test_split_data_file_record_content(tmp_path):
    data = [{"id": "x", "val": 42}]
    data_json = tmp_path / "data.json"
    data_json.write_text(json.dumps(data))

    out_dir = split_data_file(data_json)

    record = json.loads((out_dir / "x.json").read_text())
    assert record == {"id": "x", "val": 42}


def test_split_data_file_sanitizes_id(tmp_path):
    data = [{"id": "a/b:c", "val": 1}]
    data_json = tmp_path / "data.json"
    data_json.write_text(json.dumps(data))

    out_dir = split_data_file(data_json)

    assert (out_dir / "a_b_c.json").exists()


def test_split_data_file_uses_identifier_field(tmp_path):
    data = [{"identifier": "item-42", "title": "A thing"}]
    data_json = tmp_path / "data.json"
    data_json.write_text(json.dumps(data))

    out_dir = split_data_file(data_json)

    assert (out_dir / "item-42.json").exists()


def test_split_data_file_empty_returns_none(tmp_path):
    data_json = tmp_path / "data.json"
    data_json.write_text("[]")
    assert split_data_file(data_json) is None


def test_split_data_file_dir_is_sibling(tmp_path):
    data = [{"id": "r1"}]
    data_json = tmp_path / "data.json"
    data_json.write_text(json.dumps(data))

    out_dir = split_data_file(data_json)

    assert out_dir.parent == tmp_path


def test_safe_filename_replaces_special_chars():
    assert safe_filename("abc/def:123") == "abc_def_123"


def test_safe_filename_numeric():
    assert safe_filename(42) == "42"


def test_safe_filename_preserves_hyphens():
    assert safe_filename("item-001") == "item-001"
