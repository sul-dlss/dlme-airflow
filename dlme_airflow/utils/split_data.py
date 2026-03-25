import json
import re
from datetime import datetime
from pathlib import Path

# Candidate ID field names to try in priority order
_ID_CANDIDATES = ["id", "@id", "identifier", "emuIRN", "objectid", "record_id"]


def detect_id_field(records: list[dict]) -> str:
    """Return the most likely unique ID field name from a list of records."""
    for candidate in _ID_CANDIDATES:
        if all(candidate in r for r in records):
            return candidate
    # Fallback: field with the most unique values
    all_keys = {k for r in records for k in r}
    return max(all_keys, key=lambda k: len({r.get(k) for r in records}))


def dir_name_for_file(path: Path) -> str:
    """Return YYYYMMDDHHMM string from the file's modification time."""
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y%m%d%H%M")


def safe_filename(value) -> str:
    """Sanitize a value for use as a filename."""
    return re.sub(r"[^\w\-.]", "_", str(value))


def split_data_file(data_json_path: Path) -> Path | None:
    """Split a data.json into individual record files.

    Creates a sibling directory named YYYYMMDDHHMM (based on file mtime),
    then writes one {id}.json per record into that directory.

    Returns the output directory path, or None if the file is empty.
    """
    records = json.loads(data_json_path.read_text())
    if not records:
        return None

    id_field = detect_id_field(records)
    out_dir = data_json_path.parent / dir_name_for_file(data_json_path)
    out_dir.mkdir(exist_ok=True)

    for record in records:
        record_id = safe_filename(record.get(id_field, "unknown"))
        (out_dir / f"{record_id}.json").write_text(
            json.dumps(record, ensure_ascii=False, indent=2)
        )

    return out_dir
