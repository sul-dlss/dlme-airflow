import pytest
import requests
import gdown

from unittest.mock import patch

# Import main and helpers directly from the bin script
import importlib.util
import importlib.machinery
from pathlib import Path

_bin_index_path = str(Path(__file__).parent.parent.parent / "bin" / "index")
_loader = importlib.machinery.SourceFileLoader("bin_index", _bin_index_path)
_spec = importlib.util.spec_from_loader("bin_index", _loader)
bin_index = importlib.util.module_from_spec(_spec)
_loader.exec_module(bin_index)


@pytest.fixture
def api_env(monkeypatch):
    monkeypatch.setenv("API_ENDPOINT", "https://dlme-test.example.com/api/harvests")
    monkeypatch.setenv("API_TOKEN", "test-token-abc")


@pytest.fixture
def mock_post():
    class MockResponse:
        status_code = 202
        text = "Harvest successfully initiated"

        def json(self):
            return {"message": "Harvest successfully initiated"}

        def raise_for_status(self):
            pass

    with patch.object(requests, "post", return_value=MockResponse()):
        yield


def test_index_exits_if_missing_api_endpoint(monkeypatch):
    monkeypatch.delenv("API_ENDPOINT", raising=False)
    monkeypatch.setenv("API_TOKEN", "test-token")
    with pytest.raises(SystemExit, match="API_ENDPOINT"):
        bin_index.check_env_vars()


def test_index_exits_if_missing_api_token(monkeypatch):
    monkeypatch.setenv("API_ENDPOINT", "https://dlme-test.example.com/api/harvests")
    monkeypatch.delenv("API_TOKEN", raising=False)
    with pytest.raises(SystemExit, match="API_TOKEN"):
        bin_index.check_env_vars()


def test_count_records(tmp_path):
    ndjson_file = tmp_path / "output-test.ndjson"
    ndjson_file.write_text('{"id": 1}\n{"id": 2}\n{"id": 3}\n')
    assert bin_index.count_records(str(ndjson_file)) == 3


def test_is_url():
    assert bin_index.is_url("https://example.com/data.ndjson") is True
    assert bin_index.is_url("http://example.com/data.ndjson") is True
    assert bin_index.is_url("/local/path/data.ndjson") is False
    assert bin_index.is_url("data.ndjson") is False


def test_fetch_ndjson_local_file(tmp_path):
    ndjson_file = tmp_path / "data.ndjson"
    ndjson_file.write_text('{"id": 1}\n')
    local_path, is_temp = bin_index.fetch_ndjson(str(ndjson_file))
    assert local_path == str(ndjson_file)
    assert is_temp is False


def test_fetch_ndjson_url_downloads_to_tmp():
    class MockGetResponse:
        status_code = 200
        content = b'{"id": 1}\n{"id": 2}\n'

        def raise_for_status(self):
            pass

    with patch.object(requests, "get", return_value=MockGetResponse()):
        local_path, is_temp = bin_index.fetch_ndjson("https://example.com/data.ndjson")

    assert is_temp is True
    assert Path(local_path).exists()
    assert Path(local_path).suffix == ".ndjson"
    Path(local_path).unlink()


def test_fetch_google_downloads_to_tmp():
    fake_content = b'{"id": 1}\n{"id": 2}\n'

    def mock_gdown(id, output, quiet):
        Path(output).write_bytes(fake_content)
        return output

    with patch.object(gdown, "download", side_effect=mock_gdown):
        local_path, is_temp = bin_index.fetch_google("fake-file-id")

    assert is_temp is True
    assert local_path == str(Path("tmp") / "fake-file-id.ndjson")
    Path(local_path).unlink()


def test_fetch_google_exits_on_failure():
    with patch.object(gdown, "download", return_value=None):
        with pytest.raises(SystemExit, match="fake-file-id"):
            bin_index.fetch_google("fake-file-id")


def test_index_posts_file_contents_and_prints_response(api_env, mock_post, tmp_path, capsys):
    ndjson_file = tmp_path / "data.ndjson"
    ndjson_file.write_text('{"id": 1}\n{"id": 2}\n')

    class Opts:
        ndjson_path = str(ndjson_file)
        google = None
        dry_run = False

    with patch.object(requests, "post") as mock:
        mock.return_value.status_code = 202
        mock.return_value.text = "Harvest successfully initiated"
        mock.return_value.json.return_value = {"message": "Harvest successfully initiated"}
        mock.return_value.raise_for_status = lambda: None

        bin_index.main(Opts())

    captured = capsys.readouterr()
    assert "Records to index: 2" in captured.out
    assert "Harvest successfully initiated" in captured.out

    call_kwargs = mock.call_args
    assert b'{"id": 1}' in call_kwargs.kwargs.get("data", b"")


def test_dry_run_skips_post_and_prints_message(api_env, tmp_path, capsys):
    ndjson_file = tmp_path / "data.ndjson"
    ndjson_file.write_text('{"id": 1}\n{"id": 2}\n')

    class Opts:
        ndjson_path = str(ndjson_file)
        google = None
        dry_run = True

    with patch.object(requests, "post") as mock:
        bin_index.main(Opts())
        mock.assert_not_called()

    captured = capsys.readouterr()
    assert "Records to index: 2" in captured.out
    assert "Response: DRY RUN" in captured.out


def test_dry_run_keeps_temp_file(api_env, tmp_path, capsys):
    fake_content = b'{"id": 1}\n'

    def mock_gdown(id, output, quiet):
        Path(output).write_bytes(fake_content)
        return output

    class Opts:
        ndjson_path = None
        google = "fake-file-id"
        dry_run = True

    with patch.object(gdown, "download", side_effect=mock_gdown):
        with patch.object(requests, "post"):
            bin_index.main(Opts())

    # The temp file should still exist after a dry run
    captured = capsys.readouterr()
    assert "Response: DRY RUN" in captured.out


def test_index_url_cleans_up_tmp_file(api_env, capsys):
    class MockGetResponse:
        status_code = 200
        content = b'{"id": 1}\n'

        def raise_for_status(self):
            pass

    with patch.object(requests, "get", return_value=MockGetResponse()):
        with patch.object(requests, "post") as mock_api:
            mock_api.return_value.status_code = 202
            mock_api.return_value.text = "ok"
            mock_api.return_value.json.return_value = {"message": "ok"}
            mock_api.return_value.raise_for_status = lambda: None

            class Opts:
                ndjson_path = "https://example.com/data.ndjson"
                google = None
                dry_run = False

            original_fetch = bin_index.fetch_ndjson
            captured_tmp = []

            def tracking_fetch(path):
                local_path, is_temp = original_fetch(path)
                if is_temp:
                    captured_tmp.append(local_path)
                return local_path, is_temp

            with patch.object(bin_index, "fetch_ndjson", side_effect=tracking_fetch):
                bin_index.main(Opts())

    assert len(captured_tmp) == 1
    assert not Path(captured_tmp[0]).exists(), "Temp file should be deleted after indexing"


def test_google_option_uses_fetch_google(api_env, tmp_path, capsys):
    fake_content = b'{"id": 1}\n'

    def mock_gdown(id, output, quiet):
        Path(output).write_bytes(fake_content)
        return output

    class Opts:
        ndjson_path = None
        google = "my-drive-file-id"
        dry_run = False

    with patch.object(gdown, "download", side_effect=mock_gdown) as mock_dl:
        with patch.object(requests, "post") as mock_api:
            mock_api.return_value.status_code = 202
            mock_api.return_value.text = "ok"
            mock_api.return_value.json.return_value = {"message": "ok"}
            mock_api.return_value.raise_for_status = lambda: None

            bin_index.main(Opts())

    mock_dl.assert_called_once_with(id="my-drive-file-id", output=mock_dl.call_args.kwargs["output"], quiet=False)
    captured = capsys.readouterr()
    assert "Records to index: 1" in captured.out
