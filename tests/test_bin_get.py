import pytest
import pandas as pd

from pathlib import Path
from unittest.mock import MagicMock, patch
from importlib.machinery import SourceFileLoader
from importlib.util import spec_from_loader, module_from_spec


def _load_bin_get():
    """Load bin/get as a module despite having no .py extension."""
    loader = SourceFileLoader("bin_get", "bin/get")
    spec = spec_from_loader("bin_get", loader)
    module = module_from_spec(spec)
    loader.exec_module(module)
    return module


def _make_provider_mock(df=None):
    """Return a mock Provider class whose get_collection() returns a usable catalog."""
    if df is None:
        df = pd.DataFrame([{"id": "test1"}])

    catalog_mock = MagicMock()
    catalog_mock.read.return_value = df

    collection_mock = MagicMock()
    collection_mock.catalog = catalog_mock

    provider_mock = MagicMock()
    provider_mock.return_value.get_collection.return_value = collection_mock

    return provider_mock


def _make_opts(provider="penn", collection="penn_egyptian", mode="production",
               output=None, limit=None, id=None, format="JSON"):
    opts = MagicMock()
    opts.provider = provider
    opts.collection = collection
    opts.mode = mode
    opts.output = output
    opts.limit = limit
    opts.id = id
    opts.format = format
    return opts


def test_analyze_mode_requires_output():
    """--mode analyze without --output should sys.exit with an error message."""
    opts = _make_opts(mode="analyze", output=None)

    provider_mock = _make_provider_mock()
    with patch("dlme_airflow.models.provider.Provider", provider_mock), \
         patch("dlme_airflow.drivers.register_drivers"):
        module = _load_bin_get()
        with pytest.raises(SystemExit) as exc_info:
            module.main(opts)

    assert "output" in str(exc_info.value).lower()


def test_analyze_mode_creates_timestamped_dir(tmp_path):
    """--mode analyze passes structured output_dir to catalog.read()."""
    df = pd.DataFrame([{"id": "record1"}])
    provider_mock = _make_provider_mock(df=df)
    opts = _make_opts(mode="analyze", output=str(tmp_path))

    with patch("dlme_airflow.models.provider.Provider", provider_mock), \
         patch("dlme_airflow.drivers.register_drivers"):
        module = _load_bin_get()
        module.main(opts)

    catalog_mock = provider_mock.return_value.get_collection.return_value.catalog
    call_kwargs = catalog_mock.read.call_args.kwargs
    assert call_kwargs["mode"] == "analyze"

    output_dir = Path(str(call_kwargs["output_dir"]))
    # structure: {tmp_path}/penn/penn_egyptian/{timestamp}
    assert output_dir.parts[-3] == "penn"
    assert output_dir.parts[-2] == "penn_egyptian"
    assert len(output_dir.parts[-1]) == 12, "timestamp should be YYYYMMDDHHMM"


def test_production_mode_calls_read_with_production(tmp_path):
    """Default production mode passes mode='production' and output_dir=None."""
    df = pd.DataFrame([{"id": "record1"}])
    provider_mock = _make_provider_mock(df=df)
    opts = _make_opts(mode="production", output=None)

    with patch("dlme_airflow.models.provider.Provider", provider_mock), \
         patch("dlme_airflow.drivers.register_drivers"):
        module = _load_bin_get()
        module.main(opts)

    catalog_mock = provider_mock.return_value.get_collection.return_value.catalog
    call_kwargs = catalog_mock.read.call_args.kwargs
    assert call_kwargs["mode"] == "production"
    assert call_kwargs["output_dir"] is None
