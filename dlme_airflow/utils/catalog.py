import intake
import os


def catalog_file():
    return os.getenv("CATALOG_SOURCE", "catalogs/catalog.yaml")


def fetch_catalog():
    return intake.open_catalog(catalog_file())


def catalog_for_provider(provider):
    try:
        return getattr(
            fetch_catalog(), provider
        )  # Raises Attribute error for missing provider
    except AttributeError:
        raise ValueError(f"Provider ({provider}) not found in catalog")


def get_working_csv(data_path):
    root_dir = os.path.dirname(os.path.abspath("metadata"))
    return os.path.join(root_dir, "metadata", data_path, "data.csv")
