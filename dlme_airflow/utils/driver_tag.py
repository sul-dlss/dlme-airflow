import yaml


def fetch_driver(provider):
    """Returns a unique list of drivers from the catalog files.

    @param -- provider The registered provider name
    """
    drivers = []
    with open("/opt/airflow/catalogs/catalog.yaml", "r") as stream:
        catalog = yaml.safe_load(stream)
        if (
            catalog["sources"][provider].get("driver")
            == "intake.catalog.local.YAMLFileCatalog"
        ):
            path = catalog["sources"][provider]["args"].get("path")
            with open(path, "r") as collection_stream:
                collection_catalog = yaml.safe_load(collection_stream)
                for key in collection_catalog["sources"].keys():
                    drivers.append(collection_catalog["sources"][key].get("driver"))
        else:
            drivers.append(catalog["sources"][provider].get("driver"))

    return list(set(drivers))
