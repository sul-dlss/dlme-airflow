from pathlib import Path

from jsonschema import validate
import pytest
import yaml


@pytest.fixture
def validate_schema(load_schema, load_catalog):
    def assert_valid_schema(catalog_file, schema_file):
        """Checks whether the given catalog matches the schema"""
        schema = load_schema(schema_file)
        catalog = load_catalog(catalog_file)
        return validate(catalog, schema)

    return assert_valid_schema


@pytest.fixture
def load_schema():
    def load_yaml_schema(filename):
        """Loads the given schema file"""
        schema_path = Path.cwd().joinpath("tests", "support", "schemas", filename)
        with open(schema_path) as schema_file:
            return yaml.safe_load(schema_file.read())

    return load_yaml_schema


@pytest.fixture
def load_catalog():
    def load_catalog_file(filename):
        """Load the catalog file"""
        catalog_path = Path.cwd().joinpath("catalogs", filename)
        with open(catalog_path) as catalog_file:
            return yaml.safe_load(catalog_file.read())

    return load_catalog_file


def test_validate_catalogs(validate_schema):
    schema = "schema.yaml"
    for catalog in Path.cwd().joinpath("catalogs").iterdir():
        if catalog.name != "catalog.yaml":
            validate_schema(catalog, schema)
