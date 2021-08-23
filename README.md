# dlme-airflow
This is a new repository to capture the work related to the DLME ETL Pipeline and establish airflow

NOTE: This is a work-in-progress

# Getting Started

## Initialize local Docker infrastructure

```
docker compose up airflow-init
```

## Start local Docker Resources

```
docker compose up
```

## Adding new DAGs to the image

After creating a new DAG or editing an existing DAG, the resources must be restarted, and in the case of dlme-airflow, the base image rebuilt.

```
docker build . -f Dockerfile --tag suldlss/dlme-airflow:latest
```

then stop and restart all of the docker compose resources.

## Visit the Airflow dashboard

open your browser to `http://localhost:8080`

## Enable the DAGs you wish to run locally

## Run individual DAGs

## Development
### Set-up
Create a Python virtual environment by running `python3 -m venv {name-of-virtual-env}`
and then activating (on Linux/OSX) by `source {name-of-virtual-env}/bin/activate`.

From the root directory of this repository, install the dependencies by
`pip install -r requirements.txt`.

### Running Code Formatter and Linter
We are using [flake8][FLK8] for python code linting. To run [flake9][FLK8]
against the entire code repository, run `flake8 dlme_airflow` from the root
directory. To run the linter on a single file, run `flake8 dlme_airflow/path/to/file.py`.

To assist in passing the linter, use the [Black][BLK] opinionated code formatter
by running `black dlme_airflow/path/to/file.py`.

### Running Tests
To run the entire test suite from the root directory, `PYTHONPATH=dlme_airflow pytest`.
You can also run individual tests with `PYTHONPATH=dlme_airflow pytest tests/path/to/test.py`.

### Intake Catalogs
The `catalog.yaml` contains nested [catalogs](https://intake.readthedocs.io/en/latest/catalog.html#catalog-nesting)
for larger collections.

#### CSV Catalog
For CSV based collections, we are using the `csv` as driver and under the
*metadata* section, the `current_directory` should reference the location in the
Docker container where the [dlme-metadata](https://github.com/sul-dlss/dlme-metadata)
has been cloned from a previous DAG run at `/opt/airflow/metadata/`,

In the *args/csv_kwargs* section, make sure the dtype has the correct values
for the particular source and under *args/urlpath* list the URLs to download the
CSV files from the institution.

##### Example source entry:

```yaml
yale_babylonian:
  description: 'Yale Babylonian Collection'
  driver: csv
  metadata:
    current_directory: "/opt/airflow/metadata/yale/babylonian/data/"
  args:
    csv_kwargs:
      blocksize: null
      dtype:
        id: int64
        occurrence_id: object
        last_modified: object
        callnumber: object
        title: object
        .
        .
        .

```

##### Example source entry:

```yaml
exploring_egypt:
  description: "Exploring Egypt in the 19th Century"
  driver: iiif_json
  args:
    collection_url: https://iiif.bodleian.ox.ac.uk/iiif/collection/exploring-egypt
    dtype:
      description: object
      rendering: object
      thumbnail: object
      .
      .
  metadata:
    current_directory: "/opt/airflow/metadata/bodleian/exploring-egypt/data/"
    fields:
      description:
        path: "description"
      rendering:
        path: "rendering..@id"
        optional: true

```

#### IIIF Catalog
For the collections with IIIF JSON format, we created a new `IIIFJsonSource`
driver class that extends `intake.source.base.DataSource`. In catalogs sources
that use this driver, set the driver value to *iiif_json*.

Under the *args* section, the *collection_url* should be an URL to the JSON file
that contains links to other manfest files under it's *manifests* field. The
catalog *args* should also contain the *dtype* values for the data source.

In the *metadata* section, the *current_directory* value is the same as the
csv source above. In the *metadata/fields* section, the key is the name of the
column in the resulting Pandas DataFrame with the *path* value containing the
[JSON Path]() for extracting the value from the IIIF JSON. The *optional* value
is set to **true** for optional values (if this value is set to **false** or
not present, a logging error will result for values not found).

Exampel

[BLK]: https://black.readthedocs.io/en/stable/index.html
[FLK8]: https://flake8.pycqa.org/en/latest/
