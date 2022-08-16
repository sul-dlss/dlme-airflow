[![CircleCI](https://circleci.com/gh/sul-dlss/dlme-airflow/tree/main.svg?style=svg)](https://circleci.com/gh/sul-dlss/dlme-airflow/tree/main)
[![Maintainability](https://api.codeclimate.com/v1/badges/a20e808e66e0a20e30ad/maintainability)](https://codeclimate.com/github/sul-dlss/dlme-airflow/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/a20e808e66e0a20e30ad/test_coverage)](https://codeclimate.com/github/sul-dlss/dlme-airflow/test_coverage)

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

### Allowing local Airflow to execute AWS resources

In order to trigger `dlme-transform` or `dlme-index` while running Airflow locally via `docker compose` your
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `DEV_ROLE_ARN`, `ECS_SECURITY_GROUP`, `ECS_SUBNET` must be set in your local environment and a [configured aws connection](https://github.com/sul-dlss/dlme-airflow/wiki/Amazon-Web-Services-(AWS)-connection-configuration). 

Add a `.env` file to the root directory of the project and add your AWS credentials:
```
AWS_ACCESS_KEY_ID={YOUR AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY={YOUR AWS_SECRET_ACCESS_KEY}
DEV_ROLE_ARN={The DEV_ROLE_ARN}
ECS_SECURITY_GROUP={Get value from shared configs}
ECS_SUBNET={Get value from shared configs}
```

# Fetching data for review from S3

DLME-airlfow writes the metadata harvested from providers to S3. It is possible to fetch the written data in CSV format using the [aws cli](https://github.com/sul-dlss/terraform-aws/wiki/AWS-DLSS-Dev-Env-Setup).

Note below that `metadata`, `metadata/bodleian`, and `metadata/bodleian/persian/data.csv` are the local paths to where the aws cli will copy the data. These paths will be created by the aws cli if they do not exist.

Fetch all current metadata:
```
aws s3 cp s3://dlme-metadata-dev/metadata metadata --recursive --profile development
```

Fetch all current metadata for a provider:
```
aws s3 cp s3://dlme-metadata-dev/metadata/bodleian metadata/bodleian --recursive --profile development
```

Fetch an individual collection file:
```
aws s3 cp s3://dlme-metadata-dev/metadata/bodleian/persian/data.csv metadata/bodleian/persian/data.csv --profile development
```

## Development

### Set-up

Create a Python virtual environment for dlme-airflow by first installing the  [Poetry] dependency management and packaging tool:

```
pip3 install poetry
```

Then you can bootstrap your environment:

```
poetry shell
```

and install the dependencies:

```
poetry install
```

Every time you open a new shell terminal you will want to run `poetry shell` again to ensure your you are using the dlme-airflow development environment. If you are using VSCode the [Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) should enable it automatically for you when you open a terminal window.

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

[BLK]: https://black.readthedocs.io/en/stable/index.html
[FLK8]: https://flake8.pycqa.org/en/latest/
[Poetry]: https://python-poetry.org
