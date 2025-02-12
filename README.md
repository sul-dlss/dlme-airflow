[![CircleCI](https://circleci.com/gh/sul-dlss/dlme-airflow/tree/main.svg?style=svg)](https://circleci.com/gh/sul-dlss/dlme-airflow/tree/main)
[![codecov](https://codecov.io/gh/sul-dlss/dlme-airflow/graph/badge.svg?token=H53NPLQ7IC)](https://codecov.io/gh/sul-dlss/dlme-airflow)

# dlme-airflow

This repository contains an [ETL] pipeline for the [Digital Library of the Middle East] (DLME) project. The pipeline is implemented in [Apache Airflow] and uses [Intake] to manage a catalog of IIIF, OAI-PMH and CSV data sources that are hosted at participating institutions. `dlme-airflow` collects data from these sources, transforms it with [dlme-transform], and stores the resulting data on a shared filesystem where it is loaded by the [dlme] [Spotlight] application.

# Running Airflow Locally

## Environment

Add a `.env` file to the root directory of the project:

```
REDIS_PASSWORD=thisisasecurepassword
API_ENDPOINT=https://dlme-stage.stanford.edu/api/harvests
API_TOKEN=[GET API TOKEN FROM SERVER]
```

The `API_ENDPOINT` identifies a [dlme](https://github.com/sul-dlss/dlme) instance that is used for indexing harvested and transformed content. You will need to get the `API_TOKEN` from the server or from a DLME developer.

If you would like to be able to skip report generation and delivery in your development environment (which can be time consuming) you can add `SKIP_REPORT=true` to your `.env` as well.

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

# Development

## Set-up

1. Install `uv` for dependency management as described in [the uv docs](https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started).
2. Create a virtual environment:
```
uv venv
```

This will create the virtual environment at the default location of `.venv/`. `uv` automatically looks for a venv at this location when installing dependencies.

3. Activate the virtual environment:
```
source .venv/bin/activate
```

## Install dependencies
```
uv pip install -r requirements.txt
```

To add a dependency:
1. `uv pip install flask`
2. Add the dependency to `pyproject.toml`.
3. To re-generate the locked dependencies in `requirements.txt`:
```
uv pip compile pyproject.toml -o requirements.txt
```

Unlike poetry, uv's dependency resolution is not platform-agnostic. If we find we need to generate a requirements.txt for linux, we can use [uv's multi-platform resolution options](https://github.com/astral-sh/uv?tab=readme-ov-file#multi-platform-resolution).

## Upgrading dependencies
To upgrade Python dependencies:
```
uv pip compile pyproject.toml -o requirements.txt --upgrade
```

## Running Code Formatter and Linter
We are using [flake8][FLK8] for python code linting. To run [flake8][FLK8]
against the entire code repository, run `flake8 dlme_airflow` from the root
directory. To run the linter on a single file, run `flake8 dlme_airflow/path/to/file.py`.

To assist in passing the linter, use the [Black][BLK] opinionated code formatter
by running `black dlme_airflow/path/to/file.py` (this will immediately apply the
formatting it would suggest).

To help keep Intake catalog configs consistent in terms of basic style and formatting, we're using [yamllint][YAMLLINT].
To lint all of the configs, run `yamllint catalogs/`.  If you want to lint individual files, run e.g. `yamllint file1.yaml path/file2.yml`.
Note that if `yamllint` produces warnings but no errors, it'll return an exit code of 0, allowing the build to pass. If
it detects errors, it'll return an exit code of 1 and fail the build.

## Typechecking
We're using [mypy][MYPY] for type checking.  Type checking is opt-in, so you shouldn't have to specify
types for new code, and unknown types from dependencies will be ignored (via project configuration).  But if
expected type (especially for function or method return) is unclear or hard to reason about, consider adding
a type annotation instead of a comment.  This will likely be more concise and the type enforcement in CI can
help catch errors.  You can run it locally by calling `mypy .`.

## Running Tests
To run the entire test suite from the root directory, `PYTHONPATH=dlme_airflow pytest`.
You can also run individual tests with `PYTHONPATH=dlme_airflow pytest tests/path/to/test.py`.

## Misc useful development commands

### Debugging breakpoints
You can call `breakpoint()` in your code to set a breakpoint for dropping into the debugger.  Note from @jmartin-sul:
limited experience shows that this winds up in the expected context when used in code being tested, but can wind up in
an unexpected stack frame without access to the expected variable context when called from the test code itself.


## Intake Catalogs
The `catalog.yaml` contains nested [catalogs](https://intake.readthedocs.io/en/latest/catalog.html#catalog-nesting)
for larger collections.

### CSV Catalog
For CSV based collections, we are using the `csv` as driver and under the
*metadata* section, the `current_directory` should reference the location in the
Docker container where the [dlme-metadata](https://github.com/sul-dlss/dlme-metadata)
has been cloned from a previous DAG run at `/opt/airflow/metadata/`,

In the *args/csv_kwargs* section, make sure the dtype has the correct values
for the particular source and under *args/urlpath* list the URLs to download the
CSV files from the institution.

#### Example source entry:

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

### IIIF Catalog
For the collections with IIIF JSON format, we created a new `IiifJsonSource`
driver class that extends `intake.source.base.DataSource`. In catalogs sources
that use this driver, set the driver value to *iiif_json*.

Under the *args* section, the *collection_url* should be an URL to the JSON file
that contains links to other manifest files under it's *manifests* field. The
catalog *args* should also contain the *dtype* values for the data source.

In the *metadata* section, the *current_directory* value is the same as the
csv source above. In the *metadata/fields* section, the key is the name of the
column in the resulting Pandas DataFrame with the *path* value containing the
[JSON Path]() for extracting the value from the IIIF JSON. The *optional* value
is set to **true** for optional values (if this value is set to **false** or
not present, a logging error will result for values not found).

#### Example source entry:

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

### OAI-PMH Catalog

The `OaiXmlSource` Intake driver allows for harvesting of metadata from [OAI-PMH](https://www.openarchives.org/pmh/) services. The OAI-PMH protocol allows for multiple types of XML based metadata to be made available. DLME currently understands Dublin Core, MODS, and MARCXML. By default the driver will only harvest records that have been updated or created since the last successful harvest. Incremental harvesting can be turned off with the *full_harvest* option (see below).

There are several settings in the *args* section that need to be configured for the driver to work properly.

- **collection_url**: the URL for a OAI-PMH endpoint
- **metadata_prefix**: what type of metadata should be harvested: `oai_dc`, `mods`, or `marc21`
- **set**: indicate which OAI-PMH set to harvest from (optional)
- **wait**: number of seconds to wait between requests to the server (optional)
- **fields**: a mapping of property names and XPath locations where to find the value in the XML
- **full_harvest**: when set to a value this will turn off incremental harvesting and fetch all records on each run

#### Example source entry:

```yaml
voice_of_america:
  description: "Voice of America radio recordings"
  driver: oai_xml
  args:
    collection_url: https://cdm15795.contentdm.oclc.org/oai/oai.php
    metadata_prefix: oai_dc
    set: p15795coll40
    wait: 2
  metadata:
    data_path: auc/voice_of_america
    config: auc
    fields:
      id:
        path: "//header:identifier"
        namespace:
          header: "http://www.openarchives.org/OAI/2.0/"
        optional: true
```


### Getting Data

Sometimes it can be useful to be able to fetch data from a provider on the command line. This can be useful when adding or modifying a catalog entry, developing a driver, or when working with the data that is collected. To aid in that the `bin/get` utility will fetch data from a provider/collection and output the collected CSV to stdout or to a file.

You use `uv` to run `bin/get` with a provider and collection as arguments (optionally you can write to a file with `--output`, or abort the harvest early with `--limit`):

```
$ uv run bin/get yale babylonian --limit 20
```

To harvest a single record for a known identifier:

```
$ uv run bin/get yale babylonian --id some_known_id
```

By default `bin/get` will output json format. A csv file may be useful for checking available fields and coverage. To harvest as csv, use `--format`:

```
$ uv run bin/get yale babylonian --format csv
```

### Manually run the report for a collection

This method requires setting the path to the ndjson output directory on execution, this path
can be wherever the `output-provider-collection.ndjson` file will be found.

Example:
```
METADATA_OUTPUT_PATH=$PWD/metadata uv run bin/report aims aims > report.html
open report.html # to open in your browser
```

[BLK]: https://black.readthedocs.io/en/stable/index.html
[FLK8]: https://flake8.pycqa.org/en/latest/
[Apache Airflow]: https://airflow.apache.org/
[Intake]: https://intake.readthedocs.io/
[Digital Library of the Middle East]: https://dlmenetwork.org/library
[dlme]: https://github.com/sul-dlss/dlme/
[dlme-transform]: https://github.com/sul-dlss/dlme-transform
[Spotlight]: https://github.com/projectblacklight/spotlight
[ETL]: https://en.wikipedia.org/wiki/Extract,_transform,_load
[MYPY]: https://mypy.readthedocs.io/
[YAMLLINT]: https://yamllint.readthedocs.io/en/stable/index.html

### Validate a traject config file

There is a utility for validating traject mappings when the input file is json. It will compare
all fields in the input data against all fields in the traject config and write a report listing
unmapped fields and fields the traject config attempts to map that do not exist in the input data.
Call it with the data path used to invoke the traject transformation.

Example:
```
uv run bin/validate-traject qnl
```

