[![CircleCI](https://circleci.com/gh/sul-dlss/dlme-airflow/tree/main.svg?style=svg)](https://circleci.com/gh/sul-dlss/dlme-airflow/tree/main)
[![Maintainability](https://api.codeclimate.com/v1/badges/a20e808e66e0a20e30ad/maintainability)](https://codeclimate.com/github/sul-dlss/dlme-airflow/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/a20e808e66e0a20e30ad/test_coverage)](https://codeclimate.com/github/sul-dlss/dlme-airflow/test_coverage)

# dlme-airflow

This repository contains an [ETL] pipeline for the [Digital Library of the Middle East] (DLME) project. The pipeline is implemented in [Apache Airflow] and uses [Intake] to manage a catalog of IIIF, OAI-PMH and CSV data sources that are hosted at participating institutions. `dlme-airflow` collects data from these sources, transforms it with [dlme-transform], and stores the resulting data in an Amazon S3 bucket where it is loaded by the [dlme] [Spotlight] application. 

# Running Airflow Locally

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

### Run individual DAGs

Add a `.env` file to the root directory of the project and add your AWS credentials:
```
SKIP_REPORT=true
REDIS_PASSWORD=thisisasecurepassword
API_ENDPOINT=https://dlme-stage.stanford.edu/api/harvests
API_TOKEN=[GET API TOKEN FROM SERVER]
```

If you would like to be able to skip report generation and delivery in your development environment (which can be time consuming) you can add `SKIP_REPORT=true` to your `.env` as well. 

# Development

## Set-up

Create a Python virtual environment for dlme-airflow by first installing the [Poetry] dependency management and packaging tool. Use the [Poetry installer] script:

```
curl -sSL https://install.python-poetry.org | python3 -
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

### Other random Poetry tips

As an alternative to running `poetry shell`, you can prefix all project-specific python
commands from your default shell with `poetry run ...` (similar to `bundle exec ...` in
Ruby-land).

If you're running into dependency issues but believe your dependencies are specified correctly,
and you've run `poetry install` to make sure the environment is up to date, you can try:
* Updating to the latest version of `poetry`.  From Poetry 1.2.0 on, you should be able to call
`poetry self update`, but see the docs for details.
* Re-installing your env for the project.  You can see your installed environments with
`poetry env list`.  Then `poetry env remove <env id>`, then `poetry install` to install
  dependencies from a clean slate.
* To add a dependency, run `poetry add <dependency>` or `poetry add <dependency> --group dev`.

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

### Similar checks to CI, as one shell command
```sh
poetry run black --diff --check . &&
  poetry run mypy . &&
  poetry run flake8 &&
  poetry run yamllint catalogs/ &&
  PYTHONPATH=dlme_airflow poetry run pytest -s --pdb
```
* run black first because it's fast; remove the flags to just apply formatting
* typechecking next because it's also fast
* pytest: `-s` to show stdout, `--pdb` to drop to debugger on test failure, e.g. failed assertion

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

First you'll want to enter the poetry virtual environment:

```
$ poetry shell
```

and then run `bin/get` with a provider and collection as arguments (optionally you can write to a file with `--output`, or aboart the harvest early with `--limit`):

```
$ bin/get yale babylonian --limit 20
```

To harvest a single record for a known identifier:

```
$ bin/get yale babylonian --id some_known_id
```

### Manually run the report for a collection

This method requires setting the path to the ndjson output directory on execution, this path
can be wherever the `output-provider-collection.ndjson` file will be found.

Example:
```
METADATA_OUTPUT_PATH=$PWD/metadata poetry run bin/report aims aims > report.html
open report.html # to open in your browser
```

[BLK]: https://black.readthedocs.io/en/stable/index.html
[FLK8]: https://flake8.pycqa.org/en/latest/
[Poetry]: https://python-poetry.org/docs
[Poetry installer]: https://python-poetry.org/docs/#installation
[Apache Airflow]: https://airflow.apache.org/
[Intake]: https://intake.readthedocs.io/
[Digital Library of the Middle East]: https://dlmenetwork.org/library
[dlme]: https://github.com/sul-dlss/dlme/
[dlme-transform]: https://github.com/sul-dlss/dlme-transform
[Spotlight]: https://github.com/projectblacklight/spotlight
[ETL]: https://en.wikipedia.org/wiki/Extract,_transform,_load
[MYPY]: https://mypy.readthedocs.io/
[YAMLLINT]: https://yamllint.readthedocs.io/en/stable/index.html
