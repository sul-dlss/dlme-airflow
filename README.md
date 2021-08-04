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

[BLK]: https://black.readthedocs.io/en/stable/index.html
[FLK8]: https://flake8.pycqa.org/en/latest/
