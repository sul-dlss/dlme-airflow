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

## Enable the DAGs you wish to run locallay

## Run inidividual DAGs