FROM apache/airflow:2.5.0-python3.10

ENV POETRY_VERSION=1.2.0

USER root
RUN apt-get -y update && apt-get -y install git jq
USER airflow

RUN pip install --upgrade pip
RUN pip --no-cache-dir install --upgrade awscli
RUN pip install "poetry==$POETRY_VERSION"

COPY --chown=airflow:root poetry.lock pyproject.toml /opt/airflow/
COPY --chown=airflow:root ./dlme_airflow /opt/airflow/dlme_airflow
COPY --chown=airflow:root ./catalogs /opt/airflow/catalogs

RUN poetry build --format=wheel
RUN pip install dist/*.whl
