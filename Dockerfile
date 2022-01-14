FROM apache/airflow:2.1.2-python3.8

USER root
RUN apt-get -y update && \
    apt-get -y install \
        git \
        python3-dev \
        python3-pip \
        jq
USER airflow

RUN pip --no-cache-dir install --upgrade awscli

COPY --chown=airflow:root ./dlme_airflow /opt/dlme_airflow
COPY --chown=airflow:root ./catalogs /opt/catalogs
