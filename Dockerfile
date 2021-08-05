FROM apache/airflow:2.1.2-python3.8

USER root
RUN apt-get -y update && apt-get -y install git
USER airflow

COPY --chown=airflow:root ./dlme_airflow /opt/dlme_airflow
