FROM apache/airflow:2.1.2-python3.8

COPY --chown=airflow:root ./dags/* /opt/airflow/dags