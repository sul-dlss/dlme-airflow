FROM apache/airflow:2.9.3-python3.12

USER root
RUN apt-get update && apt-get install -y gcc g++ git
# libjpeg-dev zlib1g-dev

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"

USER airflow

COPY dlme_airflow ./dlme_airflow
COPY requirements.txt ./

RUN uv pip install --no-cache "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt
