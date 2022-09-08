from dlme_airflow.services.harvest_dag_generator import create_provider_dags
from dlme_airflow.drivers import register_drivers

register_drivers()

# Believe it or not airflow will only load this file during dag discovery
# if the file contains the words "dag" and "airflow", at least in Ariflow v2
# https://github.com/apache/airflow/blob/3b76e81bcc9010cfec4d41fe33f92a79020dbc5b/airflow/utils/file.py#L337-L357

create_provider_dags(module_name=__name__)
