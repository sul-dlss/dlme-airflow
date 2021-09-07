import intake
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators and utils required from airflow
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

# Our stuff
from task_groups.validate_dlme_metadata import build_validate_metadata_taskgroup
from task_groups.iiif_harvest import build_iiif_harvester_taskgroup
from task_groups.detect_metadata_changes import build_detect_metadata_changes_taskgroup

from drivers.iiif_json import IIIfJsonSource

intake.source.register_driver("iiif_json", IIIfJsonSource)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [Variable.get("data_manager_email")],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    'catchup': False,
}

provider = 'bodleian'

with DAG(
    f'harvest.{provider}',
    default_args=default_args,
    description=f'IIIF harvester for the {provider} collection(s)',
    schedule_interval='@once',
    start_date=datetime(2021, 8, 26),
    tags=['metadata', 'iiif', provider],
    catchup=False
) as dag:

    validate_dlme_metadata = build_validate_metadata_taskgroup(dag=dag)

    harvester = build_iiif_harvester_taskgroup(provider, dag)

    # A dummy operator is required as a transition point between task groups
    harvest_complete = DummyOperator(task_id='harvest_complete', trigger_rule='none_failed', dag=dag)

    collect_metadata_changes = build_detect_metadata_changes_taskgroup(provider, dag)

    validate_dlme_metadata >> harvester >> harvest_complete >> collect_metadata_changes
