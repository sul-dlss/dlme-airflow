import os
from dlme_airflow.drivers import register_drivers

register_drivers()

# Set environment variable for tests
os.environ["METADATA_OUTPUT_PATH"] = "tests/data/ndjson"
