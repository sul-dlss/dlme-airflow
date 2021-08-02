echo "Starting up airflow worker"

# Install boto and awscli for the seed dag
# python -m pip install awscli --user

# Intall python packages through req.txt and pip (if exists)
if [[ -f "/opt/airflow/requirements.txt" ]]; then
    echo "requirements.txt provided, installing it with pip"
    python -m pip install -r /opt/airflow/requirements.txt --user
fi

# Run the celery worker
airflow celery worker