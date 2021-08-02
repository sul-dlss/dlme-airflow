echo "Starting up airflow init"

airflow db init

echo "Adding admin users, users list is empty!"

airflow users create -r Admin -u airflow -e airflow@stanford.edu -f Air -l Flow -p airflow
