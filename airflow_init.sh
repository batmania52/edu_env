mkdir -m 777 -p airflow/dags airflow/plugins airflow/logs airflow/config
cp -f refs/edu/airflow_assets/dags/*.py airflow/dags
cp -f refs/edu/airflow_assets/plugins/*.py airflow/plugins
cp -f refs/edu/dbconf.json airflow/
