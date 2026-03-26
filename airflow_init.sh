#mkdir -m 777 -p airflow/dags airflow/plugins airflow/logs airflow/config
cp -f refs/edu/airflow_assets/dags/* airflow/dags
cp -f refs/edu/airflow_assets/plugins/* airflow/plugins
cp -f refs/edu/dbconf.json airflow/
