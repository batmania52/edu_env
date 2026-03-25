FROM apache/airflow:2.10.4-python3.11
USER root
RUN apt-get update && apt-get install -y \
    gcc python3-dev libpq-dev git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.11.7" \
        "dbt-postgres==1.10.0"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt" \
    "apache-airflow-providers-celery" \
    "apache-airflow-providers-redis" \
    "astronomer-cosmos==1.10.0" \
    redis psycopg2-binary

ENTRYPOINT ["/entrypoint.sh"]