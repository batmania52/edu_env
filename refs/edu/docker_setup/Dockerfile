FROM apache/airflow:2.8.1-python3.9

USER root

# Install system dependencies including git
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt and astronomer-cosmos
RUN pip install --no-cache-dir \
    dbt-core==1.7.4 \
    dbt-postgres==1.7.4 \
    astronomer-cosmos==1.3.2 \
    psycopg2-binary
