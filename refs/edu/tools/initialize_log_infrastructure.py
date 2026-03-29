import psycopg2
import json
import os

def initialize_db():
    # dbconf.json 로드 (스크립트와 같은 디렉토리)
    dbconf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dbconf.json')

    if not os.path.exists(dbconf_path):
        print(f"Error: dbconf.json not found at {dbconf_path}")
        return

    with open(dbconf_path, 'r') as f:
        config = json.load(f)

    # 호스트(Local) 접속 정보 사용
    conn_params = config.get('postgres_default')

    if not conn_params:
        print("Error: 'postgres_default' config not found in dbconf.json")
        return

    try:
        # DB 연결
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cur = conn.cursor()

        # 1. admin 스키마 생성
        print("Creating 'admin' schema if not exists...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS admin;")

        # 2. dbt_log 테이블 생성
        print("Creating 'admin.dbt_log' table if not exists...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS admin.dbt_log (
                dbt_invocation_id TEXT,
                model_name TEXT,
                status TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                execution_time_seconds NUMERIC,
                rows_affected INTEGER,
                variables JSONB,
                airflow_run_id TEXT,
                PRIMARY KEY (dbt_invocation_id, model_name)
            );
        """)

        print("Infrastructure initialized successfully using dbconf.json.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error initializing infrastructure: {e}")

if __name__ == "__main__":
    initialize_db()
