import json
import psycopg2
import os
import argparse

def get_db_config(db_config_path, db_key='postgres_local'):
    """DB 설정 파일을 읽어 DB 접속 정보를 반환합니다."""
    try:
        with open(db_config_path, 'r') as f:
            db_configs = json.load(f)
        db_config = db_configs.get(db_key)
        if not db_config:
            raise ValueError(f"오류: {db_key}에 대한 DB 설정이 {db_config_path}에 없습니다.")
        return db_config
    except FileNotFoundError:
        raise FileNotFoundError(f"오류: DB 설정 파일 {db_config_path}을 찾을 수 없습니다.")
    except json.JSONDecodeError:
        raise ValueError(f"오류: DB 설정 파일 {db_config_path}의 형식이 올바르지 않습니다.")

def execute_sql_file(db_config, sql_file_path):
    """
    SQL 파일의 내용을 읽어 데이터베이스에서 실행합니다.
    """
    conn = None
    try:
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"오류: SQL DDL 파일 {sql_file_path}을 찾을 수 없습니다.")

        with open(sql_file_path, 'r') as f:
            sql_query = f.read()

        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        cur.execute(sql_query)
        conn.commit()
        print(f"성공: {os.path.basename(sql_file_path)} 파일의 SQL이 성공적으로 실행되었습니다.")
        return True

    except FileNotFoundError as e:
        print(e)
        return False
    except Exception as e:
        print(f"오류: SQL 실행 중 오류 발생: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQL DDL 파일을 PostgreSQL 데이터베이스에 실행합니다.")
    parser.add_argument("db_config_path", help="dbconf.json 파일의 전체 경로")
    parser.add_argument("sql_file_path", help="실행할 DDL SQL 파일의 전체 경로")
    parser.add_argument("--db_key", default="postgres_local", help="dbconf.json 내 DB 설정 키 (기본값: postgres_local)")

    args = parser.parse_args()

    print(f"DB config path: {args.db_config_path}")
    print(f"SQL file path: {args.sql_file_path}")

    try:
        db_config = get_db_config(args.db_config_path, args.db_key)
    except (FileNotFoundError, ValueError) as e:
        print(e)
        exit(1)

    if execute_sql_file(db_config, args.sql_file_path):
        print("SQL 파일 실행 완료.")
    else:
        print("SQL 파일 실행 실패.")
