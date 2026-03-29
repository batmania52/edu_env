import json
import psycopg2
import os

def create_table_from_sql(db_config_path, sql_file_path, db_key='postgres_default'):
    """
    SQL 파일에 정의된 DDL을 사용하여 PostgreSQL 테이블을 생성합니다.
    """
    # 1. DB 설정 로드
    try:
        if not os.path.exists(db_config_path):
            print(f"오류: DB 설정 파일 {db_config_path}을 찾을 수 없습니다. (경로 확인)")
            return False
        with open(db_config_path, 'r') as f:
            db_configs = json.load(f)
        db_config = db_configs.get(db_key)
        if not db_config:
            print(f"오류: {db_key}에 대한 DB 설정이 {db_config_path}에 없습니다.")
            return False
    except FileNotFoundError: # os.path.exists()로 미리 확인하므로 이 블록에 도달할 일은 거의 없음
        print(f"오류: DB 설정 파일 {db_config_path}을 찾을 수 없습니다. (FileNotFoundError)")
        return False
    except json.JSONDecodeError:
        print(f"오류: DB 설정 파일 {db_config_path}의 형식이 올바르지 않습니다.")
        return False

    # 2. DDL 쿼리 로드
    try:
        if not os.path.exists(sql_file_path):
            print(f"오류: SQL DDL 파일 {sql_file_path}을 찾을 수 없습니다. (경로 확인)")
            return False
        with open(sql_file_path, 'r') as f:
            ddl_query = f.read()
    except FileNotFoundError: # os.path.exists()로 미리 확인하므로 이 블록에 도달할 일은 거의 없음
        print(f"오류: SQL DDL 파일 {sql_file_path}을 찾을 수 없습니다. (FileNotFoundError)")
        return False

    conn = None
    try:
        # 3. PostgreSQL에 연결
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        cur = conn.cursor()

        # 4. DDL 쿼리 실행
        cur.execute(ddl_query)
        conn.commit()
        print(f"성공: {os.path.basename(sql_file_path)} 파일의 DDL이 성공적으로 실행되었습니다.")
        return True

    except Exception as e:
        print(f"오류: DDL 실행 중 오류 발생: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__)) # 절대 경로 사용

    db_config_path = os.path.join(current_dir, 'dbconf.json')
    sql_file_path = os.path.normpath(os.path.join(current_dir, "../ddls/edu_purchase_orders.sql"))

    print(f"Resolved DB config path: {db_config_path}")
    print(f"Resolved SQL file path: {sql_file_path}")

    if create_table_from_sql(db_config_path, sql_file_path):
        print("테이블 생성 스크립트 실행 완료.")
    else:
        print("테이블 생성 스크립트 실행 실패.")
