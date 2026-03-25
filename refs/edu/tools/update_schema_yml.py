import psycopg2
import json
import os
import yaml
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

def get_tables_info_from_db(db_config, schema_name, table_names):
    """지정된 스키마 내 여러 테이블의 컬럼 정보, Primary Key, 테이블/컬럼 주석을 DB에서 조회합니다."""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        tables_info = {}
        not_found_in_db = []

        # 스키마 내 모든 테이블 이름 조회 (존재 여부 검증용)
        cur.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE';
        """)
        all_existing_tables_in_schema = {row[0] for row in cur.fetchall()}

        for t_name in table_names:
            if t_name not in all_existing_tables_in_schema:
                not_found_in_db.append(t_name)
                continue # DB에 없는 테이블은 건너뛰고 다음 테이블 처리

            # 테이블 주석 조회
            cur.execute(f"""
                SELECT obj_description('{schema_name}.{t_name}'::regclass, 'pg_class');
            """)
            table_comment = cur.fetchone()[0] if cur.rowcount > 0 else f"Model for {t_name} in {schema_name} schema."

            # 컬럼 정보 및 주석 조회
            cur.execute(f"""
                SELECT a.attname AS column_name,
                       format_type(a.atttypid, a.atttypmod) AS data_type,
                       pg_catalog.col_description(a.attrelid, a.attnum) AS column_comment
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = '{schema_name}.{t_name}'::regclass
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attnum;
            """)
            columns_data = []
            for row in cur.fetchall():
                columns_data.append({
                    "name": row[0],
                    "description": row[2] if row[2] else f"Column {row[0]} with type {row[1]}",
                    "data_type": row[1]
                })

            # Primary Key 조회
            cur.execute(f"""
                SELECT kcu.column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = '{schema_name}'
                  AND tc.table_name = '{t_name}';
            """)
            pk_results = cur.fetchall()
            pk_columns = [row[0] for row in pk_results] if pk_results else []
            
            tables_info[t_name] = {
                "columns_data": columns_data,
                "pk_columns": pk_columns,
                "table_comment": table_comment
            }
        
        cur.close()
        conn.close()
        
        return tables_info, not_found_in_db # DB에 없는 테이블 목록도 함께 반환

    except Exception as e:
        raise Exception(f"오류: DB에서 테이블 정보를 가져오는 중 오류 발생: {e}")
    finally:
        if conn:
            conn.close()

def update_schema_yml(schema_file_path, schema_name, table_name, columns_data, pk_columns, table_comment, update_existing=False):
    """
    schema.yml 파일을 업데이트하거나 새로 생성합니다. (단일 테이블용)
    """
    schema_dict = {"version": 2, "models": []}
    
    # 처리 결과 추적용 변수
    model_status = {"name": table_name, "action": "added", "updated": False, "skipped": False}

    if os.path.exists(schema_file_path):
        with open(schema_file_path, 'r', encoding='utf-8') as f:
            try:
                existing_data = yaml.safe_load(f)
                if existing_data and "models" in existing_data:
                    schema_dict = existing_data
            except yaml.YAMLError as e:
                print(f"경고: 기존 schema.yml 파일 {schema_file_path} 파싱 오류: {e}. 새 파일로 시작합니다.")

    new_model_entry = {
        "name": table_name,
        "description": table_comment if table_comment else f"Model for {table_name} in {schema_name} schema.",
        "config": {
            "materialized": "incremental",
            "incremental_strategy": "append",
        },
        "columns": []
    }

    if pk_columns:
        new_model_entry["config"]["unique_key"] = pk_columns

    for col in columns_data:
        new_model_entry["columns"].append({
            "name": col["name"],
            "description": col["description"],
            "data_type": col["data_type"]
        })

    model_found = False
    for i, model in enumerate(schema_dict["models"]):
        if model.get("name") == table_name:
            if update_existing:
                schema_dict["models"][i] = new_model_entry
                model_found = True
                model_status["action"] = "updated"
                model_status["updated"] = True
                print(f"정보: 기존 모델 '{table_name}'을 업데이트했습니다.")
            else:
                print(f"정보: 모델 '{table_name}'이(가) 이미 존재하여 건너뜁니다. 업데이트하려면 '--update-existing' 플래그를 사용하세요.")
                model_found = True
                model_status["action"] = "skipped"
                model_status["skipped"] = True
            break
    
    if not model_found:
        schema_dict["models"].append(new_model_entry)
        print(f"정보: 새로운 모델 '{table_name}'을 추가했습니다.")
        model_status["action"] = "added"


    os.makedirs(os.path.dirname(schema_file_path), exist_ok=True)
    with open(schema_file_path, 'w', encoding='utf-8') as f:
        yaml.dump(schema_dict, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)

    print(f"성공: {schema_file_path} 파일이 성공적으로 업데이트되었습니다.")
    return model_status # 처리 결과 반환

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DB에서 테이블 정보를 조회하여 schema.yml 모델 정의를 생성 또는 업데이트합니다.")
    parser.add_argument("--schema", required=True, help="DB 스키마 이름")
    parser.add_argument("--tables", required=True, help="스키마 정의를 생성할 테이블 이름 (콤마로 구분)", type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument("--schema_file_path", required=True, help="schema.yml 파일의 전체 경로")
    parser.add_argument("--dbconf_path", required=True, help="dbconf.json 파일의 전체 경로")
    parser.add_argument("--db_key", default="postgres_local", help="dbconf.json 내 DB 설정 키 (기본값: postgres_local)")
    parser.add_argument("--update_existing", action="store_true", help="기존 모델이 존재할 경우 업데이트합니다. 없으면 추가합니다.")
    
    args = parser.parse_args()

    processed_models = [] # 성공적으로 처리된 모델 (추가 또는 업데이트)
    skipped_models = [] # 건너뛴 모델
    updated_models = [] # 업데이트된 모델
    not_found_in_db = [] # DB에 없는 테이블

    try:
        db_config = get_db_config(args.dbconf_path, args.db_key)
        
        # 모든 테이블 정보 한 번에 조회 (DB에 없는 테이블 목록도 함께 가져옴)
        tables_info, not_found_in_db_from_func = get_tables_info_from_db(db_config, args.schema, args.tables)
        not_found_in_db.extend(not_found_in_db_from_func) # 함수에서 반환된 목록 추가

        # 각 테이블 정보에 대해 schema.yml 업데이트 함수 호출
        for table_name in args.tables:
            # 이미 DB에 없는 것으로 확인된 테이블은 건너뜀
            if table_name in not_found_in_db_from_func:
                continue

            info = tables_info.get(table_name)
            if info:
                model_status = update_schema_yml(args.schema_file_path, args.schema, table_name, 
                                                 info["columns_data"], info["pk_columns"], info["table_comment"], 
                                                 args.update_existing)
                # processed_models 리스트에 추가 (added 또는 updated 된 경우만)
                if model_status["action"] == "added" or model_status["action"] == "updated":
                    processed_models.append(model_status["name"])

                if model_status["action"] == "updated":
                    updated_models.append(model_status["name"])
                elif model_status["action"] == "skipped":
                    skipped_models.append(model_status["name"])
            else:
                print(f"경고: 테이블 '{table_name}'에 대한 정보를 가져올 수 없습니다. 스키마 업데이트를 건너뜁니다.")


    except Exception as e:
        print(f"오류 발생: {e}")
        exit(1)
    finally:
        # 최종 결과 출력
        print("""
--- 스키마 업데이트 요약 ---""")
        print(f"1. 처리완료 모델 : {', '.join(processed_models) if processed_models else '없음'}")
        
        if not args.update_existing: # update_existing이 False일 때만 건너뛴 모델 출력
            print(f"2. 존재하는 모델(건너뜀) : {', '.join(skipped_models) if skipped_models else '없음'}")
        elif args.update_existing and updated_models: # update_existing이 True이고 업데이트된 모델이 있을 경우
            print(f"2. 존재하는 모델(업데이트됨) : {', '.join(updated_models)}")
        else: # update_existing이 True이지만 업데이트된 모델이 없는 경우 (기존 모델이 없어서 추가된 경우 등)
            print(f"2. 존재하는 모델 : 없음") # 이때는 업데이트된 모델이 없으므로 '없음'으로 표시
        
        print(f"3. DB에 없는 테이블 : {', '.join(not_found_in_db) if not_found_in_db else '없음'}")
        print("""---------------------------
""")