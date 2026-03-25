import psycopg2
import json
import os
import yaml

def generate_schema_yml(schema_name, output_path):
    script_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_dir, '..', '..', '..'))
    dbconf_path = os.path.join(project_root, 'airflow', 'dbconf.json')
    with open(dbconf_path, 'r') as f:
        config = json.load(f)['postgres_default']
    
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # 스키마 내 테이블 목록 조회
        cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE';")
        tables = [row[0] for row in cur.fetchall()]

        models = []
        for table in tables:
            # 각 테이블의 컬럼 정보 조회
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table}' ORDER BY ordinal_position;")
            columns = [{"name": row[0], "description": f"Column {row[0]} with type {row[1]}"} for row in cur.fetchall()]
            
            models.append({
                "name": table,
                "description": f"Model for {table} in {schema_name} schema.",
                "columns": columns
            })

        schema_dict = {
            "version": 2,
            "models": models
        }

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("version: 2\n\nmodels:\n")
            for model in models:
                f.write(f"  - name: {model['name']}\n")
                f.write(f"    description: \"{model['description']}\"\n")
                f.write("    columns:\n")
                for col in model['columns']:
                    f.write(f"      - name: {col['name']}\n")
                    f.write(f"        description: \"{col['description']}\"\n")
        
        print(f"Successfully generated {output_path}")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error generating schema.yml for {schema_name}: {e}")

if __name__ == "__main__":
    # staging 및 marts 스키마에 대해 schema.yml 생성
    generate_schema_yml('stg', 'dbt_projects/edu001/models/stg/schema.yml')
    generate_schema_yml('marts', 'dbt_projects/edu001/models/marts/schema.yml')
