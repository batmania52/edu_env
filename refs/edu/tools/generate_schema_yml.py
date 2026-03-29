import psycopg2
import json
import os
import yaml

def get_db_config():
    dbconf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dbconf.json')
    if not os.path.exists(dbconf_path):
        dbconf_path = '/home/batmania/projects/edu_env/refs/edu/dbconf.json'
    with open(dbconf_path, 'r') as f:
        configs = json.load(f)
        return configs.get('postgres_local') or configs.get('postgres_default')

def get_full_schema_info(cur, schema_name):
    cur.execute(f"""
        SELECT 
            t.table_name,
            obj_description((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass, 'pg_class') as table_comment
        FROM information_schema.tables t
        WHERE t.table_schema = '{schema_name}' AND t.table_type = 'BASE TABLE';
    """)
    tables = cur.fetchall()
    models = []
    for table_name, table_comment in tables:
        cur.execute(f"""
            SELECT a.attname AS column_name,
                   format_type(a.atttypid, a.atttypmod) AS data_type,
                   pg_catalog.col_description(a.attrelid, a.attnum) AS column_comment
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = '{schema_name}.{table_name}'::regclass
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
        cur.execute(f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = '{schema_name}'
              AND tc.table_name = '{table_name}';
        """)
        pk_columns = [row[0] for row in cur.fetchall()]
        model_entry = {
            "name": table_name,
            "description": table_comment if table_comment else f"Model for {table_name} in {schema_name} schema.",
            "config": {
                "materialized": "incremental",
                "incremental_strategy": "append",
            },
            "columns": columns_data
        }
        if pk_columns:
            model_entry["config"]["unique_key"] = pk_columns
        models.append(model_entry)
    return models

def generate_schema_yml(schema_name, output_path):
    config = get_db_config()
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        print(f"Fetching info for schema: {schema_name}...")
        models = get_full_schema_info(cur, schema_name)
        schema_dict = {"version": 2, "models": models}
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(schema_dict, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)
        print(f"Successfully generated {output_path} ({len(models)} models)")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error generating schema.yml for {schema_name}: {e}")

if __name__ == "__main__":
    base_dir = '/home/batmania/projects/edu_env'
    generate_schema_yml('stg', os.path.join(base_dir, 'dbt_projects/edu001/models/stg/schema.yml'))
    generate_schema_yml('marts', os.path.join(base_dir, 'dbt_projects/edu001/models/marts/schema.yml'))
