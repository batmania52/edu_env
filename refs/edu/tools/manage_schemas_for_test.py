import json
import psycopg2
import os

def execute_sql_query(sql_query, db_conf, success_message="SQL query executed successfully."):
    """
    Executes a given SQL query against the PostgreSQL database.

    Args:
        sql_query (str): The SQL query to execute.
        db_conf (dict): Database connection configuration.
        success_message (str): Message to print on successful execution.
    """
    conn = None
    cur = None
    try:
        print(f"DEBUG(execute_sql): Attempting to connect to DB: host={db_conf['host']}, port={db_conf['port']}, user={db_conf['user']}, database={db_conf['database']}") # ADDED DEBUG PRINT
        conn = psycopg2.connect(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database=db_conf['database']
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
        print(success_message)
    except psycopg2.errors.DuplicateSchema:
        print(f"Warning: Schema already exists. Skipping creation/rename if applicable.")
        if conn:
            conn.rollback()
    except psycopg2.errors.InvalidSchemaName:
        print(f"Warning: Schema does not exist. Skipping rename if applicable.")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def schema_exists(db_conf, schema_name):
    conn = None
    cur = None
    try:
        print(f"DEBUG(schema_exists): Attempting to connect to DB: host={db_conf['host']}, port={db_conf['port']}, user={db_conf['user']}, database={db_conf['database']}") # ADDED DEBUG PRINT
        conn = psycopg2.connect(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database=db_conf['database']
        )
        cur = conn.cursor()
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '{schema_name}');")
        return cur.fetchone()[0]
    except Exception as e:
        print(f"Error checking if schema '{schema_name}' exists: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    dbconf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dbconf.json')

    try:
        with open(dbconf_path, 'r') as f:
            db_config = json.load(f)['postgres_default']
            db_config['schema'] = 'edu' # Base schema for dbt models
    except FileNotFoundError:
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_default' configuration not found in {dbconf_path}")
        exit(1)

    # --- Manage 'edu' schema ---
    if schema_exists(db_config, 'edu'):
        print("Renaming schema 'edu' to 'edu_bak'...")
        rename_schema_sql = "ALTER SCHEMA edu RENAME TO edu_bak;"
        execute_sql_query(rename_schema_sql, db_config, "Schema 'edu' renamed to 'edu_bak'.")
    else:
        print("Schema 'edu' does not exist. No rename needed.")

    if schema_exists(db_config, 'edu_bak'):
        print("Dropping old 'edu_bak' schema...")
        drop_bak_schema_sql = "DROP SCHEMA edu_bak CASCADE;"
        execute_sql_query(drop_bak_schema_sql, db_config, "Old 'edu_bak' schema dropped.")

    if not schema_exists(db_config, 'edu'):
        print("Creating fresh 'edu' schema for test...")
        create_edu_schema_sql = "CREATE SCHEMA edu;" # ORIGINAL
        execute_sql_query(create_edu_schema_sql, db_config, "Fresh 'edu' schema created.")
    else:
        print("'edu' schema already exists (should not happen if rename/drop worked). Proceeding with existing 'edu'.")

    # --- Manage 'stg' schema ---
    if schema_exists(db_config, 'stg'):
        print("Renaming schema 'stg' to 'stg_bak'...")
        rename_schema_sql = "ALTER SCHEMA stg RENAME TO stg_bak;"
        execute_sql_query(rename_schema_sql, db_config, "Schema 'stg' renamed to 'stg_bak'.")
    else:
        print("Schema 'stg' does not exist. No rename needed.")

    if schema_exists(db_config, 'stg_bak'):
        print("Dropping old 'stg_bak' schema...")
        drop_bak_schema_sql = "DROP SCHEMA stg_bak CASCADE;"
        execute_sql_query(drop_bak_schema_sql, db_config, "Old 'stg_bak' schema dropped.")

    if not schema_exists(db_config, 'stg'):
        print("Creating fresh 'stg' schema for test...")
        create_stg_schema_sql = "CREATE SCHEMA stg;" # ORIGINAL
        execute_sql_query(create_stg_schema_sql, db_config, "Fresh 'stg' schema created.")
    else:
        print("'stg' schema already exists (should not happen if rename/drop worked). Proceeding with existing 'stg'.")

    # --- Manage 'marts' schema ---
    if schema_exists(db_config, 'marts'):
        print("Renaming schema 'marts' to 'marts_bak'...")
        rename_schema_sql = "ALTER SCHEMA marts RENAME TO marts_bak;"
        execute_sql_query(rename_schema_sql, db_config, "Schema 'marts' renamed to 'marts_bak'.")
    else:
        print("Schema 'marts' does not exist. No rename needed.")

    if schema_exists(db_config, 'marts_bak'):
        print("Dropping old 'marts_bak' schema...")
        drop_bak_schema_sql = "DROP SCHEMA marts_bak CASCADE;"
        execute_sql_query(drop_bak_schema_sql, db_config, "Old 'marts_bak' schema dropped.")

    if not schema_exists(db_config, 'marts'):
        print("Creating fresh 'marts' schema for test...")
        create_marts_schema_sql = "CREATE SCHEMA marts;" # ORIGINAL
        execute_sql_query(create_marts_schema_sql, db_config, "Fresh 'marts' schema created.")
    else:
        print("'marts' schema already exists (should not happen if rename/drop worked). Proceeding with existing 'marts'.")
