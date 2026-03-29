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
        print(f"DEBUG: Attempting to connect to DB: host={db_conf['host']}, port={db_conf['port']}, user={db_conf['user']}, database={db_conf['database']}")
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
    except psycopg2.errors.DuplicateTable:
        print(f"Warning: Table already exists. Skipping DDL execution.")
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

if __name__ == '__main__':
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    dbconf_path = os.path.join(_script_dir, 'dbconf.json')
    ddl_dir = os.path.normpath(os.path.join(_script_dir, '..', 'ddls'))

    try:
        with open(dbconf_path, 'r') as f:
            db_config = json.load(f)['postgres_default']
    except FileNotFoundError:
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_default' configuration not found in {dbconf_path}")
        exit(1)

    print(f"""
--- Executing DDLs from {ddl_dir} ---""")
    for filename in sorted(os.listdir(ddl_dir)):
        if filename.endswith('.sql'):
            ddl_file_path = os.path.join(ddl_dir, filename)
            print(f"Executing DDL from: {filename}")
            try:
                with open(ddl_file_path, 'r', encoding='utf-8') as f:
                    ddl_sql = f.read()
                execute_sql_query(ddl_sql, db_config, f"Successfully executed DDL for {filename}")
            except Exception as e:
                print(f"Error processing DDL file {filename}: {e}")
