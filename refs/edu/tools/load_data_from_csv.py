import json
import psycopg2
import psycopg2.extras
import csv
import os

def table_exists(conn, schema_name, table_name):
    """
    Checks if a table exists in the specified schema.
    """
    # ADDED DEBUG PRINT
    print(f"DEBUG(table_exists): Attempting to connect to DB: host={conn.dsn.split('host=')[1].split(' ')[0]}, port={conn.dsn.split('port=')[1].split(' ')[0]}, user={conn.dsn.split('user=')[1].split(' ')[0]}, database={conn.dsn.split('dbname=')[1].split(' ')[0]}")
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            );
        """, (schema_name, table_name))
        return cur.fetchone()[0]

def load_data_from_csv(file_path, table_name, db_conf, schema_name='edu'):
    """
    Loads data from a CSV file into a specified PostgreSQL table.

    Args:
        file_path (str): The path to the CSV file.
        table_name (str): The name of the table to load data into.
        db_conf (dict): Database connection configuration.
        schema_name (str): The schema name where the table resides.
    """
    conn = None
    cur = None
    try:
        print(f"DEBUG(load_data_from_csv): Attempting to connect to DB: host={db_conf['host']}, port={db_conf['port']}, user={db_conf['user']}, database={db_conf['database']}") # ADDED DEBUG PRINT
        conn = psycopg2.connect(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database=db_conf['database']
        )
        cur = conn.cursor()

        # Check if table exists, if not, try to create it using DDL
        if not table_exists(conn, schema_name, table_name):
            print(f"Table {schema_name}.{table_name} does not exist. Attempting to create from DDL...")
            ddl_file_path = os.path.join(
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), # Project root
                'refs', 'edu', 'ddls', f"{schema_name}_{table_name}.sql"
            )
            try:
                with open(ddl_file_path, 'r', encoding='utf-8') as f:
                    ddl_sql = f.read()
                cur.execute(ddl_sql)
                conn.commit()
                print(f"Successfully created table {schema_name}.{table_name} using DDL from {ddl_file_path}")
            except FileNotFoundError:
                print(f"Warning: DDL file not found for {schema_name}.{table_name} at {ddl_file_path}. Cannot create table automatically.")
                return # Cannot proceed without table
            except Exception as ddl_e:
                print(f"Error executing DDL for {schema_name}.{table_name}: {ddl_e}. Cannot create table automatically.")
                conn.rollback()
                return # Cannot proceed without table

        # Truncate table before loading new data to ensure a clean state for training
        print(f"Truncating table {schema_name}.{table_name}...")
        cur.execute(f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY CASCADE;")
        conn.commit()

        with open(file_path, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)  # Read header row
            
            # Construct INSERT statement
            columns = ', '.join([f'"{col}"' for col in header]) # Quote column names to handle special chars

            insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES %s"

            # Prepare data for insertion
            data_to_insert = [tuple(row) for row in csv_reader]
            
            if not data_to_insert:
                print(f"No data found in {file_path} to load into {schema_name}.{table_name}")
                return

            # Execute batch insert
            psycopg2.extras.execute_values(cur, insert_query, data_to_insert)
            conn.commit()
        
        print(f"Successfully loaded data from {file_path} into {schema_name}.{table_name}")

    except Exception as e:
        print(f"Error loading data into {schema_name}.{table_name} from {file_path}: {e}")
        if conn:
            conn.rollback() # Rollback in case of error
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    dbconf_path = os.path.join(project_root, 'airflow', 'dbconf.json')
    input_data_dir = os.path.join(project_root, 'refs', 'edu', 'datas')
    
    try:
        with open(dbconf_path, 'r') as f:
            db_config = json.load(f)['postgres_default']
            db_config['schema'] = 'edu' # Explicitly set schema to 'edu'
    except FileNotFoundError:
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_default' configuration not found in {dbconf_path}")
        exit(1)

    # Iterate through CSV files in refs/datas
    for filename in os.listdir(input_data_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(input_data_dir, filename)
            # Assuming filename format is schema_tablename.csv, e.g., edu_test001.csv
            parts = filename.replace('.csv', '').split('_')
            if len(parts) >= 2 and parts[0] == db_config['schema']: # Ensure schema matches 'edu'
                table_name = '_'.join(parts[1:])
                load_data_from_csv(file_path, table_name, db_config, db_config['schema'])
            else:
                print(f"Skipping {filename}: Does not match expected format (edu_tablename.csv)")