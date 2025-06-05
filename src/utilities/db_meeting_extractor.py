# src/utilities/db_meeting_extractor.py

import sqlite3
import json
import os

def load_schema(schema_file_path):
    try:
        with open(schema_file_path, 'r') as f:
            schema = json.load(f)
        return schema
    except FileNotFoundError:
        print(f"Error: Schema file not found at {schema_file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {schema_file_path}")
        return None

def create_tables_in_new_db(new_db_conn, schema):
    if not schema:
        print("Error: Schema not loaded. Cannot create tables.")
        return

    cursor = new_db_conn.cursor()
    for table_name, columns_def in schema.items():
        if table_name == "sqlite_sequence":
            continue

        column_definitions = []
        primary_keys = []
        for col_info in columns_def:
            col_name = col_info['name']
            col_type = col_info['type']
            sqlite_type = col_type.upper()
            if "BOOLEAN" in sqlite_type:
                sqlite_type = "INTEGER"

            col_def_str = f'"{col_name}" {sqlite_type}'
            if col_info.get('not_null', False):
                col_def_str += " NOT NULL"
            if col_info.get('primary_key', False):
                primary_keys.append(f'"{col_name}"')
            column_definitions.append(col_def_str)

        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ('
        create_table_sql += ", ".join(column_definitions)
        create_table_sql += ");"

        try:
            cursor.execute(create_table_sql)
        except sqlite3.Error as e:
            print(f"Error creating table {table_name}: {e}")
            print(f"Problematic SQL: {create_table_sql}")
    new_db_conn.commit()
    print("Tables created successfully in the new database.")

def extract_and_insert_data(source_db_conn, new_db_conn, schema, meeting_id):
    if not schema:
        print("Error: Schema not loaded. Cannot extract data.")
        return

    source_cursor = source_db_conn.cursor()
    new_cursor = new_db_conn.cursor()

    for table_name, columns_def in schema.items():
        if table_name in {"sqlite_sequence", "fetch_progress"}:
            print(f"Skipping table {table_name}")
            continue

        has_meeting_key_column = any(col['name'] == 'meeting_key' for col in columns_def)
        columns = [f'"{col["name"]}"' for col in columns_def]
        col_names_str = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))

        if has_meeting_key_column:
            select_sql = f'SELECT {col_names_str} FROM "{table_name}" WHERE "meeting_key" = ?;'
            source_cursor.execute(select_sql, (meeting_id,))
        elif any(col['name'] == 'session_key' for col in columns_def):
            source_cursor.execute('SELECT "session_key" FROM "sessions" WHERE "meeting_key" = ?', (meeting_id,))
            session_keys = [row[0] for row in source_cursor.fetchall()]
            if not session_keys:
                print(f"No sessions found for meeting {meeting_id}. Skipping table {table_name}.")
                continue
            session_key_placeholders = ", ".join(["?"] * len(session_keys))
            select_sql = f'SELECT {col_names_str} FROM "{table_name}" WHERE "session_key" IN ({session_key_placeholders});'
            source_cursor.execute(select_sql, session_keys)
        else:
            print(f"Skipping table {table_name} — no 'meeting_key' or 'session_key'")
            continue

        rows = source_cursor.fetchall()
        if rows:
            insert_sql = f'INSERT INTO "{table_name}" ({col_names_str}) VALUES ({placeholders});'
            try:
                new_cursor.executemany(insert_sql, rows)
                print(f"Inserted {len(rows)} rows into {table_name} for meeting {meeting_id}.")
            except sqlite3.Error as e:
                print(f"Error inserting into {table_name}: {e}")

    new_db_conn.commit()
    print("Data extraction and insertion complete.")

def main():
    # --- HARDCODED MEETING ID ---
    meeting_id = 1229  # Replace with the actual meeting ID you want to extract data for

    # --- Constants (can adjust if needed) ---
    source_db = "../../data/f1_data.db"
    new_db_name_template = "f1_meeting_{meeting_id}.db"
    schema_file = "../../data/db_schema.json"
    output_dir = "../../data/"

    # --- Path Handling ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_db_path = os.path.normpath(os.path.join(script_dir, source_db))
    schema_file_path = os.path.normpath(os.path.join(script_dir, schema_file))
    output_directory = os.path.normpath(os.path.join(script_dir, output_dir))
    os.makedirs(output_directory, exist_ok=True)

    new_db_filename = new_db_name_template.replace("{meeting_id}", str(meeting_id))
    new_db_path = os.path.join(output_directory, new_db_filename)

    print(f"Source DB: {source_db_path}")
    print(f"Schema file: {schema_file_path}")
    print(f"New DB will be created at: {new_db_path}")
    print(f"Extracting data for Meeting ID: {meeting_id}")

    schema = load_schema(schema_file_path)
    if not schema:
        return

    try:
        source_conn = sqlite3.connect(source_db_path)
        print(f"Connected to source DB.")
    except sqlite3.Error as e:
        print(f"Connection error: {e}")
        return

    if os.path.exists(new_db_path):
        print(f"Warning: {new_db_path} exists. Overwriting.")
        try:
            os.remove(new_db_path)
        except OSError as e:
            print(f"Failed to remove existing DB: {e}")
            source_conn.close()
            return

    try:
        new_conn = sqlite3.connect(new_db_path)
        print("Connected to new DB.")
    except sqlite3.Error as e:
        print(f"Connection error (new DB): {e}")
        source_conn.close()
        return

    create_tables_in_new_db(new_conn, schema)
    extract_and_insert_data(source_conn, new_conn, schema, meeting_id)

    source_conn.close()
    new_conn.close()
    print(f"Done. New database for meeting {meeting_id} at {new_db_path}")

if __name__ == "__main__":
    main()
