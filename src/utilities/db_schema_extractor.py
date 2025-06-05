import sqlite3
import json
import os

def get_db_schema(db_path):
    """
    Connects to a SQLite database and extracts schema information for all tables.

    Args:
        db_path (str): The path to the SQLite database file.

    Returns:
        dict: A dictionary where keys are table names and values are lists
              of dictionaries, each describing a column (name, type, pk, notnull, default_value).
              Returns None if the database file does not exist or an error occurs.
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return None

    schema = {}
    conn = None  # Initialize conn to None
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get a list of all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Iterate over each table
        for table_name_tuple in tables:
            table_name = table_name_tuple[0]
            schema[table_name] = []

            # Get column information for the current table
            # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            cursor.execute(f"PRAGMA table_info('{table_name}');")
            columns_info = cursor.fetchall()

            for col_info in columns_info:
                column_details = {
                    "name": col_info[1],
                    "type": col_info[2],
                    "not_null": bool(col_info[3]), # Convert 0/1 to False/True
                    "default_value": col_info[4],
                    "primary_key": bool(col_info[5]) # Convert 0/1 to False/True
                }
                schema[table_name].append(column_details)
        
        return schema

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None
    finally:
        # Ensure the database connection is closed
        if conn:
            conn.close()

if __name__ == "__main__":
    # The script is in src/utilities/
    # The database is in data/
    # Relative path from script to db: ../../data/f1_data.db
    base_dir = os.path.dirname(__file__)
    data_folder_path = os.path.join(base_dir, '..', '..', 'data')
    
    # Normalize the path to resolve '..' components
    data_folder_path = os.path.normpath(data_folder_path)

    db_file_path = os.path.join(data_folder_path, 'f1_data.db')
    output_json_path = os.path.join(data_folder_path, 'db_schema.json')


    print(f"Attempting to connect to database at: {db_file_path}")
    database_schema = get_db_schema(db_file_path)

    if database_schema:
        # Convert the schema dictionary to a JSON string
        # indent=4 makes the JSON output human-readable
        json_output = json.dumps(database_schema, indent=4)
        
        try:
            # Create the data directory if it doesn't exist
            os.makedirs(data_folder_path, exist_ok=True)
            
            with open(output_json_path, 'w') as f:
                f.write(json_output)
            print(f"\nDatabase schema successfully saved to: {output_json_path}")
        except IOError as e:
            print(f"Error writing JSON to file: {e}")
            print("\nDatabase Schema (JSON):") # Fallback to printing if file write fails
            print(json_output)
    else:
        print("Failed to extract database schema.")

