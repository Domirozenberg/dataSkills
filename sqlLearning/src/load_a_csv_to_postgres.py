import psycopg2
import csv
import sys
import os
import io
import pandas as pd  # Import pandas for data manipulation


def load_csv_to_postgres(csv_file_path, table_name, db_config):
    """
    Loads data from a CSV file into a PostgreSQL table.
    Removes extraneous double quotes from string values before loading.

    Args:
        csv_file_path (str): The path to the CSV file.
        table_name (str): The name of the table to load data into.
        db_config (dict): A dictionary containing database connection details
                          (e.g., 'host', 'database', 'user', 'password', 'port').
    """
    conn = None
    try:
        # Establish connection to PostgreSQL
        print(f"Connecting to the PostgreSQL database '{db_config['database']}'...")
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        print("Database connection established successfully.")

        # --- Create Schema (as you had it) ---
        create_schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS sources AUTHORIZATION postgres;
        """
        print(f"Attempting to create schema sources if it doesn't exist...")
        cur.execute(create_schema_sql)
        conn.commit()
        print(f"Schema 'sources' checked/created successfully.")

        # --- Step 1: Read CSV, Infer Schema (basic), and Clean Data ---
        print(f"Reading CSV file '{csv_file_path}' for schema inference and cleaning...")
        df = pd.read_csv(csv_file_path, dtype=str)  # Read all columns as string to handle quotes consistently

        # --- IMPORTANT: Data Cleaning (Removing unwanted quotes) ---
        # Iterate over all string/object columns and remove double quotes
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)  # Use regex=False for literal replacement

        print("Data cleaning (removing quotes) complete.")

        # Infer basic schema from DataFrame columns
        columns_with_types = []
        for col_name, dtype in df.dtypes.items():
            # For this basic inference, we'll still default to TEXT.
            # A more advanced inference would check for numeric/date types.
            columns_with_types.append(f'"{col_name}" TEXT')  # Quote column names for safety

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns_with_types)}
        );
        """
        print(f"Generated CREATE TABLE SQL: {create_table_sql}")
        print(f"Attempting to create table '{table_name}' if it doesn't exist...")
        cur.execute(create_table_sql)
        conn.commit()
        print(f"Table '{table_name}' checked/created successfully.")

        # --- Step 2: Load cleaned data using COPY_FROM ---
        # Convert DataFrame to CSV string in memory
        csv_buffer = io.StringIO()
        # Write to buffer without quoting values that don't need it, and without an index
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_MINIMAL)
        csv_buffer.seek(0)  # Rewind the buffer to the beginning

        print(f"Loading cleaned data into '{table_name}' using COPY_EXPERT...")
        # Use copy_expert with StringIO buffer
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", csv_buffer)
        conn.commit()
        print(f"Data from '{csv_file_path}' loaded into '{table_name}' successfully.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()  # Rollback changes on error
    except FileNotFoundError:
        print(f"Error: CSV file not found at '{csv_file_path}'")
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file '{csv_file_path}' is empty.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    # --- Configuration ---
    db_connection_details = {
        'host': 'localhost',
        'database': 'sqlLearningDB',
        'user': 'postgres',
        'password': 'postgres',
        'port': '5430'
    }

    # --- File/Path Handling ---
    if len(sys.argv) == 2:
        input_file_path = sys.argv[1]  # This is now the full path to the CSV
    else:
        # Prompt user for file path if not provided
        input_file_path = input("Enter the full path to the CSV file to upload: ")

    print(f"Processing file: '{input_file_path}'")

    # Extract table name from file path
    base_name = os.path.basename(input_file_path)
    file_name_without_extension = os.path.splitext(base_name)[0]

    # Target table name includes schema
    target_table_name = 'sources.' + file_name_without_extension

    print(f"Target table name: '{target_table_name}'")

    # Call the function to load the data
    load_csv_to_postgres(input_file_path, target_table_name, db_connection_details)