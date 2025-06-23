import psycopg2
import csv
import sys
import io
import pandas as pd  # Import pandas for data manipulation
import os


def find_csv_files_in_directory(directory_path):
    """
    Finds all JSON files in the specified directory.

    Args:
        directory_path (str): The path to the directory to scan.

    Returns:
        list: A list of full paths to JSON files.
    """
    csv_files = []
    if not os.path.isdir(directory_path):
        print(f"Error: Directory not found at '{directory_path}'")
        return []

    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            csv_files.append(os.path.join(directory_path, filename))
    return csv_files

def open_connection_and_create_schema(db_config):
    """
    Loads data from a CSV file into a PostgreSQL table.

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

        # --- Step 1: Infer schema and create table (example - you'll need to adapt this) ---
        # In a real scenario, you would robustly infer types or have a predefined schema.
        # For simplicity, this example assumes the CSV structure.
        # You would typically read the header and a few rows to determine data types.
        # For now, let's assume a simple table for demonstration.
        # YOU WILL NEED TO ADAPT THIS `CREATE TABLE` STATEMENT
        # BASED ON YOUR ACTUAL CSV COLUMNS AND DESIRED DATA TYPES!

        # -- Create a schema
        create_schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS sources AUTHORIZATION postgres;
        """
        print(f"Attempting to create schema sqlLearningDB.sources if it doesn't exist...")
        cur.execute(create_schema_sql)
        conn.commit()
        print(f"Schema 'sources' checked/created successfully.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()  # Rollback changes on error
    except FileNotFoundError:
        print(f"Error: CSV file not found at '{csv_file_path}'")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def load_csv_to_postgres(csv_file_path, table_name, db_config):
    conn = None
    try:
        # Establish connection to PostgreSQL
        print(f"Connecting to the PostgreSQL database '{db_config['database']}'...")
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        print("Database connection established successfully.")

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
        print(f"Attempting to create table '{table_name}' if it doesn't exist...")
        print(create_table_sql)  # For debugging, see the generated SQL
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
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    # --- Configuration ---
    # Replace with your actual database connection details
    db_connection_details = {
        'host': 'localhost',  # Or your PostgreSQL server IP/hostname
        'database': 'sqlLearningDB',
        'user': 'postgres',
        'password': 'postgres',
        'port': '5430'  # Default PostgreSQL port
    }

    if len(sys.argv) == 2:
        # Command-line argument for directory provided
        input_directory = sys.argv[1]
    else:
        # Prompt user for directory name
        input_directory = input("Enter the path to the directory containing your csv files: ")

    open_connection_and_create_schema(db_connection_details)
    csv_files_files_to_convert = find_csv_files_in_directory(input_directory)
    for csv_file in csv_files_files_to_convert:
        # Create a CSV filename by changing the extension
        base_name = os.path.basename(csv_file)
        file_name_without_extension = os.path.splitext(base_name)[0]

        csv_file_path = input_directory+'/'+csv_file  # e.g., 'athletes.csv'
        target_table_name = 'sources.' + file_name_without_extension  # e.g., 'athletes'

        print(f"File is '{csv_file_path}'")
        print(f"File is '{target_table_name}'")

        load_csv_to_postgres(csv_file_path, target_table_name, db_connection_details)
