import psycopg2
import csv
import sys
import os
import io  # Used to create an in-memory file-like object for COPY_FROM


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
        create_table_sql = f"""
        CREATE SCHEMA IF NOT EXISTS sources AUTHORIZATION postgres;
        """
        print(f"Attempting to create schema sqlLearningDB.sources if it doesn't exist...")
        print(create_table_sql)  # For debugging, see the generated SQL
        cur.execute(create_table_sql)
        conn.commit()
        print(f"Table sqlLearningDB.sources checked/created successfully.")

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

        # Read the header to get column names
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Get the first row as header

        # Basic type inference (very simplistic for demonstration)
        # For a robust solution, you'd iterate through data, check for numbers, dates etc.
        columns_with_types = []
        for col_name in header:
            # Default to TEXT, you'd add more sophisticated logic here
            columns_with_types.append(f'"{col_name}" TEXT')  # Quote column names in case they are keywords

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

        # --- Step 2: Load data using COPY_FROM ---
        # Open the CSV file and wrap it in an in-memory buffer for COPY_FROM
        # COPY_FROM is faster than row-by-row INSERTs for large datasets.
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            # We skip the header for COPY, as the table structure defines it.
            next(f)  # Skip header row if your CSV has one and your table doesn't map it.
            # Or, if you want to include header in table, remove this line and
            # ensure your CREATE TABLE statement is accurate and you
            # include HEADER option in COPY FROM

            # Use io.StringIO to create a file-like object from the CSV content
            # that COPY_FROM can read directly.
            # It's better to pass the file object directly if possible, but
            # sometimes buffering can help.
            # In most cases, `cur.copy_from(f, table_name, sep=',')` is sufficient
            # for text files and is more memory efficient for very large files.
            # Let's use the direct file object method for efficiency.

            # Re-open file or seek to beginning if you skipped header for table creation
            f.seek(0)
            # Skip header if CSV has one
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
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
