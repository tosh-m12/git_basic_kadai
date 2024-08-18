import pandas as pd
import mysql.connector
from mysql.connector import Error

################ ADJUST PARAMETERS HERE ################

# MySQL connection details
host = 'localhost'
user = 'root'
password = 'ngls@123'
database = 'dnmasterlist'

# Table to insert
table = 'c_name_alias'

# CSV file path to import
csv_file_path = f'{table}.csv'

########################################################

def upload_csv_to_mysql(csv_file_path, host, user, password, database, table):
    connection = None
    try:
        # Read CSV file
        df = pd.read_csv(csv_file_path, dtype=str)

        # Replace NaN values with None for DataFrame insertion
        df = df.where(pd.notnull(df), None)

        # Connect to MySQL
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Fetch existing table columns and their data types
            cursor.execute(f"DESCRIBE {table}")
            table_columns = cursor.fetchall()
            table_columns_dict = {row[0]: row[1] for row in table_columns}
            
            # Check if DataFrame columns exist in the table
            missing_columns = [col for col in df.columns if col not in table_columns_dict]
            if missing_columns:
                raise ValueError(f"Missing columns in the database table: {', '.join(missing_columns)}")

            # Ensure DataFrame columns match existing table columns
            df = df[[col for col in df.columns if col in table_columns_dict]]

            # Check data types
            for col in df.columns:
                if not all(isinstance(val, str) or val is None for val in df[col]):
                    raise TypeError(f"Data type mismatch for column: {col}")

            # Insert DataFrame into MySQL in batches
            batch_size = 1000
            for start in range(0, len(df), batch_size):
                end = start + batch_size
                batch = df.iloc[start:end]
                rows = [tuple(None if val is None else str(val) for val in row) for _, row in batch.iterrows()]
                placeholders = ', '.join(['%s'] * len(rows[0]))
                sql = f"INSERT INTO {table} ({', '.join(df.columns)}) VALUES ({placeholders})"
                cursor.executemany(sql, rows)
                connection.commit()

            print("Data uploaded successfully")

    except Error as e:
        print(f"Database error: {e}")
    except ValueError as ve:
        print(f"ValueError: {ve}")
    except TypeError as te:
        print(f"TypeError: {te}")
    finally:
        if connection is not None and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

upload_csv_to_mysql(csv_file_path, host, user, password, database, table)
