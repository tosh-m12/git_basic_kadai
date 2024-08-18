import mysql.connector
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_and_print_table_data(host, user, password, database, table):
    try:
        # Connect to the database
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        
        # Execute the query
        query = f"SELECT * FROM {table}"
        cursor.execute(query)
        
        # Fetch column names
        columns = [col[0] for col in cursor.description]
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Print each row's values
        for row in rows:
            row_dict = dict(zip(columns, row))
            for column, value in row_dict.items():
                print(f"{column}: {value}")
            print("-" * 50)  # Separator between rows
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
        logging.info(f"Data extracted and printed successfully from table {table}")
        
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")

# MySQL connection details
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_user = os.getenv('MYSQL_USER', 'root')
mysql_password = os.getenv('MYSQL_PASSWORD', 'ngls@123')
mysql_database = os.getenv('MYSQL_DATABASE', 'dnmasterlist')
source_table = 'full_bd'

# Extract and print data from the table
extract_and_print_table_data(mysql_host, mysql_user, mysql_password, mysql_database, source_table)
