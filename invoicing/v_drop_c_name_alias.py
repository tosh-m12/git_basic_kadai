import mysql.connector

# Database connection details
config = {
  'user': 'root',
  'password': 'ngls@123',
  'host': 'localhost',
  'database': 'dnmasterlist',
}

# Establish a database connection
conn = mysql.connector.connect(**config)

# Create a cursor object
cursor = conn.cursor()

# SQL query to drop the table
drop_table_query = "DROP TABLE c_name_alias;"

try:
    # Execute the query
    cursor.execute(drop_table_query)
    print("Table bd_mexico dropped successfully.")
except mysql.connector.Error as err:
    print(f"Error: {err}")

# Close the cursor and connection
cursor.close()
conn.close()
