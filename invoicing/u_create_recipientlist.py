import mysql.connector
from mysql.connector import errorcode

# Define your database connection details
config = {
  'user': 'root',
  'password': 'ngls@123',
  'host': 'localhost',
  'database': 'dnmasterlist'
}

# Establish the database connection
try:
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    # Create the table
    create_table_query = """
    CREATE TABLE recipientlist (
        id INT AUTO_INCREMENT PRIMARY KEY,
        c_code varchar(10),
        b_cat CHAR(1),
        company_name_full text,
        company_alias varchar(10),
        add1 varchar(255),
        add2 varchar(255),
        pic varchar(30),
        mr_ms varchar(10),
        tel varchar(255),
        email varchar(255)
    );
    """
    cursor.execute(create_table_query)
    
    # Commit the changes
    connection.commit()
    
    print("Database and table created successfully")
    
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
finally:
    # Close the connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
