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
    CREATE TABLE statusmaster (
        id INT AUTO_INCREMENT PRIMARY KEY,
        company_name VARCHAR(255),
        b_cat CHAR(1),
        ac_term CHAR(6),
        mxbd_rcvd CHAR(8),
        bd_issued CHAR(8),
        1st_rep CHAR(8),
        affirm_yn CHAR(1),
        bd_finalized CHAR(8),
        fapiao_d CHAR(8),
        payment_d CHAR(8),
        sup_rem char(1),
        mx_rem char(1),
        fpio_f char(1),
        pymnt_rem char(1)
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
