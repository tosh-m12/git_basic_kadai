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
    CREATE TABLE dnmasterlist (
        id INT AUTO_INCREMENT PRIMARY KEY,
        c_code varchar(10),
        b_cat CHAR(1),
        ac_term char(6),
        serial_num char(4),
        bill_from char(8),
        bill_to char(8),
        sto decimal(12,2),
        whs decimal(12,2),
        svc decimal(12,2),
        trs decimal(12,2),
        cst decimal(12,2),
        dsb decimal(12,2),
        sga decimal(12,2),
        oth decimal(12,2),
        cn_vat6 decimal(12,2),
        cn_vat9 decimal(12,2),
        rate_date char(8),
        rate decimal(10,4),
        sub_total_cny decimal(12,2),
        sub_total_usd decimal(12,2),
        iv_amount_cny decimal(12,2),
        iv_amount_usd decimal(12,2),
        d_issued datetime
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
