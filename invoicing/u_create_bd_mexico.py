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
    CREATE TABLE bd_mexico (
        id INT AUTO_INCREMENT PRIMARY KEY,
        company_name text,
        mxbd_rcvd text,
        bill_from text,
        bill_to text,
        ac_term text,
        b_cat text,
        iv_value decimal(10,2),
        total_stock decimal(10,2),
        total_in decimal(10,2),
        eom_inventory decimal(10,2),
        total_out decimal(10,2),
        storage decimal(10,2),
        labelling decimal(10,2),
        immex_en decimal(10,2),
        immex_vex decimal(10,2),
        ins decimal(10,2), 
        sub_total decimal(10,2),
        16percent_vat decimal(10,2),
        grand_total decimal(10,2),
        cancel char(1),
        new char(1),
        rev varchar(3),
        f_extract_id int,
        dnmasterlist_id int
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
