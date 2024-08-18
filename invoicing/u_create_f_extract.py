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
    CREATE TABLE f_extract (
        id INT AUTO_INCREMENT PRIMARY KEY,
        c_code varchar(10),
        company_name varchar(255),
        b_cat CHAR(1),
        bill_from text,
        bill_to text,
        ac_term text,
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
        col_chrg decimal(10,2),
        iv_amount decimal(10,2),
        cn_vat6 decimal(10,2)
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
