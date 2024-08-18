import pandas as pd
import os
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime

# Adjust parameters here
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_user = os.getenv('MYSQL_USER', 'root')
mysql_password = urllib.parse.quote_plus(os.getenv('MYSQL_PASSWORD', 'ngls@123'))
mysql_database = os.getenv('MYSQL_DATABASE', 'dnmasterlist')

# List of tables to export
tables_to_export = ['bd_mexico', 'f_extract', 'dnmasterlist', 'recipientlist', 'customer_code', 'c_name_alias', 'full_bd', 'statusmaster', 'log']

def get_sqlalchemy_engine(host, user, password, database):
    return create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

def export_table_to_csv(engine, table):
    try:
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, engine)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        folder_path = os.path.join('log', table)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        file_path = os.path.join(folder_path, f"{timestamp}_{table}.csv")
        df.to_csv(file_path, index=False)
        print(f"Data exported successfully to CSV file {file_path}")
    except Exception as err:
        print(f"Error: {err}")

# Get SQLAlchemy engine
engine = get_sqlalchemy_engine(mysql_host, mysql_user, mysql_password, mysql_database)

# Export each table to a CSV file
for table in tables_to_export:
    export_table_to_csv(engine, table)
