import pandas as pd
import os
from sqlalchemy import create_engine
import urllib.parse

################ ADJUST PARAMETERS HERE ################

# MySQL connection details
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_user = os.getenv('MYSQL_USER', 'root')
mysql_password = urllib.parse.quote_plus(os.getenv('MYSQL_PASSWORD', 'ngls@123'))
mysql_database = os.getenv('MYSQL_DATABASE', 'dnmasterlist')

# Table to export
source_table = 'customer_code'

# CSV file path
csv_file_path = f'{source_table}.csv'

########################################################

def get_sqlalchemy_engine(host, user, password, database):
    return create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

def export_table_to_csv(engine, table, file_path):
    try:
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, engine)
        df.to_csv(file_path, index=False)
        print(f"Data exported successfully to CSV file {file_path}")
    except Exception as err:
        print(f"Error: {err}")

# Get SQLAlchemy engine
engine = get_sqlalchemy_engine(mysql_host, mysql_user, mysql_password, mysql_database)

# Export table to CSV
export_table_to_csv(engine, source_table, csv_file_path)
