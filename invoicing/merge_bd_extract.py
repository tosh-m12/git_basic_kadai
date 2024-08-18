import pandas as pd
import os
import logging
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

################ ADJUST PARAMETERS HERE ################

# MySQL connection details
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_user = os.getenv('MYSQL_USER', 'root')
mysql_password = os.getenv('MYSQL_PASSWORD', 'ngls@123')
mysql_database = os.getenv('MYSQL_DATABASE', 'dnmasterlist')
source_table1 = 'bd_mexico'
source_table2 = 'f_extract'
destination_table = 'full_bd'
csv_file_path = 'merged_data.csv'  # File path for the CSV export

# Specified values for the new columns in YYYYMMDD, YYYYMMDD format
bill_from_value = '20240601'
bill_to_value = '20240630'

########################################################

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create SQLAlchemy engine with URL-encoded password
encoded_password = quote_plus(mysql_password)
connection_string = f"mysql+mysqlconnector://{mysql_user}:{encoded_password}@{mysql_host}/{mysql_database}"
engine = create_engine(connection_string)

def extract_table_data(engine, table, columns, condition=None):
    try:
        columns_str = ', '.join(columns)
        query = f"SELECT {columns_str} FROM {table}"
        if condition:
            query += f" WHERE {condition}"
        df = pd.read_sql(query, engine)
        logging.info(f"Data extracted successfully from table {table}")
        logging.info(f"Columns in {table}: {df.columns.tolist()}")
        return df
    except SQLAlchemyError as err:
        logging.error(f"Error extracting data from {table}: {err}")
        return None

def merge_tables(df1, df2, on_column, bill_from, bill_to):
    try:
        logging.info(f"DataFrame df1 columns before merge: {df1.columns.tolist()}")
        logging.info(f"DataFrame df2 columns before merge: {df2.columns.tolist()}")

        # Remove duplicate entries based on the merging column
        df1 = df1.drop_duplicates(subset=on_column)
        df2 = df2.drop_duplicates(subset='id')

        # Merge the DataFrames on the specified column
        merged_df = pd.merge(df1, df2, left_on=on_column, right_on='id', how='inner')

        logging.info("Merging DataFrames...")

        # Add specified values for bill_from, bill_to
        merged_df['bill_from'] = bill_from
        merged_df['bill_to'] = bill_to

        # Calculate cn_vat6 as 6% of iv_amount
        if 'iv_amount' in merged_df.columns:
            merged_df['cn_vat6'] = merged_df['iv_amount'] * 0.06
        else:
            raise KeyError("Column 'iv_amount' not found in the merged DataFrame")

        # Keep only the necessary columns
        final_columns = [
            'c_code', 'company_name', 'b_cat', 'bill_from', 'bill_to', 'ac_term', 'iv_value',
            'total_stock', 'total_in', 'eom_inventory', 'total_out', 'storage',
            'labelling', 'immex_en', 'immex_vex', 'ins', 'sub_total', '16percent_vat',
            'grand_total', 'col_chrg', 'iv_amount', 'cn_vat6', on_column
        ]
        merged_df = merged_df[final_columns]

        logging.info(f"Data merged successfully on column {on_column}")
        logging.info(f"Merged DataFrame columns: {merged_df.columns.tolist()}")

        # Ensure no duplicates before insertion
        merged_df = merged_df.drop_duplicates()

        return merged_df
    except Exception as e:
        logging.error(f"Error in merging data: {e}")
        return None

def get_table_columns(engine, table):
    try:
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table)]
        logging.info(f"Retrieved columns for table {table}: {columns}")
        return columns
    except SQLAlchemyError as err:
        logging.error(f"Error retrieving columns for {table}: {err}")
        return []

def export_to_mysql_and_update(data, engine, table, source_table, merge_column):
    try:
        logging.info("Getting table columns for export...")
        table_columns = get_table_columns(engine, table)
        logging.info(f"Table columns for {table}: {table_columns}")
        table_columns_without_id = [col for col in table_columns if col != 'id']
        data_to_insert = data[table_columns_without_id]

        logging.info("Exporting data to MySQL...")
        # Export data to MySQL
        data_to_insert.to_sql(name=table, con=engine, if_exists='append', index=False)
        logging.info(f"Data exported successfully to MySQL table {table}")

        logging.info("Retrieving newly inserted IDs...")
        # Retrieve the newly inserted IDs
        with engine.connect() as connection:
            # Assuming `full_bd` has an auto-increment primary key `id`
            query = text(f"SELECT id FROM {table} ORDER BY id DESC LIMIT {len(data_to_insert)}")
            result = connection.execute(query)
            new_ids = [int(row[0]) for row in result.fetchall()]  # Ensure conversion to native Python int
            logging.info(f"Newly inserted IDs: {new_ids}")

        logging.info("Creating ID map...")
        # Create a DataFrame to map old IDs to new IDs
        id_map = pd.DataFrame(new_ids, columns=['full_bd_id'])
        id_map[merge_column] = data[merge_column].values

        logging.info("Updating bd_mexico with new full_bd_id...")
        # Update `bd_mexico` with the new `full_bd_id`
        with engine.connect() as connection:
            for _, row in id_map.iterrows():
                update_query = text(f"UPDATE {source_table} SET `full_bd_id` = :full_bd_id WHERE `f_extract_id` = :f_extract_id")
                logging.info(f"Executing update for f_extract_id={int(row[merge_column])} with full_bd_id={int(row['full_bd_id'])}")
                connection.execute(update_query, {"full_bd_id": int(row['full_bd_id']), "f_extract_id": int(row[merge_column])})  # Ensure conversion to native Python int

        logging.info(f"Updated `full_bd_id` in table {source_table}")
    except SQLAlchemyError as err:
        logging.error(f"Error: {err}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def export_to_csv(data, file_path):
    try:
        data.to_csv(file_path, index=False)
        logging.info(f"Data exported successfully to CSV file {file_path}")
    except Exception as e:
        logging.error(f"Error in exporting data to CSV: {e}")

# Define the columns to be extracted from each table
columns_bd_mexico = [
    'company_name', 'b_cat', 'ac_term', 'iv_value', 'total_stock', 'total_in', 
    'eom_inventory', 'total_out', 'storage', 'labelling', 'immex_en', 'immex_vex', 
    'ins', 'sub_total', '16percent_vat', 'grand_total', 'f_extract_id'
]

columns_f_extract = ['c_code', 'col_chrg', 'iv_amount', 'id']

# Extract data from bd_mexico where new is marked as '*'
logging.info("Extracting data from bd_mexico...")
data1 = extract_table_data(engine, source_table1, columns_bd_mexico, condition="`new` = '*'")
# Extract necessary columns from f_extract
logging.info("Extracting data from f_extract...")
data2 = extract_table_data(engine, source_table2, columns_f_extract)

if data1 is not None and data2 is not None:
    # Check the columns of both dataframes
    logging.info(f"Columns in bd_mexico: {data1.columns.tolist()}")
    logging.info(f"Columns in f_extract: {data2.columns.tolist()}")

    # Merge the data on 'f_extract_id'
    logging.info("Merging data...")
    merged_data = merge_tables(data1, data2, 'f_extract_id', bill_from_value, bill_to_value)

    if merged_data is not None:
        # Ensure the merged_data includes the merge_column for updating
        if 'f_extract_id' not in merged_data.columns:
            logging.error("Merged data does not include 'f_extract_id' column")
        else:
            # Export the merged data to a new MySQL table
            logging.info("Exporting merged data to MySQL and updating bd_mexico...")
            export_to_mysql_and_update(merged_data, engine, destination_table, source_table1, 'f_extract_id')

            # Export the merged data to a CSV file
            logging.info("Exporting merged data to CSV...")
            export_to_csv(merged_data, csv_file_path)
else:
    logging.error("Data extraction failed. Merging and exporting processes aborted.")
