import pandas as pd
import os
from sqlalchemy import create_engine, String, DECIMAL, Integer, text
import urllib.parse

################ ADJUST PARAMETERS HERE ################

# MySQL connection details
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_user = os.getenv('MYSQL_USER', 'root')
mysql_password = urllib.parse.quote_plus(os.getenv('MYSQL_PASSWORD', 'ngls@123'))
mysql_database = os.getenv('MYSQL_DATABASE', 'dnmasterlist')

# Target tables
source_table = 'bd_mexico'
alias_table = 'c_name_alias'
destination_table = 'f_extract'

########################################################

def get_sqlalchemy_engine(host, user, password, database):
    return create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

def extract_data_from_mysql(engine, table):
    try:
        query = f"SELECT * FROM {table} WHERE `new` = '*'"
        df = pd.read_sql(query, engine)
        print(f"Data extracted from {table}")
        return df
    except Exception as err:
        print(f"Error extracting data from {table}: {err}")
        return None

def extract_customer_code(engine, table):
    try:
        query = f"SELECT company_name, b_cat, c_code FROM {table}"
        df = pd.read_sql(query, engine)
        print(f"Customer codes extracted from {table}")
        return df
    except Exception as err:
        print(f"Error extracting customer codes from {table}: {err}")
        return None

def calculate_charges(df):
    try:
        df['col_chrg'] = (df['grand_total'] * 0.065).round(2)
        df['iv_amount'] = (df['grand_total'] + df['col_chrg']).round(2)
        df['cn_vat6'] = (df['iv_amount'] * 0.06).round(2)
        print("Charges calculated")
        return df
    except Exception as e:
        print(f"Error in calculating charges: {e}")
        return None

def merge_data(main_df, alias_df):
    try:
        # Attempt to merge on both company_name and b_cat
        merged_df = pd.merge(main_df, alias_df, on=['company_name', 'b_cat'], how='left')

        # Check for any missing c_code after the merge
        if merged_df['c_code'].isnull().any():
            # Attempt to fill missing c_code by merging on company_name only (ignoring b_cat)
            alias_df_no_b_cat = alias_df[alias_df['b_cat'].isnull()][['company_name', 'c_code']].drop_duplicates()
            merged_df_no_b_cat = pd.merge(main_df, alias_df_no_b_cat, on='company_name', how='left', suffixes=('', '_only_name'))

            # Update the c_code in the original merged dataframe
            merged_df['c_code'] = merged_df['c_code'].fillna(merged_df_no_b_cat['c_code'])

            # Check again for any missing c_code
            if merged_df['c_code'].isnull().any():
                missing = merged_df[merged_df['c_code'].isnull()]['company_name'].tolist()
                raise ValueError(f"Company names not found in c_name_alias: {', '.join(missing)}")

        print("Data merged successfully")
        return merged_df
    except Exception as e:
        print(f"Error in merging data: {e}")
        return None

def export_and_update(engine, data, source_table, destination_table):
    try:
        with engine.begin() as conn:
            for _, row in data.iterrows():
                insert_query = f"""
                    INSERT INTO {destination_table} (c_code, company_name, b_cat, bill_from, bill_to, ac_term, iv_value, total_stock, total_in, eom_inventory, total_out, storage, labelling, immex_en, immex_vex, ins, sub_total, 16percent_vat, grand_total, col_chrg, iv_amount, cn_vat6)
                    VALUES ('{row['c_code']}', '{row['company_name']}', '{row['b_cat']}', '{row['bill_from']}', '{row['bill_to']}', '{row['ac_term']}', {row['iv_value']}, {row['total_stock']}, {row['total_in']}, {row['eom_inventory']}, {row['total_out']}, {row['storage']}, {row['labelling']}, {row['immex_en']}, {row['immex_vex']}, {row['ins']}, {row['sub_total']}, {row['16percent_vat']}, {row['grand_total']}, {row['col_chrg']}, {row['iv_amount']}, {row['cn_vat6']})
                """
                conn.execute(text(insert_query))

                # Get the last inserted ID
                result = conn.execute(text("SELECT LAST_INSERT_ID()")).fetchone()
                last_inserted_id = result[0]

                # Update the bd_mexico table
                update_query = f"""
                    UPDATE {source_table}
                    SET f_extract_id = {last_inserted_id}
                    WHERE id = {row['id']}
                """
                conn.execute(text(update_query))

            print(f"Data exported to {destination_table} and {source_table} updated successfully")
    except Exception as err:
        print(f"Error during export and update: {err}")

# Get SQLAlchemy engine
engine = get_sqlalchemy_engine(mysql_host, mysql_user, mysql_password, mysql_database)
print("SQLAlchemy engine created")

# Extract data from bd_mexico
data = extract_data_from_mysql(engine, source_table)

if data is not None:
    # Extract customer code data from c_name_alias
    customer_code_data = extract_customer_code(engine, alias_table)

    if customer_code_data is not None:
        # Merge the customer code data with the main data
        merged_data = merge_data(data, customer_code_data)

        if merged_data is not None:
            # Calculate charges
            data_with_charges = calculate_charges(merged_data)

            if data_with_charges is not None:
                # Reorder columns for export
                data_with_charges = data_with_charges[['id', 'c_code', 'company_name', 'b_cat', 'bill_from', 'bill_to', 'ac_term', 'iv_value', 'total_stock', 'total_in', 'eom_inventory', 'total_out', 'storage', 'labelling', 'immex_en', 'immex_vex', 'ins', 'sub_total', '16percent_vat', 'grand_total', 'col_chrg', 'iv_amount', 'cn_vat6']]

                # Rename columns to match f_extract schema
                data_with_charges.columns = ['id', 'c_code', 'company_name', 'b_cat', 'bill_from', 'bill_to', 'ac_term', 'iv_value', 'total_stock', 'total_in', 'eom_inventory', 'total_out', 'storage', 'labelling', 'immex_en', 'immex_vex', 'ins', 'sub_total', '16percent_vat', 'grand_total', 'col_chrg', 'iv_amount', 'cn_vat6']

                # Export data to f_extract and update bd_mexico
                export_and_update(engine, data_with_charges, source_table, destination_table)
