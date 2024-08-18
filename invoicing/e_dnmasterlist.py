import pandas as pd
import os
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime

import json
with open("temp.json", "r") as f:
    data = json.load(f)

rate = data["rate"]
rate_date = data["rate_date"]

################ ADJUST PARAMETERS HERE ################

# MySQL connection details
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_user = os.getenv('MYSQL_USER', 'root')
mysql_password = urllib.parse.quote_plus(os.getenv('MYSQL_PASSWORD', 'ngls@123'))
mysql_database = os.getenv('MYSQL_DATABASE', 'dnmasterlist')

# # Adjustable parameters
# rate = 7.1268
# rate_date = '20240628'

# Source and destination tables
source_table = 'f_extract'
lookup_table = 'bd_mexico'
destination_table = 'dnmasterlist'

########################################################

def get_sqlalchemy_engine(host, user, password, database):
    return create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

def extract_ids_from_bd_mexico(engine):
    try:
        query = f"SELECT id, f_extract_id FROM {lookup_table} WHERE `new` = '*' AND grand_total <> 0"
        df = pd.read_sql(query, engine)
        print("IDs extracted from bd_mexico")
        return df
    except Exception as err:
        print(f"Error extracting IDs from bd_mexico: {err}")
        return None

def extract_data_from_f_extract(engine, ids):
    try:
        query = f"SELECT * FROM {source_table} WHERE id IN ({','.join(map(str, ids))})"
        df = pd.read_sql(query, engine)
        print("Data extracted successfully from f_extract")
        return df
    except Exception as err:
        print(f"Error extracting data from f_extract: {err}")
        return None

def get_next_serial_number(conn, ac_term):
    try:
        query = f"SELECT MAX(serial_num) FROM {destination_table} WHERE ac_term = '{ac_term}'"
        result = conn.execute(text(query)).fetchone()
        max_serial = result[0]
        if max_serial is None:
            return '0001'
        else:
            return str(int(max_serial) + 1).zfill(4)
    except Exception as err:
        print(f"Error retrieving next serial number: {err}")
        return '0001'

def insert_to_dnmasterlist_and_update_bd_mexico(engine, data, lookup_data):
    try:
        with engine.begin() as conn:
            for _, row in data.iterrows():
                serial_num = get_next_serial_number(conn, row['ac_term'])
                sub_total_usd = row['iv_amount'] + row['cn_vat6']
                sub_total_cny = sub_total_usd * rate
                iv_amount_cny = sub_total_cny
                iv_amount_usd = sub_total_usd

                insert_query = f"""
                    INSERT INTO {destination_table} (c_code, b_cat, ac_term, serial_num, bill_from, bill_to, sto, whs, svc, trs, cst, dsb, sga, oth, cn_vat6, cn_vat9, rate_date, rate, sub_total_cny, sub_total_usd, iv_amount_cny, iv_amount_usd, d_issued)
                    VALUES ('{row['c_code']}', '{row['b_cat']}', '{row['ac_term']}', '{serial_num}', '{row['bill_from']}', '{row['bill_to']}', {row['iv_amount']}, 0, 0, 0, 0, 0, 0, 0, {row['cn_vat6']}, 0, '{rate_date}', {rate}, {sub_total_cny}, {sub_total_usd}, {iv_amount_cny}, {iv_amount_usd}, NOW())
                """
                conn.execute(text(insert_query))

                # Get the last inserted ID
                result = conn.execute(text("SELECT LAST_INSERT_ID()")).fetchone()
                last_inserted_id = result[0]

                # Update the bd_mexico table with the last inserted ID
                bd_mexico_id = lookup_data.loc[lookup_data['f_extract_id'] == row['id'], 'id'].values[0]
                update_query = f"""
                    UPDATE {lookup_table}
                    SET dnmasterlist_id = {last_inserted_id}
                    WHERE id = {bd_mexico_id}
                """
                conn.execute(text(update_query))

            print(f"Data inserted successfully into {destination_table} and bd_mexico updated")
    except Exception as err:
        print(f"Error inserting data into {destination_table} and updating bd_mexico: {err}")

# Get SQLAlchemy engine
engine = get_sqlalchemy_engine(mysql_host, mysql_user, mysql_password, mysql_database)
print("SQLAlchemy engine created")

# Extract f_extract_id from bd_mexico where new is "*"
lookup_data = extract_ids_from_bd_mexico(engine)
if lookup_data is not None:
    ids = lookup_data['f_extract_id'].tolist()

    if ids:
        # Extract data from f_extract based on the extracted IDs
        data = extract_data_from_f_extract(engine, ids)

        if data is not None:
            # Insert data into dnmasterlist and update bd_mexico
            insert_to_dnmasterlist_and_update_bd_mexico(engine, data, lookup_data)
