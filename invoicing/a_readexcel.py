import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np
from decimal import Decimal

import json
with open("temp.json", "r") as f:
    data = json.load(f)


file_path = data["file_path"]
sheet_name = data["sheet_name"]
search_text = data["search_text"]
ac_term = data["ac_term"]
bill_from = data["bill_from"]
bill_to = data["bill_to"]
b_cat = data["b_cat"]
mxbd_rcvd = data["mxbd_rcvd"]
csv_output_path = data["csv_output_path"]

# ################ ADJUST PARAMETERS HERE ################

# file_path = './Forvia_mailsys/bd_mexico/202406A.xlsx'
# sheet_name = 'Sheet1'
# search_text = 'Grand total'
# ac_term = '202406'
# bill_from = '20240601'
# bill_to = '20240630'
# b_cat = 'A'
# mxbd_rcvd = '20240709'
# csv_output_path = f'./Forvia_mailsys/uploaded_bd/{ac_term}{b_cat}{mxbd_rcvd}.csv'

# ########################################################

def get_offsets(b_cat):
    if b_cat == 'C':
        return {
            'company_name': (-10, 0),
            'ac_term': (0, 0),
            'b_cat': (0, 0),
            'iv_value': (-10, 3),
            'total_stock': (-10, 1),
            'total_in': (-9, 1),
            'eom_inventory': (-10, 2),
            'total_out': (-8, 2),
            'storage': (-7, 1),
            'labelling': (-4, 1),
            'immex_en': (-6, 1),
            'immex_vex': (-5, 2),
            'ins': (-3, 3),
            'sub_total': (-2, 3),
            '16percent_vat': (-1, 3),
            'grand_total': (0, 3)
        }
    else:
        return {
            'company_name': (-10, 0),
            'ac_term': (0, 0),
            'b_cat': (0, 0),
            'iv_value': (-10, 3),
            'total_stock': (-10, 1),
            'total_in': (-9, 1),
            'eom_inventory': (-10, 2),
            'total_out': (-8, 2),
            'storage': (-7, 1),
            'labelling': (-6, 1),
            'immex_en': (-5, 1),
            'immex_vex': (-4, 2),
            'ins': (-3, 3),
            'sub_total': (-2, 3),
            '16percent_vat': (-1, 3),
            'grand_total': (0, 3)
        }

def find_values_by_offsets(file_path, sheet_name, text, offsets):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return []

    text_lower = text.lower()
    results = []

    for row_idx, row in df.iterrows():
        for col_idx, value in enumerate(row):
            if pd.notna(value) and text_lower in str(value).lower():
                result = {}
                for label, (row_offset, column_offset) in offsets.items():
                    if label == 'ac_term':
                        result[label] = ac_term
                    elif label == 'b_cat':
                        result[label] = b_cat
#                    elif label in ['immex_en', 'immex_vex'] and b_cat == 'C':
#                        result[label] = 0
                    else:
                        if row_offset is not None and column_offset is not None:
                            target_row_idx = row_idx + row_offset
                            target_col_idx = col_idx + column_offset

                            if 0 <= target_row_idx < len(df) and 0 <= target_col_idx < len(df.columns):
                                cell_value = df.iloc[target_row_idx, target_col_idx]
                                if pd.api.types.is_number(cell_value):
                                    cell_value = round(cell_value, 2)
                                result[label] = cell_value
                            else:
                                result[label] = None

                # Add mxbd_rcvd, bill_from, bill_to, rev fields
                result['mxbd_rcvd'] = mxbd_rcvd
                result['bill_from'] = bill_from
                result['bill_to'] = bill_to
                result['cancel'] = ''  # Initialize cancel field as blank
                result['new'] = ''  # Initialize new field as blank
                result['rev'] = ''  # Initialize rev field as blank

                results.append(result)

    return results

def convert_to_decimal(record):
    # Convert all numeric values in the record to Decimal
    for key, value in record.items():
        if isinstance(value, (float, np.float64)):
            record[key] = Decimal(str(round(value, 2)))
    return record

def get_next_revision(cursor, table, company_name, ac_term, b_cat):
    query = f"""
    SELECT rev FROM {table}
    WHERE company_name = %s AND ac_term = %s AND b_cat = %s
    ORDER BY rev DESC
    """
    cursor.execute(query, (company_name, ac_term, b_cat))
    existing_revs = cursor.fetchall()

    if not existing_revs:
        return 'R01'

    last_rev = existing_revs[0][0]
    if not last_rev or last_rev == '':
        return 'R01'
    
    last_rev_number = int(last_rev[1:])
    next_rev_number = last_rev_number + 1

    return f"R{next_rev_number:02}"

def handle_existing_records(record, cursor, table):
    # Find active records with the same company_name, ac_term, and b_cat
    query = f"""
    SELECT * FROM {table}
    WHERE company_name = %s AND ac_term = %s AND b_cat = %s AND cancel != '*'
    """
    cursor.execute(query, (record['company_name'], record['ac_term'], record['b_cat']))
    existing_records = cursor.fetchall()

    match_found = False

    if existing_records:
        for existing_record in existing_records:
            # Convert the existing record to a dictionary for easy comparison, excluding the 'cancel' and 'new' fields
            existing_record_dict = dict(zip(cursor.column_names, existing_record))
            existing_record_dict.pop('cancel', None)
            existing_record_dict.pop('new', None)
            existing_record_dict = convert_to_decimal(existing_record_dict)  # Convert existing record to Decimal

            # Check if all fields match, excluding 'cancel' and 'new'
            if all(existing_record_dict[key] == record[key] for key in record.keys() if key not in ['cancel', 'new']):
                return False  # Exact match found, skip insertion
            else:
                match_found = True

        # If match found but not exact, mark existing records as cancelled and insert new record with revision
        if match_found:
            update_query = f"""
            UPDATE {table}
            SET cancel = '*'
            WHERE company_name = %s AND ac_term = %s AND b_cat = %s AND cancel != '*'
            """
            cursor.execute(update_query, (record['company_name'], record['ac_term'], record['b_cat']))
            record['rev'] = get_next_revision(cursor, table, record['company_name'], record['ac_term'], record['b_cat'])
            return True

    # If no existing records found, insert as new
    return True

def reset_new_markings(cursor, table):
    update_query = f"""
    UPDATE {table}
    SET new = ''
    """
    cursor.execute(update_query)
    print("Reset all 'new' markings to blank.")

def export_to_mysql(data, host, user, password, database, table):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        # Reset all 'new' markings to blank before inserting new records
        reset_new_markings(cursor, table)

        new_record_count = 0
        for row in data:
            sanitized_row = convert_to_decimal(row.copy())  # Convert the new record to Decimal before comparing
            if handle_existing_records(sanitized_row, cursor, table):
                sanitized_row['new'] = '*'  # Mark the new record with '*'
                placeholders = ", ".join(["%s"] * len(sanitized_row))
                columns = ", ".join([f"`{col}`" for col in sanitized_row.keys()])
                insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                
                # Convert NaN to None (NULL in SQL) before inserting
                values = tuple(None if pd.isna(val) else val for val in sanitized_row.values())
                
                cursor.execute(insert_query, values)
                new_record_count += 1

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Data exported successfully to MySQL table {table}. {new_record_count} new record(s) added.")
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    except Exception as e:
        print(f"Error exporting to MySQL: {e}")

def export_to_csv(data, file_path):
    try:
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        print(f"Data exported successfully to CSV file {file_path}")
    except Exception as e:
        print(f"Error exporting to CSV: {e}")

offsets = get_offsets(b_cat)

values = find_values_by_offsets(file_path, sheet_name, search_text, offsets)

if values:
    # MySQL export details
    mysql_host = 'localhost'
    mysql_user = 'root'
    mysql_password = 'ngls@123'
    mysql_database = 'dnmasterlist'
    mysql_table = 'bd_mexico'

    export_to_mysql(values, mysql_host, mysql_user, mysql_password, mysql_database, mysql_table)
   
    # Export to CSV
    export_to_csv(values, csv_output_path)
else:
    print("Grand Total not found or out of bounds.")
