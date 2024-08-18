import pandas as pd
import mysql.connector
from mysql.connector import Error

def process_statusmaster_to_csv(host, user, password, database, table, output_csv_path):
    connection = None
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()

            df = pd.DataFrame(rows)

            # Apply conditions and update MySQL table
            for i, row in df.iterrows():
                sup_rem = ''
                mx_rem = ''
                fpio_f = ''

                if pd.isna(row['payment_d']) and pd.isna(row['1st_rep']) and pd.isna(row['bd_finalized']) and pd.isna(row['fapiao_d']):
                    sup_rem = '*'
                if pd.isna(row['payment_d']) and pd.notna(row['1st_rep']) and row['affirm_yn'] == 'n' and pd.isna(row['bd_finalized']) and pd.isna(row['fapiao_d']):
                    mx_rem = '*'
                if (row['affirm_yn'] == 'y' or pd.notna(row['bd_finalized'])) and pd.isna(row['payment_d']) and pd.isna(row['fapiao_d']):
                    fpio_f = '*'

                # Update DataFrame
                df.at[i, 'sup_rem'] = sup_rem
                df.at[i, 'mx_rem'] = mx_rem
                df.at[i, 'fpio_f'] = fpio_f

                # Update MySQL table
                update_sql = f"""
                UPDATE {table} 
                SET sup_rem = %s, mx_rem = %s, fpio_f = %s
                WHERE id = %s
                """
                cursor.execute(update_sql, (sup_rem, mx_rem, fpio_f, row['id']))

            connection.commit()
            print("MySQL table updated successfully")

            # Save the DataFrame to a CSV file
            df.to_csv(output_csv_path, index=False)
            print(f"Data processed and saved to {output_csv_path}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection is not None and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Example usage
host = 'localhost'
user = 'root'
password = 'ngls@123'
database = 'dnmasterlist'
table = 'statusmaster'
output_csv_path = './Forvia_mailsys/invoicing/data/processed_statusmaster.csv'

process_statusmaster_to_csv(host, user, password, database, table, output_csv_path)
