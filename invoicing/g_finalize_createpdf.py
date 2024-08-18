import os
import re
import win32com.client as win32
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

# MySQL connection details
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_user = os.getenv('MYSQL_USER', 'root')
mysql_password = urllib.parse.quote_plus(os.getenv('MYSQL_PASSWORD', 'ngls@123'))
mysql_database = os.getenv('MYSQL_DATABASE', 'dnmasterlist')

# Base output path
base_output_path = './invoicing/data/'

def connect_to_database():
    try:
        connection_string = f'mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}'
        engine = create_engine(connection_string)
        print("Database connection established")  # Debug statement
        return engine
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def extract_records_from_bd_mexico(engine):
    try:
        query = "SELECT company_name, rev, ac_term FROM bd_mexico WHERE `new` = '*' AND grand_total <> 0"
        df = pd.read_sql(query, engine)
        print("Records extracted from bd_mexico")  # Debug statement
        return df
    except Exception as e:
        print(f"Error extracting records from bd_mexico: {e}")
        return None

def sanitize_name(name):
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', name)  # Replace invalid characters with underscore
    sanitized = re.sub(r'_+', '_', sanitized)         # Replace multiple underscores with a single one
    sanitized = sanitized.strip('_')                  # Remove leading and trailing underscores
    return sanitized

def merge_excel_sheets(company_folder, sanitized_company_name, rev_suffix):
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    excel.Visible = False
    excel.DisplayAlerts = False

    try:
        bd_excel_file_name = f"BD_{sanitized_company_name}{rev_suffix}.xlsx"
        dn_excel_file_name = f"DN_{sanitized_company_name}{rev_suffix}.xlsx"
        bd_excel_file_path = os.path.join(company_folder, bd_excel_file_name)
        dn_excel_file_path = os.path.join(company_folder, dn_excel_file_name)

        print(f"Checking files: {bd_excel_file_path} and {dn_excel_file_path}")  # Debug statement

        if os.path.exists(bd_excel_file_path) and os.path.exists(dn_excel_file_path):
            print(f"Processing files: {bd_excel_file_path} and {dn_excel_file_path}")

            wb1 = excel.Workbooks.Open(os.path.abspath(bd_excel_file_path))
            wb2 = excel.Workbooks.Open(os.path.abspath(dn_excel_file_path))

            for sheet in wb1.Sheets:
                sheet.Copy(After=wb2.Sheets(wb2.Sheets.Count))

            new_excel_file_name = f"{sanitized_company_name}{rev_suffix}.xlsx"
            new_excel_file_path = os.path.join(company_folder, new_excel_file_name)
            wb2.SaveAs(os.path.abspath(new_excel_file_path))
            wb2.Close(False)
            wb1.Close(False)

            os.remove(bd_excel_file_path)
            os.remove(dn_excel_file_path)
            print(f"Combined and saved as: {new_excel_file_path}")
            print(f"Deleted files: {bd_excel_file_path} and {dn_excel_file_path}")
        else:
            print(f"Files not found: {bd_excel_file_path} or {dn_excel_file_path}")

    except Exception as e:
        print(f"Error processing Excel files in folder {company_folder}: {e}")
    finally:
        excel.Quit()

def generate_pdf_from_excel(company_folder, sanitized_company_name, rev_suffix):
    try:
        excel_file_name = f"{sanitized_company_name}{rev_suffix}.xlsx"
        excel_file_path = os.path.join(company_folder, excel_file_name)
        pdf_file_path = os.path.splitext(excel_file_path)[0] + ".pdf"

        excel = win32.gencache.EnsureDispatch('Excel.Application')
        wb = excel.Workbooks.Open(os.path.abspath(excel_file_path))
        wb.ExportAsFixedFormat(0, os.path.abspath(pdf_file_path))
        wb.Close(False)
        excel.Quit()

        print(f"PDF generated: {pdf_file_path}")
    except Exception as e:
        print(f"Error generating PDF from {excel_file_path}: {e}")

def main():
    engine = connect_to_database()
    if engine is not None:
        try:
            records = extract_records_from_bd_mexico(engine)
            if records is not None and not records.empty:
                for _, row in records.iterrows():
                    company_name = row['company_name']
                    rev_suffix = f"_R{row['rev'][-2:]}" if row['rev'] else ""
                    sanitized_company_name = sanitize_name(company_name)
                    ac_term = row['ac_term']

                    company_folder = os.path.join(base_output_path, str(ac_term), sanitized_company_name)
                    if os.path.exists(company_folder):
                        print(f"Processing folder: {company_folder}")  # Debug statement
                        merge_excel_sheets(company_folder, sanitized_company_name, rev_suffix)
                        generate_pdf_from_excel(company_folder, sanitized_company_name, rev_suffix)
                    else:
                        print(f"Folder not found: {company_folder}")  # Debug statement
            else:
                print("No new records found or data is empty.")
        finally:
            engine.dispose()

if __name__ == "__main__":
    main()
