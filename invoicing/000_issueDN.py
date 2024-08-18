import subprocess
import json

file_path = './Forvia_mailsys/bd_mexico/202406A.xlsx'
sheet_name = 'Sheet1'
search_text = 'Grand total'
ac_term = '202406'
bill_from = '20240601'
bill_to = '20240630'
b_cat = 'A'
mxbd_rcvd = '20240709'
csv_output_path = f'./Forvia_mailsys/uploaded_bd/{ac_term}{b_cat}{mxbd_rcvd}.csv'

rate = 7.1268
rate_date = '20240628'

with open("temp.json", "w") as f:
    json.dump({

        "file_path": file_path,
        "sheet_name": sheet_name,
        "search_text": search_text,
        "ac_term": ac_term,
        "bill_from": bill_from,
        "bill_to": bill_to,
        "b_cat": b_cat,
        "mxbd_rcvd": mxbd_rcvd,
        "csv_output_path": csv_output_path,
        "rate": rate,
        "rate_date": rate_date
    }, f)

# Adjust parameters in a_readexcel.py before operating

subprocess.run(["python", "./Forvia_mailsys/invoicing/a_readexcel.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/b_f_extract.py"])

#subprocess.run(["python", "./Forvia_mailsys/invoicing/d_createBD.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/testbd.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/e_dnmasterlist.py"])

#subprocess.run(["python", "./Forvia_mailsys/invoicing/f_completeDN.py"])

subprocess.run(["python", "./Forvia_mailsys/invoicing/testDN.py"])

#subprocess.run(["python", "./Forvia_mailsys/invoicing/g_finalize_createpdf.py"])

#subprocess.run(["python", "./Forvia_mailsys/invoicing/i_logcsv.py"])
